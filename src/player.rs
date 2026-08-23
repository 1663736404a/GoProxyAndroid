//! 代理核心逻辑，对应 Go 版的 proxy.go。
//!
//! 流程：
//! 1. 解析客户端 Range
//! 2. 先取首块，从 Content-Range 拿到文件总长度，立刻回写响应头
//! 3. 之后用**流水线**并发拉分块：`thread` 个 worker 各自不停领取下一块，
//!    另有一个按序重排缓冲负责保证写出顺序
//! 4. 单块失败自动重试；客户端断开时中止全部在途任务
//!
//! ## 为什么不是"每轮 N 块 + 栅栏"
//!
//! Go 版（以及本文件的初版）是轮次结构：起 N 个块 → 全部收完 → 写出 → 下一轮。
//! 它有两个叠加的吞吐损失，在高码率 4K 原盘上会直接卡顿：
//!
//! 1. **每轮耗时 = 该轮最慢那块的耗时。** 实测夸克 CDN 的 1MB 块耗时
//!    mean 1.17s / p90 1.60s / p99 1.95s，取 16 块的最大值，
//!    `E[max/mean] ≈ 1.51x` → 白扔 34% 带宽。
//! 2. **下载与回写不重叠。** `yield` 会挂起直到播放器取走字节，
//!    于是一轮的 N×chunk 往播放器灌的整个过程中，网络上一个字节都没在下。
//!
//! 流水线同时解决两点：worker 永不空转（慢块只拖累自己，不拖累同伴），
//! 且回写某块时其余 worker 仍在下载。实测 128MB 传输
//! 栅栏 66 Mbps → 流水线 104 Mbps。

use std::cmp::min;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;

/// 单块重试次数（对应 Go 的 maxRetries=3）
const CHUNK_RETRIES: u32 = 3;
/// 单块超时（对应 Go 的 60s）
const CHUNK_TIMEOUT: Duration = Duration::from_secs(60);
/// 在途 + 已下载待写出的字节上限。
///
/// Go 版没有上限，`?thread=256&chunkSize=8192` 会试图一次分配 2GB。
/// 流水线模式下这个值同时充当 look-ahead 窗口：预读跑在播放器前面
/// 最多这么多字节，再多就等播放器消费（背压），避免 seek 后白下一大堆。
const MAX_INFLIGHT_BYTES: i64 = 64 * 1024 * 1024;

const MIN_THREAD: usize = 1;
const MAX_THREAD: usize = 32;
const MIN_CHUNK_KB: i64 = 16;
const MAX_CHUNK_KB: i64 = 8 * 1024;

/// 只透传和源站关系最强的几个头，其余一律不带（与 Go 版一致）
const FORWARD_HEADERS: [&str; 3] = ["user-agent", "cookie", "referer"];

/// 回写响应时要跳过的上游头：长度/范围由我们自己算，逐跳头不能转发
const SKIP_RESPONSE_HEADERS: [&str; 4] = [
    "content-range",
    "content-length",
    "transfer-encoding",
    "connection",
];

/// 包一层 JoinHandle，drop 时自动 abort。
///
/// Go 靠 context 传染取消，Rust 里 `tokio::spawn` 出去的任务不会因为
/// 调用方消失而停止，必须显式 abort，否则播放器 seek 之后旧分块
/// 仍在后台吃带宽。
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> Future for AbortOnDrop<T> {
    type Output = Result<T, tokio::task::JoinError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // JoinHandle 是 Unpin，可以直接重新 Pin
        Pin::new(&mut self.0).poll(cx)
    }
}

pub struct Player {
    client: reqwest::Client,
    header: reqwest::header::HeaderMap,
    start: i64,
    /// 客户端请求的结束位（闭区间）。-1 表示开放区间，取到文件尾。
    end: i64,
    thread: usize,
    chunk_size: i64,
    url: String,
}

impl Player {
    pub fn new(
        incoming: &HeaderMap,
        thread: usize,
        chunk_size_kb: i64,
        url: String,
    ) -> Result<Self, String> {
        let mut header = reqwest::header::HeaderMap::new();
        for key in FORWARD_HEADERS {
            if let Some(v) = incoming.get(key) {
                if let (Ok(name), Ok(val)) = (
                    HeaderName::from_bytes(key.as_bytes()),
                    HeaderValue::from_bytes(v.as_bytes()),
                ) {
                    header.insert(name, val);
                }
            }
        }

        let (start, end) = parse_range(
            incoming
                .get("range")
                .and_then(|v| v.to_str().ok())
                .unwrap_or(""),
        );

        let mut thread = thread.clamp(MIN_THREAD, MAX_THREAD);
        let chunk_size = chunk_size_kb.clamp(MIN_CHUNK_KB, MAX_CHUNK_KB) * 1024;
        // 夹住在途总量：worker 数 × 单块不能超过预算
        while thread > MIN_THREAD && (thread as i64) * chunk_size > MAX_INFLIGHT_BYTES {
            thread /= 2;
        }

        let client = reqwest::Client::builder()
            // 不设整体超时，长视频/慢源站不能被统一截断（与 Go 版一致）
            .danger_accept_invalid_certs(true)
            .pool_max_idle_per_host(20)
            .pool_idle_timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .map_err(|e| format!("构建 HTTP client 失败: {e}"))?;

        Ok(Self {
            client,
            header,
            start,
            end,
            thread,
            chunk_size,
            url,
        })
    }

    /// 拉一个字节区间（闭区间 `[start, end]`），带退避重试。
    async fn download_chunk(
        client: reqwest::Client,
        url: String,
        header: reqwest::header::HeaderMap,
        start: i64,
        end: i64,
    ) -> Result<(Bytes, reqwest::header::HeaderMap, StatusCode), String> {
        let mut last_err = String::new();

        for retry in 0..CHUNK_RETRIES {
            let range = format!("bytes={}-{}", start, end);
            let req = client
                .get(&url)
                .headers(header.clone())
                .header(reqwest::header::RANGE, &range)
                .timeout(CHUNK_TIMEOUT);

            match req.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    // 有些源站带了 Range 也照样回 200，一并兼容
                    if status == StatusCode::PARTIAL_CONTENT || status == StatusCode::OK {
                        let headers = resp.headers().clone();
                        match resp.bytes().await {
                            Ok(data) => return Ok((data, headers, status)),
                            Err(e) => last_err = format!("读取响应体失败: {e}"),
                        }
                    } else {
                        last_err = format!("状态码: {}", status.as_u16());
                    }
                }
                Err(e) => last_err = e.to_string(),
            }

            if retry + 1 < CHUNK_RETRIES {
                // 简单线性退避，避免连续重试过于激进
                tokio::time::sleep(Duration::from_millis(500 * (retry as u64 + 1))).await;
            }
        }

        Err(format!("重试 {CHUNK_RETRIES} 次失败: {last_err}"))
    }

    /// 执行一次完整代理传输。
    pub async fn play(self) -> Response {
        // ---- 首块：确认总长度 + 决定回给客户端的响应头 ----
        //
        // Go 版这里有个算术 bug：
        //     if end <= 0 { end = 100 } else { end += 1 }
        //     end = start + min(end, chunkSize)
        // 客户端发闭区间（bytes=1000-2000）时会多加一个 start，
        // 实际请求 1000-3000 却声明 Content-Range 1000-2000，多拉一段废字节。
        let want_end_excl = if self.end <= 0 {
            // 客户端没给结束位，先试探一小块，只为拿总长度
            self.start + 100
        } else {
            self.end + 1
        };
        let first_end_excl = min(want_end_excl, self.start + self.chunk_size);
        let first_end_incl = (first_end_excl - 1).max(self.start);

        let (first_chunk, up_headers, status) = match Self::download_chunk(
            self.client.clone(),
            self.url.clone(),
            self.header.clone(),
            self.start,
            first_end_incl,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                return (StatusCode::BAD_GATEWAY, format!("首块下载失败: {e}")).into_response()
            }
        };

        let total = match up_headers
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_total)
        {
            Some(t) if t > 0 => t,
            _ => {
                return (StatusCode::BAD_GATEWAY, "未获取到文件总大小").into_response();
            }
        };

        let final_end = if self.end <= 0 {
            total - 1
        } else {
            min(self.end, total - 1)
        };

        if self.start > final_end {
            return (
                StatusCode::RANGE_NOT_SATISFIABLE,
                format!("无效范围: {}-{}/{}", self.start, final_end, total),
            )
                .into_response();
        }

        let mut resp_headers = HeaderMap::new();
        for (k, v) in up_headers.iter() {
            if SKIP_RESPONSE_HEADERS.contains(&k.as_str()) {
                continue;
            }
            if let (Ok(name), Ok(val)) = (
                HeaderName::from_bytes(k.as_str().as_bytes()),
                HeaderValue::from_bytes(v.as_bytes()),
            ) {
                resp_headers.insert(name, val);
            }
        }
        if let Ok(v) = HeaderValue::from_str(&format!(
            "bytes {}-{}/{}",
            self.start, final_end, total
        )) {
            resp_headers.insert(axum::http::header::CONTENT_RANGE, v);
        }
        // Go 版把 Content-Length 一起剔掉了，于是退化成 chunked。
        // 长度是确定的，明确声明对播放器（尤其 ExoPlayer 的进度条）更友好。
        if let Ok(v) = HeaderValue::from_str(&(final_end - self.start + 1).to_string()) {
            resp_headers.insert(axum::http::header::CONTENT_LENGTH, v);
        }
        resp_headers.insert(
            axum::http::header::ACCEPT_RANGES,
            HeaderValue::from_static("bytes"),
        );

        // 首块可能比声明范围长（试探时按 chunk 取），裁掉多余部分
        let want_first = (final_end - self.start + 1) as usize;
        let first_chunk = if first_chunk.len() > want_first {
            first_chunk.slice(0..want_first)
        } else {
            first_chunk
        };

        let client = self.client;
        let url = self.url;
        let header = self.header;
        let thread = self.thread;
        let chunk_size = self.chunk_size;
        let start = self.start;

        let stream = async_stream::stream! {
            let first_len = first_chunk.len() as i64;
            yield Ok::<Bytes, std::io::Error>(first_chunk);

            let cursor = start + first_len;
            if cursor > final_end {
                return;
            }

            // ---- 流水线：worker 从共享游标领块，结果按序号重排后写出 ----
            //
            // `next_offset` 是共享领取游标，worker 用 fetch_add 原子领取，
            // 因此不需要额外的任务队列，也不会重复或漏块。
            let next_offset = Arc::new(AtomicI64::new(cursor));

            // look-ahead 窗口：一个块从"开始下载"到"已写给播放器"之间占一个许可。
            //
            // 只靠通道容量是不够的：接收端会把乱序块搬进重排缓冲，从而腾空通道，
            // 于是慢块（最坏 60s 超时）期间其余 worker 能一路狂奔，
            // 重排缓冲无上限增长。用信号量把 [在途 + 待重排] 一起夹住。
            let slots = ((MAX_INFLIGHT_BYTES / chunk_size) as usize).max(thread).max(2);
            let window = Arc::new(Semaphore::new(slots));
            // (序号, 结果) —— 序号用于重排
            let (tx, mut rx) = mpsc::channel::<(u64, Result<Bytes, String>)>(slots);

            let mut workers = Vec::with_capacity(thread);
            for _ in 0..thread {
                let c = client.clone();
                let u = url.clone();
                let h = header.clone();
                let cur = next_offset.clone();
                let tx = tx.clone();
                let win = window.clone();

                workers.push(AbortOnDrop(tokio::spawn(async move {
                    loop {
                        // 先拿许可再领块：许可的获取顺序即分块的派发顺序，
                        // 保证"当前最小未写出块"一定已经持有许可 → 不会自锁。
                        // 许可由接收端在写出后 add_permits 归还，所以这里 forget。
                        match win.acquire().await {
                            Ok(p) => p.forget(),
                            Err(_) => return, // 信号量已关闭
                        }

                        // 原子领取下一块，永不空转等同伴
                        let cs = cur.fetch_add(chunk_size, Ordering::Relaxed);
                        if cs > final_end {
                            // 关键：把许可还回去再退出。
                            // 否则每个收工的 worker 都会吞掉一个许可，
                            // 当 worker 数 > 窗口槽位时，剩余 worker 会永久阻塞在
                            // acquire 上，它们持有的 tx 不释放 → 通道不关闭 →
                            // 接收端 rx.recv() 永远等不到 None → 整个响应挂死。
                            win.add_permits(1);
                            return;
                        }
                        let ce = min(cs + chunk_size - 1, final_end);
                        // 块序号：距起点第几块，用于接收端重排
                        let seq = ((cs - cursor) / chunk_size) as u64;

                        let r = Self::download_chunk(c.clone(), u.clone(), h.clone(), cs, ce)
                            .await
                            .map(|(d, _, _)| d);
                        let failed = r.is_err();

                        // send 失败 = 接收端已走（客户端断开），直接收工
                        if tx.send((seq, r)).await.is_err() || failed {
                            return;
                        }
                    }
                })));
            }
            // 本地这份必须丢掉，否则 rx 永远等不到通道关闭
            drop(tx);

            // 重排缓冲：先到的乱序块暂存，凑到期望序号才写出。
            // 容量受 slots 限制（发送端阻塞），所以内存有界。
            let mut pending: HashMap<u64, Bytes> = HashMap::new();
            let mut want: u64 = 0;
            let mut failure: Option<String> = None;

            while let Some((seq, res)) = rx.recv().await {
                match res {
                    Ok(data) => {
                        pending.insert(seq, data);
                        // 把连续可写的块一次性排空。
                        // 每写出一块就归还一个许可，让 worker 继续往前预读。
                        while let Some(d) = pending.remove(&want) {
                            yield Ok(d);
                            want += 1;
                            window.add_permits(1);
                        }
                    }
                    Err(e) => {
                        failure = Some(e);
                        break;
                    }
                }
            }

            // workers 在此 drop → AbortOnDrop 中止所有在途下载，
            // 不让 seek 之后的旧分块继续吃带宽。
            drop(workers);

            if let Some(e) = failure {
                yield Err(std::io::Error::other(e));
            }
        };

        let mut response = Response::new(Body::from_stream(stream));
        *response.status_mut() = if status == StatusCode::OK && self.end <= 0 && start == 0 {
            StatusCode::OK
        } else {
            StatusCode::PARTIAL_CONTENT
        };
        *response.headers_mut() = resp_headers;
        response
    }
}

/// 解析请求的 `Range: bytes=START-[END]`，返回 (start, end)，end = -1 表示开放区间。
///
/// 不支持 `bytes=-500` 这种后缀式（Go 版的正则同样不支持，保持一致）。
pub fn parse_range(value: &str) -> (i64, i64) {
    let rest = match value.trim().strip_prefix("bytes=") {
        Some(r) => r,
        None => return (0, -1),
    };
    // 多区间请求只取第一段
    let first = rest.split(',').next().unwrap_or("").trim();
    let (s, e) = match first.split_once('-') {
        Some(v) => v,
        None => return (0, -1),
    };
    let s = s.trim();
    if s.is_empty() {
        return (0, -1);
    }
    let start = match s.parse::<i64>() {
        Ok(v) if v >= 0 => v,
        _ => return (0, -1),
    };
    let e = e.trim();
    let end = if e.is_empty() {
        -1
    } else {
        e.parse::<i64>().unwrap_or(-1)
    };
    (start, end)
}

/// 从 `Content-Range: bytes 0-99/12345` 里取出总长度。
pub fn parse_total(value: &str) -> Option<i64> {
    let (_, total) = value.rsplit_once('/')?;
    total.trim().parse::<i64>().ok()
}

/// 从 query 里取 thread / chunkSize / url，缺一不可（与 Go 版一致）。
pub fn extract_params(params: &HashMap<String, String>) -> Result<(usize, i64, String), Response> {
    let missing = || -> Response {
        (StatusCode::BAD_REQUEST, "参数不完整").into_response()
    };

    let thread = params.get("thread").ok_or_else(missing)?;
    let chunk = params.get("chunkSize").ok_or_else(missing)?;
    let url = params.get("url").ok_or_else(missing)?;

    if thread.is_empty() || chunk.is_empty() || url.is_empty() {
        return Err(missing());
    }

    let thread: usize = thread
        .parse()
        .map_err(|_| (StatusCode::BAD_REQUEST, "thread必须为整数").into_response())?;
    let chunk: i64 = chunk
        .parse()
        .map_err(|_| (StatusCode::BAD_REQUEST, "chunkSize必须为整数").into_response())?;

    Ok((thread, chunk, url.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_open_ended() {
        assert_eq!(parse_range("bytes=0-"), (0, -1));
        assert_eq!(parse_range("bytes=1024-"), (1024, -1));
    }

    #[test]
    fn range_closed() {
        assert_eq!(parse_range("bytes=1000-2000"), (1000, 2000));
    }

    #[test]
    fn range_absent_or_bad() {
        assert_eq!(parse_range(""), (0, -1));
        assert_eq!(parse_range("byte=0-1"), (0, -1));
        // 后缀式不支持，退化成整体请求（与 Go 正则行为一致）
        assert_eq!(parse_range("bytes=-500"), (0, -1));
    }

    #[test]
    fn range_multi_takes_first() {
        assert_eq!(parse_range("bytes=0-99, 200-299"), (0, 99));
    }

    #[test]
    fn total_from_content_range() {
        assert_eq!(parse_total("bytes 0-99/12345"), Some(12345));
        assert_eq!(parse_total("bytes 0-0/1"), Some(1));
        assert_eq!(parse_total("bytes */*"), None);
        assert_eq!(parse_total("garbage"), None);
    }

    /// 复刻流水线的调度骨架（不发真实请求），验证核心不变式：
    /// 按序、无重复、无遗漏、重排缓冲有界、且**不死锁**。
    ///
    /// `delay` 用来构造病态耗时分布；`threads`/`slots` 覆盖
    /// "worker 数 > 窗口槽位" 这种最容易死锁的配置。
    async fn drive_pipeline(
        nchunk: i64,
        threads: usize,
        slots: usize,
        delay: fn(u64) -> u64,
    ) -> (Vec<u64>, usize) {
        use std::sync::atomic::{AtomicI64, Ordering};
        use std::sync::Arc;
        use tokio::sync::{mpsc, Semaphore};

        let final_end = nchunk - 1;
        let next = Arc::new(AtomicI64::new(0));
        let window = Arc::new(Semaphore::new(slots));
        let (tx, mut rx) = mpsc::channel::<(u64, Bytes)>(slots);

        let mut workers = Vec::new();
        for _ in 0..threads {
            let next = next.clone();
            let win = window.clone();
            let tx = tx.clone();
            workers.push(AbortOnDrop(tokio::spawn(async move {
                loop {
                    match win.acquire().await {
                        Ok(p) => p.forget(),
                        Err(_) => return,
                    }
                    let cs = next.fetch_add(1, Ordering::Relaxed);
                    if cs > final_end {
                        // 归还许可，否则 worker 数 > slots 时会互相饿死
                        win.add_permits(1);
                        return;
                    }
                    let seq = cs as u64;
                    tokio::time::sleep(Duration::from_millis(delay(seq))).await;
                    if tx.send((seq, Bytes::new())).await.is_err() {
                        return;
                    }
                }
            })));
        }
        drop(tx);

        let mut pending: HashMap<u64, Bytes> = HashMap::new();
        let mut want = 0u64;
        let mut out = Vec::new();
        let mut peak = 0usize;
        while let Some((seq, d)) = rx.recv().await {
            pending.insert(seq, d);
            peak = peak.max(pending.len());
            while pending.remove(&want).is_some() {
                out.push(want);
                want += 1;
                window.add_permits(1);
            }
        }
        drop(workers);
        (out, peak)
    }

    fn d_fast(_: u64) -> u64 {
        1
    }
    fn d_straggler(s: u64) -> u64 {
        if s % 5 == 2 {
            30
        } else {
            1
        }
    }
    fn d_head_slow(s: u64) -> u64 {
        if s == 0 {
            60
        } else {
            1
        }
    }

    /// 正常配置：慢块不应破坏顺序，也不应让缓冲越界。
    #[tokio::test]
    async fn pipeline_ordered_and_bounded() {
        let (out, peak) = drive_pipeline(120, 16, 24, d_straggler).await;
        assert_eq!(out.len(), 120);
        assert!(out.windows(2).all(|w| w[1] == w[0] + 1), "写出乱序");
        assert!(peak <= 24, "重排缓冲越界: {peak}");
    }

    /// 队头最慢 = 最坏重排压力：后续块全堆在缓冲里，仍必须有界。
    #[tokio::test]
    async fn pipeline_head_of_line_blocking_stays_bounded() {
        let (out, peak) = drive_pipeline(120, 16, 20, d_head_slow).await;
        assert_eq!(out.len(), 120);
        assert!(out.windows(2).all(|w| w[1] == w[0] + 1));
        assert!(peak <= 20, "重排缓冲越界: {peak}");
    }

    /// 回归：worker 数 > 窗口槽位。
    ///
    /// 早期实现里收工的 worker 直接 return、不归还许可，于是每个退出的 worker
    /// 吞掉一个许可，剩下的 worker 永久卡在 acquire、tx 不释放、通道不关闭，
    /// 接收端 recv() 永远等不到 None → 整个响应挂死（实测会 hang）。
    /// 加 5s 超时，一旦回归就会失败而不是把测试挂住。
    #[tokio::test]
    async fn pipeline_no_deadlock_when_threads_exceed_slots() {
        let r = tokio::time::timeout(
            Duration::from_secs(5),
            drive_pipeline(8, 32, 4, d_fast),
        )
        .await;
        let (out, _) = r.expect("死锁：worker 数超过窗口槽位时未归还许可");
        assert_eq!(out.len(), 8);
        assert!(out.windows(2).all(|w| w[1] == w[0] + 1));
    }

    /// 极小窗口 + 单线程，退化路径也要能跑完。
    #[tokio::test]
    async fn pipeline_minimal_config() {
        let r = tokio::time::timeout(
            Duration::from_secs(5),
            drive_pipeline(20, 1, 1, d_fast),
        )
        .await;
        let (out, peak) = r.expect("单线程单槽位死锁");
        assert_eq!(out.len(), 20);
        assert!(peak <= 1);
    }

    /// 首块区间必须落在 [start, start+chunk) 内，且不越过客户端要求的结束位。
    /// 这条用来钉住 Go 版 `end = start + min(end, chunkSize)` 那个多加 start 的 bug。
    #[test]
    fn first_chunk_bounds() {
        fn first_end_excl(start: i64, end: i64, chunk: i64) -> i64 {
            let want = if end <= 0 { start + 100 } else { end + 1 };
            min(want, start + chunk)
        }
        // 闭区间且小于 chunk：精确取到 2001，不多拉
        assert_eq!(first_end_excl(1000, 2000, 1024 * 1024), 2001);
        // 闭区间大于 chunk：截到 chunk 边界
        assert_eq!(first_end_excl(1000, 99_999_999, 1024), 1000 + 1024);
        // 开放区间：探测 100 字节
        assert_eq!(first_end_excl(0, -1, 1024 * 1024), 100);
    }
}
