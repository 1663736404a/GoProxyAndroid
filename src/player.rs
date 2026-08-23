//! 代理核心逻辑，对应 Go 版的 proxy.go。
//!
//! 流程：
//! 1. 解析客户端 Range
//! 2. 先取首块，从 Content-Range 拿到文件总长度，立刻回写响应头
//! 3. 之后每轮并发拉 `thread` 个分块，**按序**写回，保持流式输出
//! 4. 单块失败自动重试；客户端断开时中止全部在途任务

use std::cmp::min;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use tokio::task::JoinHandle;

/// 单块重试次数（对应 Go 的 maxRetries=3）
const CHUNK_RETRIES: u32 = 3;
/// 单块超时（对应 Go 的 60s）
const CHUNK_TIMEOUT: Duration = Duration::from_secs(60);
/// 一轮并发缓存的字节上限。Go 版没有上限，`?thread=256&chunkSize=8192`
/// 会试图一次分配 2GB；这里夹住，避免宿主进程 OOM。
const MAX_ROUND_BYTES: i64 = 64 * 1024 * 1024;

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
        // 夹住一轮的总缓存量
        while thread > MIN_THREAD && (thread as i64) * chunk_size > MAX_ROUND_BYTES {
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

            let mut cursor = start + first_len;

            while cursor <= final_end {
                // 一轮起 thread 个任务并发下载
                let mut handles = Vec::with_capacity(thread);
                let mut next = cursor;

                for _ in 0..thread {
                    if next > final_end {
                        break;
                    }
                    let cs = next;
                    let ce = min(next + chunk_size - 1, final_end);
                    next = ce + 1;

                    let c = client.clone();
                    let u = url.clone();
                    let h = header.clone();
                    handles.push(AbortOnDrop(tokio::spawn(async move {
                        Self::download_chunk(c, u, h, cs, ce).await.map(|(d, _, _)| d)
                    })));
                }

                if handles.is_empty() {
                    break;
                }
                cursor = next;

                // 下载可以并发，写出顺序必须稳定，否则客户端数据错位。
                // 提前 return 时，Vec 的 IntoIter 会 drop 掉剩余 handle → 自动 abort。
                for h in handles {
                    match h.await {
                        Ok(Ok(data)) => yield Ok(data),
                        Ok(Err(e)) => {
                            yield Err(std::io::Error::other(e));
                            return;
                        }
                        Err(e) => {
                            // 任务 panic 或被取消
                            yield Err(std::io::Error::other(format!("分块任务异常: {e}")));
                            return;
                        }
                    }
                }
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
