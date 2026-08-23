//! JNI 桥接层 + 服务生命周期，对应 Go 版的 proxy_jni.go。
//!
//! 导出四个符号，与既有 Java 侧 `com.github.catvod.spider.GoProxyLibrary` 完全一致：
//!   startProxy(int) -> int   0=成功 1=已在运行 2=端口绑定失败 3=内部错误
//!   stopProxy()     -> int   0=成功 1=停止失败
//!   isProxyRunning()-> int   1=运行中 0=未运行
//!   getLastError()  -> String

mod player;

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::sync::{Mutex, OnceLock};

use axum::extract::Query;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use jni::objects::JClass;
use jni::sys::{jint, jstring};
use jni::JNIEnv;
use tokio::sync::oneshot;

pub use player::{extract_params, parse_range, parse_total, Player};

pub const DEFAULT_PORT: u16 = 5575;
/// Go 版 JNI 分支在 port<=0 时兜底 5576，而 main.go 与 /health 用 5575。
/// 这里统一成 DEFAULT_PORT，避免三处不一致。
pub const JNI_FALLBACK_PORT: u16 = DEFAULT_PORT;

struct ServerState {
    port: u16,
    shutdown: oneshot::Sender<()>,
    thread: std::thread::JoinHandle<()>,
}

fn state() -> &'static Mutex<Option<ServerState>> {
    static STATE: OnceLock<Mutex<Option<ServerState>>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(None))
}

fn last_error() -> &'static Mutex<String> {
    static ERR: OnceLock<Mutex<String>> = OnceLock::new();
    ERR.get_or_init(|| Mutex::new(String::new()))
}

fn set_last_error(msg: impl Into<String>) {
    if let Ok(mut g) = last_error().lock() {
        *g = msg.into();
    }
}

fn clear_last_error() {
    set_last_error("");
}

/// 构建路由。三个端点与 Go 版一致：`/`、`/proxy`、`/health`。
pub fn build_router(port: u16) -> Router {
    Router::new()
        .route("/", get(|| async { "ok" }))
        .route("/proxy", get(proxy_handler))
        .route(
            "/health",
            get(move || async move {
                (
                    [(axum::http::header::CONTENT_TYPE, "application/json")],
                    format!(
                        r#"{{"status": "healthy", "type": "rust", "port": {}, "timestamp": "{}"}}"#,
                        port,
                        now_rfc3339()
                    ),
                )
            }),
        )
}

async fn proxy_handler(Query(params): Query<HashMap<String, String>>, headers: HeaderMap) -> Response {
    let (thread, chunk_size, url) = match extract_params(&params) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    match Player::new(&headers, thread, chunk_size, url) {
        Ok(player) => player.play().await,
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

/// 不引入 chrono，手写一个 UTC RFC3339，够 /health 用。
fn now_rfc3339() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let days = secs / 86_400;
    let tod = secs % 86_400;
    let (h, mi, s) = (tod / 3600, (tod % 3600) / 60, tod % 60);

    // 从 1970-01-01 推算年月日
    let mut year = 1970i64;
    let mut d = days;
    loop {
        let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
        let len = if leap { 366 } else { 365 };
        if d < len {
            break;
        }
        d -= len;
        year += 1;
    }
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let ml = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 1;
    for len in ml {
        if d < len {
            break;
        }
        d -= len;
        month += 1;
    }
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year,
        month,
        d + 1,
        h,
        mi,
        s
    )
}

/// 启动服务。返回码与 Go 版对齐。
///
/// 关键设计（沿用 Go 版）：**同步 bind，异步 serve**。端口冲突这类错误
/// 在函数返回前就能拿到，Java 侧不必靠轮询端口猜服务起没起。
pub fn start(port: u16) -> jint {
    clear_last_error();

    let mut guard = match state().lock() {
        Ok(g) => g,
        Err(_) => {
            set_last_error("state lock poisoned");
            return 3;
        }
    };

    if guard.is_some() {
        set_last_error("proxy already running");
        return 1;
    }

    let port = if port == 0 { JNI_FALLBACK_PORT } else { port };

    // 只绑 127.0.0.1。Go 版绑 ":port"（所有网卡）且无鉴权，
    // 同网段任何设备都能把本机当免费转发器。播放器和 jar 都在本机，
    // 收窄到 loopback 不影响功能。
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let listener = match StdTcpListener::bind(addr) {
        Ok(l) => l,
        Err(e) => {
            set_last_error(e.to_string());
            return 2;
        }
    };
    if let Err(e) = listener.set_nonblocking(true) {
        set_last_error(e.to_string());
        return 2;
    }

    let (tx, rx) = oneshot::channel::<()>();

    let thread = std::thread::Builder::new()
        .name("goproxy-rt".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    set_last_error(format!("构建 runtime 失败: {e}"));
                    return;
                }
            };

            rt.block_on(async move {
                let listener = match tokio::net::TcpListener::from_std(listener) {
                    Ok(l) => l,
                    Err(e) => {
                        set_last_error(format!("转换 listener 失败: {e}"));
                        return;
                    }
                };
                let app = build_router(port);
                if let Err(e) = axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        let _ = rx.await;
                    })
                    .await
                {
                    set_last_error(format!("serve 结束: {e}"));
                }
            });
        });

    match thread {
        Ok(t) => {
            *guard = Some(ServerState {
                port,
                shutdown: tx,
                thread: t,
            });
            0
        }
        Err(e) => {
            set_last_error(format!("启动线程失败: {e}"));
            3
        }
    }
}

/// 停止服务。优雅关闭，最多等 5 秒（与 Go 版一致）。
pub fn stop() -> jint {
    let taken = match state().lock() {
        Ok(mut g) => g.take(),
        Err(_) => {
            set_last_error("state lock poisoned");
            return 1;
        }
    };

    let Some(st) = taken else {
        clear_last_error();
        return 0;
    };

    let _ = st.shutdown.send(());

    // 不能直接 join：如果 runtime 卡住会永久阻塞调用方（通常是 Java 主线程）。
    // 轮询到 5 秒为止，超时就放弃 join，线程会随进程退出。
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !st.thread.is_finished() {
        if std::time::Instant::now() >= deadline {
            set_last_error("shutdown timeout");
            return 1;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let _ = st.thread.join();
    clear_last_error();
    0
}

pub fn is_running() -> jint {
    match state().lock() {
        Ok(g) => g.is_some() as jint,
        Err(_) => 0,
    }
}

pub fn running_port() -> u16 {
    state()
        .lock()
        .ok()
        .and_then(|g| g.as_ref().map(|s| s.port))
        .unwrap_or(0)
}

pub fn take_last_error() -> String {
    last_error()
        .lock()
        .map(|g| g.clone())
        .unwrap_or_default()
}

// ============================ JNI 导出 ============================
//
// 签名必须与 Go 版逐字一致，Java 侧 GoProxyLibrary 无需任何改动。

#[no_mangle]
pub extern "system" fn Java_com_github_catvod_spider_GoProxyLibrary_startProxy(
    _env: JNIEnv,
    _class: JClass,
    port: jint,
) -> jint {
    let port = if port <= 0 || port > u16::MAX as jint {
        JNI_FALLBACK_PORT
    } else {
        port as u16
    };
    start(port)
}

#[no_mangle]
pub extern "system" fn Java_com_github_catvod_spider_GoProxyLibrary_stopProxy(
    _env: JNIEnv,
    _class: JClass,
) -> jint {
    stop()
}

#[no_mangle]
pub extern "system" fn Java_com_github_catvod_spider_GoProxyLibrary_isProxyRunning(
    _env: JNIEnv,
    _class: JClass,
) -> jint {
    is_running()
}

#[no_mangle]
pub extern "system" fn Java_com_github_catvod_spider_GoProxyLibrary_getLastError(
    mut env: JNIEnv,
    _class: JClass,
) -> jstring {
    let msg = take_last_error();
    match env.new_string(msg) {
        Ok(s) => s.into_raw(),
        // 建不出字符串就返回 null，Java 侧判空即可
        Err(_) => std::ptr::null_mut(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_and_error_codes() {
        // 未运行时 stop 返回 0（幂等）
        assert_eq!(stop(), 0);
        assert_eq!(is_running(), 0);

        // 端口 0 走兜底
        assert_eq!(start(0), 0);
        assert_eq!(is_running(), 1);
        assert_eq!(running_port(), JNI_FALLBACK_PORT);

        // 重复启动返回 1
        assert_eq!(start(0), 1);
        assert_eq!(take_last_error(), "proxy already running");

        assert_eq!(stop(), 0);
        assert_eq!(is_running(), 0);
    }

    #[test]
    fn rfc3339_shape() {
        let s = now_rfc3339();
        assert_eq!(s.len(), 20, "unexpected: {s}");
        assert!(s.ends_with('Z'));
        assert_eq!(&s[4..5], "-");
        assert_eq!(&s[10..11], "T");
    }
}
