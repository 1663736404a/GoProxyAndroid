//! 独立运行模式，对应 Go 版的 main.go。
//! 用于在 PC 上单独调试代理逻辑，不经过 JNI。
//!
//! 用法：proxy-standalone [port]

fn main() {
    let port = std::env::args()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(proxy::DEFAULT_PORT);

    match proxy::start(port) {
        0 => println!("服务器启动在 127.0.0.1:{port}"),
        1 => {
            eprintln!("服务已在运行");
            return;
        }
        2 => {
            eprintln!("端口绑定失败: {}", proxy::take_last_error());
            std::process::exit(1);
        }
        _ => {
            eprintln!("启动失败: {}", proxy::take_last_error());
            std::process::exit(1);
        }
    }

    // 主线程挂住，服务跑在后台线程里
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}
