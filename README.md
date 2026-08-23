# GoProxyAndroid

面向 Android 场景的 Go 代理服务，主要用于 CatVod 相关项目中的本地代理能力。

这个仓库包含 Go 代理的核心源码与构建脚本，可用于产出：

- Android JNI 动态库：`libgoproxy.so`
- 兼容旧打包流程的 Android 可执行二进制

## 项目结构

- `main.go`：独立运行模式下的 HTTP 入口
- `proxy.go`：代理核心逻辑，负责分块下载、并发拉流、Range 透传与重试
- `proxy_jni.go`：JNI 桥接层，用于把 Go 代理编译成 Android 可加载的 `.so`
- `build_so.bat`：Windows 下编译 Android `.so`
- `build_so.sh`：Bash 环境下编译 Android `.so`
- `build.sh`：旧版独立可执行文件构建脚本，保留作兼容用途

## 使用场景

当前主用途是把 Go 代理编译为：

- `build/arm64-v8a/libgoproxy.so`
- `build/armeabi-v7a/libgoproxy.so`

随后再由上层 Android 项目将这些文件打包进资源目录并在运行时加载。

## 构建要求

- 64 位 Go 1.22 或更高版本
- Android NDK `r26b`

注意：

- 编译 Android JNI `.so` 时，请不要使用 32 位 Go 主机工具链（例如 `windows/386`）。
- 如果使用 32 位 Go，虽然构建过程可能通过，但运行时可能在 Go runtime 内部发生原生崩溃。

## 编译 Android `.so`

### Windows

```bat
build_so.bat
```

### Bash / Git Bash / WSL

```bash
chmod +x build_so.sh
./build_so.sh
```

编译完成后，输出文件位于：

```text
build/arm64-v8a/libgoproxy.so
build/armeabi-v7a/libgoproxy.so
```

## 兼容说明

- `build_so.bat` 和 `build_so.sh` 会在检测到同级 `CatVodSpider` 项目时，自动把生成的 `.so` 复制到 `CatVodSpider/jar/assets_so`。
- `build.sh` 仍然保留，用于旧方案下直接构建 Android 可执行代理文件，但当前主链路优先使用 JNI `.so` 方案。

## 代理能力说明

这个代理主要提供以下能力：

- 本地 HTTP 服务入口
- `Range` 请求解析与透传
- 基于分块的并发下载
- 下载失败自动重试
- `/health` 健康检查接口

## Rust 实现（src/，当前发布产物）

`src/` 是 Rust 重写版，产物即 `proxy/android-{arm64,arm}.so`，
导出符号与 Go 版逐字一致，Java 侧 `GoProxyLibrary` 无需改动。

- `src/lib.rs` —— JNI 桥 + 服务生命周期（对应 `proxy_jni.go`）
- `src/player.rs` —— 下载核心（对应 `proxy.go`）
- `src/main.rs` —— PC 调试入口（对应 `main.go`）

### 调度：流水线，而不是轮次栅栏

Go 版（以及 Rust 版初稿）是**轮次结构**：起 N 个块 → 全部收完 → 写出 → 下一轮。
在高码率片源上这个结构会明显拖速，原因有两条且会叠加：

1. **每轮耗时 = 该轮最慢那块的耗时。** 实测夸克 CDN 的 1MB 块耗时
   mean 1.17s / p90 1.60s / p99 1.95s，取 16 块的最大值，
   `E[max/mean] ≈ 1.51x`，等于白扔约 34% 带宽。
2. **下载与回写不重叠。** `yield` 会挂起直到播放器取走字节，
   于是一轮的 N×chunk 往播放器灌的整个过程中，网络上一个字节都没在下载。

现在改成**有界流水线**：`thread` 个 worker 通过一个原子游标各自领取下一块，
永不空转等同伴；接收端用序号重排后按序写出，顺序语义不变。

实测同一条链路传 128MB：栅栏 66 Mbps → 流水线 104 Mbps（链路上限约 108）。

### 内存有界与取消

look-ahead 用信号量把 **[在途 + 已下载待重排]** 一起夹在 `MAX_INFLIGHT_BYTES`（64MB）内。
只靠通道容量是不够的：接收端会把乱序块搬进重排缓冲从而腾空通道，
慢块（最坏 60s 超时）期间其余 worker 会一路狂奔，重排缓冲将无上限增长。

许可在领块**之前**获取，而领取游标单调递增，因此第 k 个许可对应第 k 个块；
已写出 `want` 块时累计发放 `slots + want` 个许可，而块 `want` 必落在前 `want+1` 次派发内，
所以"当前最小未写出块"一定已被派发 —— 不会自锁。

有一个坑值得单独记：**收工的 worker 必须归还许可再退出**。否则每个退出的 worker
吞掉一个许可，当 worker 数 > 槽位数时剩余 worker 会永久卡在 `acquire`，
它们持有的 `tx` 不释放 → 通道不关闭 → 接收端永远等不到结束 → 整个响应挂死。
`pipeline_no_deadlock_when_threads_exceed_slots` 这条带超时的单测就是钉它的。

`AbortOnDrop` 保证响应结束/客户端断开时中止所有在途任务；
Go 靠 context 传染取消，Rust 里 `tokio::spawn` 出去的任务必须显式 abort，
否则播放器 seek 之后旧分块仍在后台吃带宽。

### 相对 Go 版修掉的问题

- `downloadFirst` 的 `end = start + min(end+1, chunkSize)` 多加了一个 `start`，
  闭区间请求会多拉一段废字节，却仍按原范围声明 `Content-Range`
- `downloadChunk` 的 `defer resp.Body.Close()` 在重试循环内，连接延迟归还
- 端口默认值三处不一致（JNI 兜底 5576 / `main.go` 5575 / `/health` 硬编码 5575）
- 回写响应剔掉了 `Content-Length` 导致退化成 chunked，长度已知时显式声明对 ExoPlayer 更友好
- 绑 `:port`（全网卡）且无鉴权，同网段可把本机当免费转发器 → 收窄到 `127.0.0.1`
- `thread` / `chunkSize` 无上限，`?thread=256&chunkSize=8192` 会试图一次分配 2GB → 加 clamp

## 鸣谢

本项目基于不夜 `@sifanss` 分享的代理源码进行二次开发与整理。

在原有实现基础上，我补充和调整了 Android JNI `.so` 构建链路、仓库结构、文档说明以及与上层项目的集成方式。

## 开源说明

这个仓库只保留真正参与构建和发布的核心源码与脚本，不包含历史备份、实验目录、NDK 压缩包或上层业务项目文件。

## License

MIT
