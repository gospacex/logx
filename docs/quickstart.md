# logx Quickstart

零外部依赖的快速上手：本 quickstart 不需要 docker，复制粘贴即可跑通。

## 0. 前置

- Go ≥ 1.21
- 本 quickstart 全部示例仅输出到 stdout，无需任何外部组件

## 1. 安装与 provider 注册

```bash
go get github.com/gospacex/logx
```

logx 采用 **provider 注册机制**（根包不依赖具体实现）：使用 Zap 后端需空白导入注册默认 provider：

```go
import (
    "github.com/gospacex/logx"
    _ "github.com/gospacex/logx/zap" // 注册 provider + 字段归一化器
)
```

## 2. 最小可用日志

```go
func main() {
    cfg := logx.DefaultConfig() // info / console / stdout
    logger, err := logx.New(cfg)
    if err != nil {
        panic(err)
    }
    defer logger.Close()

    logger.Info("hello logx", logx.String("service", "demo"))
}
```

字段构造器：`logx.String` / `logx.Int` / `logx.Any` / `logx.Err`（也可混用 `zap.Field`）。

运行完整可执行版本：

```bash
cd logx/example/01-basic && go run .
```

输出包含：7 级别演示、`*Context` 方法、`With`/`Named` 派生、全局便捷函数（`logx.Info(...)`）。

## 3. 全局函数与配置

```go
logx.ReadConfig()   // 加载 configx 配置（HUBX_CONFIG 环境变量 > ./config.yaml）
logger := logx.GetGlobal()
logx.Info("使用全局 logger")  // Debug/Info/Warn/Error 及 *Context 版本
```

程序内直接构造配置（不依赖配置文件）：

```go
cfg := logx.DefaultConfig()
cfg.Level = "debug"
cfg.Output = "file"            // stdout / file / both
cfg.FilePath = "./logs/app.log"
cfg.MaxSize = 100             // MB，lumberjack 滚动
cfg.Compress = true
```

## 4. 链式 API 30 秒上手

```go
// 全局入口：键值交替字段（string 为 key，支持中文），无需 zap 构造函数
logx.L().Warn("慢查询", "latency", 800.0, "sql语句", "select * from order")

// 链式：Service/Operation/Tags 回退链（显式 > 配置 default_* > ServiceName）
logx.L().Service("order").Operation("/api/v1/order").Tags("slow").
    Info("下单", "order_id", "1001")

// 实例入口 + ctx（活动 span 时自动注入属性）
logger.Chain().WithContext(ctx).Service("order").Warn("...", "k", "v")
```

归一化边界：落单 string 追加到消息；坏键（key 位非 string）编码为 `bad-field.<N>`，不 panic。
运行完整版本：`go run ./example/05-chained-builder`。

## 5. 开启调用链（OTel）

需要调用链环境（一行启动）：

```bash
cd logx/example && docker compose up -d   # Jaeger(26686) + Kafka(39092)
```

开启后 `InfoContext(ctx, ...)` 自动将日志写入当前 Span 作为 **Span Event**，
并附加 `trace_id`/`span_id` 字段。导出目标：

| exporter | 说明 |
|---|---|
| `jaeger`（默认） | 直推 Jaeger（http://localhost:24268/api/traces） |
| `otlp` | OTLP HTTP（http://localhost:24318） |
| `stdout` | 本地调试，span 以 JSON dump |
| `kafka` | 直推 Kafka topic（`mq` 为别名） |

运行完整用例：`go run ./example/03-otel-tracing`（验证：http://localhost:26686 搜索 `logx-demo-otel`）。

## 下一步（完整用例索引）

| 用例 | 目的 | 前置条件 |
|---|---|---|
| `example/01-basic` | 级别/字段构造器/派生/全局函数 | 无 |
| `example/02-file-rotate` | lumberjack 滚动轮转 + JSON 格式 | 无 |
| `example/03-otel-tracing` | 各级别入链 + 手动/子 span | compose(jaeger) |
| `example/04-mq-kafka` | Kafka 直推 + 消费者回写 Jaeger | compose(全量) |
| `example/05-chained-builder` | 链式 API 全特性 + 归一化边界 | 无 |
| `example/06-http-middleware` | Hertz 请求级日志（middleware + common） | compose(jaeger) |
| `example/07-otlp-exporter` | OTLP HTTP 导出器（OTelConfig.OTLP 段，与 03 对照） | compose(jaeger) |

各用例的详细文档（前置条件、运行命令、验证方法、端点格式说明）：[`docs/examples.md`](examples.md)。

一键跑全流程：`bash example/run-demo.sh`（起环境 → 跑全部用例 → 验证清单）。

Hertz 请求级日志中间件（`logx/middleware` + `logx/common`）详见 README「Hertz 请求级日志中间件」一节。
