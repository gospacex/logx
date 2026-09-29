# logx

统一日志 SDK：所有 x 系列中间件的统一日志入口，基于 **Zap + OpenTelemetry** 实现，支持调用链集成。

## 功能特性

- **统一日志接口**：`logx.Logger`（Debug/Info/Warn/Error + Context 版本）
- **多输出**：stdout / file（lumberjack 滚动）/ both，console / json 格式
- **OTel 调用链集成**：`logger.InfoContext(ctx, ...)` 自动将日志写入当前 Span 作为 **Span Event**（含 `trace_id`、`span_id`、`timestamp`、日志字段）
- **调用链属性注入**：所有日志字段自动转换为 OTel 属性（`db.*`、`http.*`、`logging.*` 等命名规范由各 x-sdk 保证）
- **数据推送目标**：
  - `jaeger`：直接推送到 Jaeger collector / agent
  - `otlp`：OTLP HTTP 推送
  - `mq`：异步推送到 MQ（Kafka），由消费者写入 Jaeger
  - `stdout`：本地调试（测试环境）
- **Hertz 请求级日志中间件**：为每个请求创建带请求信息的 logger，注入 context，自动记录访问日志与慢请求
- **全局便捷函数**：`logx.Info(...)` 等直接使用全局 logger

## 安装

```bash
go get github.com/gospacex/logx
```

> logx 采用 provider 注册机制避免包循环依赖。
> 使用前需空白导入 Zap 实现注册默认 provider：
>
> ```go
> import _ "github.com/gospacex/logx/zap"
> ```

## 快速开始

```go
package main

import (
	"context"

	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册 provider
	"go.uber.org/zap"
)

func main() {
	cfg := logx.DefaultConfig()
	cfg.Level = "debug"
	cfg.Output = "stdout"

	logger, err := logx.New(cfg)
	if err != nil {
		panic(err)
	}
	defer logger.Close()
	logx.SetGlobal(logger)

	logger.Info("hello", zap.String("k", "v"))
	logger.InfoContext(context.Background(), "with context")
}
```

## 配置

对应 `config.yaml` 中的 `logx:` 段（**顶层**，不在 `xmiddleware` 下）：

```yaml
logx:
  level: debug
  format: console          # console | json
  output: stdout           # stdout | file | both
  file_path: ./logs/app.log
  max_size: 100            # MB
  max_backups: 3
  max_age: 7
  compress: true
  caller: true
  # 链式日志 API 缺省段（Builder 回退链兜底，详见下文）
  default_service: order   # 缺省 Service（log.service）
  default_operation: /api  # 缺省 Operation（log.operation）
  default_tags: [slow, demo]  # 缺省 Tags（与链式 Tags 并集去重，log.tags 逗号拼接）
  otel:
    enabled: false
    exporter: jaeger       # jaeger | otlp | stdout | kafka（mq 为别名）
    service_name: myapp
    environment: development
    jaeger:
      endpoint: "http://localhost:14268/api/traces"
    otlp:                     # Exporter=otlp 时生效
      endpoint: "http://localhost:4318"   # 完整 URL 或 host:port（http URL 自动 Insecure）
    sampler:
      type: parentbased_traceidratio
      ratio: 1.0
```

### 配置入口三件套

与其余 x 系列 SDK 同签名（`T` = `Logger`）：

```go
// 一步到位：读配置 → 取 logx 段 → 建实例
l, err := logx.ReadAndCreate(configx.ConfigOptions{ConfigFile: "config.yaml"})

// 失败即退出进程
l := logx.MustReadAndCreate(configx.ConfigOptions{})

// 只解析配置：写入全局句柄，供 GetConfig 取回
err := logx.ReadConfig(configx.ConfigOptions{ConfigFile: "config.yaml"})
```

`ReadAndCreate` 内部复用本包 `ReadConfig`，因此调用后 `logx.GetConfig()` 同样可用。
想让它成为进程默认日志句柄（`logx.GetGlobal()` 及各 SDK 的缺省 logger）时再调 `logx.SetGlobal(l)`。

> 此前从配置驱动的路径是"手工三步"：`ReadConfig()` → `GetConfig().Unmarshal("logx", &cfg)`
> → `New(cfg)`。`ReadAndCreate` 就是把这三步收成一个入口；旧写法仍可用。

## 链式日志 API

`logx.L()` 全局入口 / `logger.Chain()` 实例入口。中间方法（`Service`/`Operation`/`Tags`/`WithContext`）不输出，末端各级别方法输出。

```go
// 全局直调：键值交替变参字段（"k", v 成对），无需 zap 构造函数
logx.L().Warn("慢查询", "latency", 800.0, "sql语句", "select * from order")

// 链式：显式 Service/Operation/Tags
logx.L().Service("order").Operation("/api/v1/order").Tags("slow", "mysql")
	.Info("curd错误", "db", "127.0.0.1:3306")

// 实例 + ctx（活动 span 属性注入）
logger.Chain().WithContext(ctx).Service("order").Warn("...", "k", "v")
```

**键值交替字段语法**（`...any` 变参）：

- `"key", value` 成对出现，value 按 Go 类型映射（string/int/bool/Duration/error/…）；**key 支持中文**
- 末尾落单 `string`：追加到消息（空格连接）；落单非 string：编码为 `arg.<N>` 兜底
- 坏键（key 位置非 string）：编码为 `bad-field.<N>`，不 panic
- `logx.Fields`（`map[string]any`）元素自动展开为各键值对
- `zap.Field` 元素原样保留——可与 `zap.String` 等构造函数混用

```go
logx.L().Info("下单",
	"order_id", "1001",                       // 键值对
	logx.Fields{"user": "u-1", "amount": 99.5}, // map 展开
	zap.String("legacy", "x"),                // 与构造函数混用
	"2012.11.22",                              // 落单 string → 追加消息
)
```

## Service / Operation / Tags 与配置默认值

回退链：**显式（链式方法） > 配置 `default_*`（`DefaultService/DefaultOperation/DefaultTags`） > `ServiceName` 兜底**。
Tags 为**并集去重**（链式 Tags 在前，`DefaultTags` 在后，保序去重）。

对应 yaml 配置段（`logx:` 下）：

```yaml
logx:
  default_service: demo-svc      # 无链式 Service 时兜底 → log.service
  default_operation: /api/v1     # 无链式 Operation 时兜底 → log.operation
  default_tags: [demo, slow]     # 与链式 Tags 并集去重 → log.tags（逗号拼接）
```

输出字段：`log.service` / `log.operation` / `log.tags`。

## 导出目标

`logx.otel.exporter` 取值：

| 取值 | 说明 |
|---|---|
| `jaeger`（默认） | 直推 Jaeger collector / agent |
| `otlp` | OTLP HTTP 推送（`otel.otlp.endpoint` 支持完整 URL `http://host:4318` 或 `host:port`；http URL 自动 Insecure） |
| `stdout` | 本地调试 |
| `kafka` | 直推 Kafka topic（复用 `mq` 配置段）；**`mq` 为别名** |

Kafka 配置示例（`logx:` 下 `mq:` 段；`otel.exporter: kafka` 时生效）：

```yaml
logx:
  otel:
    enabled: true
    exporter: kafka
  mq:
    type: kafka
    brokers: [localhost:39092]   # 缺 broker 时 fail fast（构造即报错）
    topic: otel-traces
    group_id: logx-otel-consumer
```

代码装配：`zap.SetupTracerProviderWithMQ(cfg.OTel, cfg.MQ)`；仅 `jaeger/otlp/stdout` 时 `zap.SetupTracerProvider(cfg.OTel)` 即可（kafka 路由会因缺 `mq.brokers` 而 fail fast，报 `no brokers configured` 明确错误）。

## Span 属性注入

带 `ctx` 的链式日志（`Chain().WithContext(ctx)` 或 `*Context` 方法）在 ctx 含**活动 recording span** 时自动 `span.SetAttributes`：

- `service.name`（回退链解析后的 Service）
- `operation`（回退链解析后的 Operation）
- `log.tags`（StringSlice，并集去重后）

ctx 无 span 或 span 非 recording 时**静默降级**（不报错、不阻塞输出），日志本身仍正常写入 Zap 与 Span Event（若 OTel 开启）。

## OTel 调用链集成

开启 OTel 后（`logx.otel.enabled: true`），`InfoContext(ctx, msg, fields...)` 会：

1. 将日志作为 **Span Event** 写入当前调用链（`span.AddEvent`）
2. 输出到 Zap（stdout/file），并附加 `trace_id`、`span_id` 字段

```go
ctx, span := tracer.Start(context.Background(), "place-order")
defer span.End()

logger.InfoContext(ctx, "order created",
	zap.String("order_id", "1001"),
	zap.Float64("amount", 99.5),
)
// Span Event: { name: "order created", attributes: { log.level: info, order_id: 1001, amount: 99.5 } }
```

## MQ 异步推送

```go
cfg.OTel.Exporter = "mq"
mqCfg := logx.MQConfig{
	Type:    "kafka",
	Brokers: []string{"localhost:9092"},
	Topic:   "otel-traces",
}

setup, err := zapimpl.NewMQTracerProvider(cfg.OTel, mqCfg)
// 异步消费者将 MQ 中的 span 写入 Jaeger（span JSON 随消息传输 resource 属性，
// Jaeger 按 service.name 归组；旧格式消息回退 logx-mq-legacy）
consumer, _ := zapimpl.NewMQToJaegerConsumer(queue, "http://jaeger:4318") // queue: 消费侧 MessageQueue（zapimpl.NewKafkaConsumer）
go consumer.RunMQLoop(ctx)
```

## Hertz 请求级日志中间件

`logx/middleware` 中间件：每个请求生成 16 位 hex `request_id`，回写 `X-Request-ID` 响应头，
并通过 `common.WithLogger` 将 `Named(request_id)` 的 logger 注入 ctx（handler 内
`common.GetLogger(ctx)` 提取；请求 id 可用 `logxhertz.GetRequestID(c)` 获取）。
请求结束输出访问日志（`http.method`/`http.route`/`http.request.id`/`http.response.status_code`/
`http.response.duration`）；duration 超过 `SlowThreshold`（>0）时升级为 WARN 并附 `http.slow` 字段。
缺省即记录访问日志与慢请求；`MiddlewareConfig.DisableVisitLog=true` 可关闭访问日志（request_id 注入仍保留）。

```go
import (
	logxhertz "github.com/gospacex/logx/middleware"
	"github.com/gospacex/logx/common"
)

h := server.New(server.WithHostPorts(":8080"))
h.Use(logxhertz.Middleware(&logxhertz.MiddlewareConfig{
	Logger:        logger,
	SlowThreshold: time.Second,
}))

h.GET("/hello", func(ctx context.Context, c *app.RequestContext) {
	logger := common.GetLogger(ctx)  // 请求级 logger（名字 = request_id）
	logger.InfoContext(ctx, "business logic")
	c.String(200, "ok")
})
```

完整演示：[`example/06-http-middleware/main.go`](example/06-http-middleware/main.go)（慢请求升级 WARN 与 X-Request-ID 贯穿）。

## 更多示例

参见 [`example/main.go`](example/main.go)，运行：

```bash
cd example && go run .
```

快速上手（零依赖可跑通的最小路径 + 用例索引）：[`docs/quickstart.md`](docs/quickstart.md)；
全量用例参考手册（前置条件 / 运行 / 验证方法 / 端点格式）：[`docs/examples.md`](docs/examples.md)；
一键演示全流程（起环境 → 跑 7 用例 → 验证）：`bash example/run-demo.sh`。

## 测试

```bash
go test -race ./...
```

## 目录结构

```
logx/
├── config.go       配置结构（日志 + OTel + MQ）
├── logx.go         统一 Logger 接口 + provider 注册 + 全局句柄
├── builder.go      链式 Builder（L() / Chain() 入口，键值交替字段归一化）
├── common/         context 注入/提取 logger
├── zap/
│   ├── zap.go      Zap 实现
│   ├── fields.go   键值交替 ...any → []zap.Field 归一化
│   ├── otel.go     OTelCore（zapcore.Core：日志 → Span Event）
│   ├── exporter.go Jaeger / OTLP / stdout TracerProvider
│   └── exporter_mq.go MQ 异步推送（Kafka）+ MQ → Jaeger 消费者
├── middleware/     Hertz 请求级日志中间件（request id + 访问日志 + 慢请求）
├── docs/
│   ├── quickstart.md  零依赖快速上手
│   └── examples.md    全量用例参考手册（01–07 前置/运行/验证）
└── example/        使用案例
    ├── main.go               综合演示（各级别入链 / 链式 API / MQ / Hertz）
    ├── 01-basic/             零依赖基础日志
    ├── 02-file-rotate/       文件滚动轮转（lumberjack）
    ├── 03-otel-tracing/      OTel 调用链集成（需 compose）
    ├── 04-mq-kafka/          Kafka 直推 + 回写 Jaeger（需 compose）
    ├── 05-chained-builder/   链式 API 全特性
    ├── 06-http-middleware/   Hertz 请求级日志（需 compose）
    ├── 07-otlp-exporter/     OTLP HTTP 导出器（需 compose）
    └── run-demo.sh           一键演示（起环境 → 跑用例 → 验证）
```