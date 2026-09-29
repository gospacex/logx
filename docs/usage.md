# logx 使用文档

统一日志 SDK 的完整使用手册。所有 x 系列中间件的统一日志入口，基于 **Zap + OpenTelemetry** 实现，支持调用链集成、文件滚动、MQ 异步推送与 Hertz 请求级日志。

> 本文档面向使用者，按「装 → 配 → 用 → 接调用链 → 接 MQ/中间件 → 排错」的顺序组织。
> 最小路径请直接看 [快速上手](#3-快速上手最小可运行)；全功能演示见 [用例索引](#14-使用用例索引)。
> 更轻量的零依赖入门：[docs/quickstart.md](quickstart.md)；分目录用例详单：[docs/examples.md](examples.md)。

---

## 目录

1. [功能特性](#1-功能特性)
2. [安装与 provider 注册](#2-安装与-provider-注册)
3. [快速上手（最小可运行）](#3-快速上手最小可运行)
4. [配置参考](#4-配置参考)
5. [Logger 接口](#5-logger-接口)
6. [字段构造器](#6-字段构造器)
7. [链式 Builder API](#7-链式-builder-api)
8. [全局函数](#8-全局函数)
9. [OTel 调用链集成](#9-otel-调用链集成)
10. [导出目标](#10-导出目标)
11. [MQ 异步推送](#11-mq-异步推送)
12. [Hertz 请求级日志中间件](#12-hertz-请求级日志中间件)
13. [context 注入与提取（common）](#13-context-注入与提取common)
14. [使用用例索引](#14-使用用例索引)
15. [常见问题与契约](#15-常见问题与契约)
16. [测试](#16-测试)

---

## 1. 功能特性

| 能力 | 说明 |
|---|---|
| 统一接口 | `logx.Logger`：7 级别（Debug/Info/Warn/Error/DPanic/Panic/Fatal）+ 各级 `*Context` 版本 |
| 多输出 | stdout / file（lumberjack 滚动）/ both；console（彩色）/ json 格式 |
| 文件滚动 | lumberjack 控制 `MaxSize`/`MaxBackups`/`MaxAge`/`Compress`，旧文件 gzip 压缩 |
| 调用链集成 | `logger.InfoContext(ctx, ...)` 自动把日志写入当前 Span 作为 **Span Event**，并附 `trace_id`/`span_id` |
| 字段 → 属性 | 所有日志字段自动转 OTel 属性（`db.*`、`http.*`、`log.*` 等命名规范由 x-sdk 保证） |
| 链式 API | `logx.L()` / `logger.Chain()` 键值交替变参（支持中文键），回退链兜底、Tags 并集去重、活动 span 属性注入 |
| 导出目标 | `jaeger`（Thrift 直推）/ `otlp`（OTLP HTTP）/ `stdout`（调试）/ `kafka`（MQ 直推，`mq` 为别名） |
| MQ 异步推送 | span → Kafka topic，消费者异步回写 Jaeger（`NewMQToJaegerConsumer` + `RunMQLoop`） |
| Hertz 中间件 | 请求级 logger + `X-Request-ID` + 访问日志 + 慢请求升级 WARN |
| 全局函数 | `logx.Info(...)` 等直接使用全局 logger，无需持有实例 |

---

## 2. 安装与 provider 注册

```bash
go get github.com/gospacex/logx
```

logx 采用 **provider 注册机制** 解耦根包与具体实现（避免 `logx → logx/zap → logx` 循环依赖）。
使用 Zap 后端必须空白导入注册默认 provider（同时注册 `...any` 字段归一化器）：

```go
import (
    "github.com/gospacex/logx"
    _ "github.com/gospacex/logx/zap" // 关键：注册 provider + 字段归一化器
)
```

> 未注册 provider 时：`logx.New` 返回明确错误；`logx.GetGlobal()` / `logx.L()` 回退到安全的
> no-op logger（不 panic、不阻塞），便于在装配完成前安全调用。

---

## 3. 快速上手（最小可运行）

零依赖、仅 stdout 的最小闭环：

```go
package main

import (
    "context"

    "github.com/gospacex/logx"
    _ "github.com/gospacex/logx/zap" // 注册 provider
)

func main() {
    cfg := logx.DefaultConfig() // info / console / stdout
    cfg.Level = "debug"

    logger, err := logx.New(cfg)
    if err != nil {
        panic(err)
    }
    defer logger.Close()
    logx.SetGlobal(logger) // 让 logx.Info(...) 与 logx.L() 走该实例

    logger.Info("hello logx", logx.String("service", "demo"))
    logger.InfoContext(context.Background(), "with context")
    logx.L().Warn("慢查询", "latency", 800.0, "sql语句", "select * from order")
}
```

字段构造器：`logx.String` / `logx.Int` / `logx.Any` / `logx.Err`（详见 [§6](#6-字段构造器)）。

---

## 4. 配置参考

配置对应 `config.yaml` 的 `logx:` 段，或由程序内 `logx.Config` 构造。两种来源都经 `FillDefaults()` 回落默认值。

### 4.1 默认值

| 字段 | 默认值 | 说明 |
|---|---|---|
| `Level` | `info` | debug / info / warn / error / dpanic / panic / fatal |
| `Format` | `console` | console（彩色 + 短 caller）/ json |
| `Output` | `stdout` | stdout / file / both |
| `FilePath` | `./logs/app.log` | Output=file/both 时生效 |
| `MaxSize` | `100` | 单文件上限（MB），lumberjack 滚动阈值 |
| `MaxBackups` | `3` | 保留的旧文件个数 |
| `MaxAge` | `7` | 旧文件保留天数 |
| `Compress` | `true` | 旧文件 gzip 压缩 |
| `Caller` | `true` | 记录调用位置 |
| `Development` | `false` | 开发模式（友好编码 + 堆栈） |
| `ServiceName` | `logx` | OTel 资源 `service.name` + 回退链末位兜底 |
| `ServiceVersion` | `1.0.0` | OTel 资源 `service.version` |
| `Environment` | `development` | OTel 资源 `deployment.environment` |
| `DefaultService` / `DefaultOperation` / `DefaultTags` | 空 | 链式 API 回退链兜底（见 [§7](#7-链式-builder-api)） |
| `OTel.Enabled` | `false` | 是否开启调用链 |
| `OTel.Exporter` | `jaeger` | jaeger / otlp / stdout / kafka（mq 别名） |
| `OTel.Sampler` | `parentbased_traceidratio` / `1.0` | 采样器 |

`OTelConfig` 若未单独指定，`ServiceName/ServiceVersion/Environment` 会继承顶层（`FillDefaults` 处理）。

`MQConfig` 默认：`Type=kafka`、`Topic=otel-traces`、`GroupID=logx-otel-consumer`、`MaxRetries=3`、`Timeout=5s`。

### 4.2 完整 YAML 示例

```yaml
logx:
  level: debug
  format: console              # console | json
  output: stdout               # stdout | file | both
  file_path: ./logs/app.log
  max_size: 100                # MB
  max_backups: 3
  max_age: 7
  compress: true
  caller: true
  service_name: myapp
  service_version: "1.0.0"
  environment: development

  # 链式日志 API 回退链兜底（Builder 缺省 Service/Operation/Tags）
  default_service: order
  default_operation: /api
  default_tags: [slow, demo]   # 与链式 Tags 并集去重

  otel:
    enabled: true
    exporter: jaeger           # jaeger | otlp | stdout | kafka(mq 别名)
    service_name: myapp        # 可选：覆盖顶层 ServiceName
    jaeger:
      endpoint: "http://localhost:14268/api/traces"   # 缺省回退 agent UDP 6831
    otlp:                      # Exporter=otlp 时生效
      endpoint: "http://localhost:4318"   # 完整 URL(http 自动 Insecure) 或 host:port
      insecure: false
      headers: {}
    sampler:
      type: parentbased_traceidratio      # always | never | traceidratio | parentbased_traceidratio
      ratio: 1.0

  mq:                          # otel.exporter: kafka 时生效
    type: kafka
    brokers: [localhost:39092]   # 缺 broker 时装配期 fail fast
    topic: otel-traces
    group_id: logx-otel-consumer
    max_retries: 3
    timeout: 5s
```

### 4.3 配置加载优先级

- `logx.ReadConfig(opts...)` 加载统一 `configx.Config` 句柄（所有 x 系列 SDK 共享）。
- 来源优先级：**显式 `ConfigOptions` > `HUBX_CONFIG` 环境变量 > `./config.yaml`**。
- `logx.GetConfig()` 取回该句柄；若未 `ReadConfig`，`logx.New` 使用传入的 `Config`（通常由 `DefaultConfig()` 起底）。

---

## 5. Logger 接口

`logx.Logger` 是统一抽象，Zap 后端实现它。方法分四类：

### 5.1 级别方法（非 Context）

```go
Debug(msg string, fields ...Field)
Info(msg string, fields ...Field)
Warn(msg string, fields ...Field)
Error(msg string, fields ...Field)
DPanic(msg string, fields ...Field)   // 开发模式 panic，生产模式按 error 输出
Panic(msg string, fields ...Field)    // 输出后 panic
Fatal(msg string, fields ...Field)    // 输出后 os.Exit
```

### 5.2 Context 方法

```go
DebugContext(ctx, msg, fields...)
InfoContext(ctx, msg, fields...)
WarnContext(ctx, msg, fields...)
ErrorContext(ctx, msg, fields...)
DPanicContext(ctx, msg, fields...)
PanicContext(ctx, msg, fields...)
FatalContext(ctx, msg, fields...)
```

`ctx` 含活动 recording span 时：日志写入该 span 作为 **Span Event**，并附加 `trace_id`/`span_id`（OTel 开启时）。
无 span 时静默降级，不影响日志本身输出。

### 5.3 派生与资源管理

```go
With(fields ...Field) Logger    // 固定字段，返回派生 Logger
Named(name string) Logger       // 名称前缀（如 request_id）
Level() zapcore.Level           // 当前级别
Sync() error                    // 刷新缓冲（文件输出有意义）
Close() error                   // 刷缓冲 + 关文件 + 关 TracerProvider
Zap() *zap.Logger               // 取底层 *zap.Logger（高级用法）
```

### 5.4 链式 / 回退链 / span 注入（接口尾部）

```go
Chain() *Builder                          // 绑定本实例的链式入口
DefaultService() string                   // 回退链：配置缺省 Service
DefaultOperation() string                 // 回退链：配置缺省 Operation
DefaultTags() []string                   // 回退链：配置缺省 Tags
ServiceName() string                      // 回退链末位兜底
InjectSpanAttrs(ctx, service, operation string, tags []string)  // 活动 span 属性注入
```

---

## 6. 字段构造器

避免调用方直接依赖 `zap`，提供 4 个构造器（`logx.Field` 即 `zap.Field` 别名）：

```go
logx.String("order_id", "1001")
logx.Int("amount_cents", 9950)
logx.Any("payload", map[string]any{"sku": "A-1", "qty": 2})
logx.Err(errors.New("stock insufficient"))
```

也可直接混用 `zap.Field` 构造器（如 `zap.Float64("amount", 99.5)`），二者兼容。

---

## 7. 链式 Builder API

### 7.1 入口

```go
logx.L()                       // 全局入口：末端输出走当前全局 Logger（未 SetGlobal 时自动建 stdout 实例）
logger.Chain()                 // 实例入口：绑定该 logger 实例
builder.Attach(logger)         // 后端 Chain() 内部调用，绑定输出目标
```

### 7.2 方法结构

- **中间方法（不输出，仅累积状态）**：`Service(name)` / `Operation(op)` / `Tags(tags ...string)` / `WithContext(ctx)`
- **末端方法（触发输出）**：`Debug / Info / Warn / Error / DPanic`，参数为键值交替的 `...any`

```go
// 全局直调：键值交替，无需 zap 构造函数
logx.L().Warn("慢查询", "latency", 800.0, "sql语句", "select * from order")

// 链式：显式 Service/Operation/Tags
logx.L().Service("order").Operation("/api/v1/order").Tags("demo", "slow", "mysql")
        .Info("下单", "order_id", "1001", "金额", 99.5)

// 实例 + ctx（ctx 含活动 span 时自动注入属性）
logger.Chain().WithContext(ctx).Service("order").Tags("slow")
        .Warn("curd错误", "db", "127.0.0.1:3306")
```

### 7.3 键值交替归一化规则（`...any` → 字段）

| 输入形态 | 处理 |
|---|---|
| `"key", value` 成对 | `key` 为字段名（**支持中文**），`value` 按 Go 类型映射（string/int*/uint*/float*/bool/Duration/Time/error/nil/其它） |
| `logx.Fields{...}`（`map[string]any`）元素 | 展开为各键值对 |
| `zap.Field` 元素 | 原样保留（与构造器混用） |
| 末尾**落单 string** | 作为追加消息片段（以空格拼进 `msg`） |
| 末尾**落单非 string** | 编码为 `arg.<N>` 兜底 |
| 坏键（key 位置非 string） | 编码为 `bad-field.<N>`，**不 panic** |
| `nil` 值 | `zap.Skip()`（跳过） |

`value` 的 Go 类型映射（`typedField`）：

```
string→String   int/int8..64→Int*   uint/uint8..64→Uint*
float32/64→Float*   bool→Bool   time.Duration→Duration   time.Time→Time
error→Error   nil→Skip   其它→Any
```

### 7.4 回退链与 Tags 并集去重

解析顺序：**显式（链式方法） > 配置 `default_*`（`DefaultService/Operation/Tags`） > `ServiceName` 兜底**。
Tags 为**并集去重**（链式在前、`DefaultTags` 在后，保序去重），输出为逗号拼接。

| 输出字段 | 来源 |
|---|---|
| `log.service` | 回退链解析后的 Service |
| `log.operation` | 回退链解析后的 Operation |
| `log.tags` | 并集去重后逗号拼接 |

### 7.5 活动 span 属性注入

末端方法携带 `ctx`（`WithContext(ctx)` 或走 `*Context`）时，若 `ctx` 含**活动 recording span**，自动 `span.SetAttributes`：

- `service.name`（回退链解析后的 Service）
- `operation`（回退链解析后的 Operation）
- `log.tags`（StringSlice，并集去重后）

`ctx` 无 span 或 span 非 recording 时**静默降级**（不报错、不阻塞），日志照常输出。

---

## 8. 全局函数

未持有实例时，直接走全局 logger（`SetGlobal` 注入；未注入时 `GetGlobal` 自动创建 stdout 实例或 no-op）：

```go
logx.Debug(msg, fields...)   logx.Info(...)   logx.Warn(...)   logx.Error(...)
logx.DebugContext(ctx, msg, fields...)   // 及 InfoContext/WarnContext/ErrorContext/
                                          // DPanicContext/PanicContext/FatalContext
logx.CloseGlobal()           // 关闭并清空全局句柄
```

配置与装配相关：

```go
logx.ReadConfig(opts...)    // 加载 configx 句柄
logx.GetConfig()            // 取回句柄
logx.New(cfg)               // 按配置创建 logger（走默认 provider）
logx.SetGlobal(l) / logx.GetGlobal()   // 全局句柄
logx.RegisterProvider(p) / logx.SetDefaultProvider(name)   // provider 注册
```

---

## 9. OTel 调用链集成

开启（`otel.enabled: true`）后，`*Context` 方法或带 `ctx` 的链式日志会：

1. 将日志作为 **Span Event** 写入当前 span（`span.AddEvent`）；
2. 输出到 Zap（stdout/file），并附加 `trace_id`/`span_id` 字段。

```go
cfg := logx.DefaultConfig()
cfg.OTel.Enabled = true
cfg.OTel.Exporter = "stdout"   // 本地调试：span 以 JSON dump
logger, _ := logx.New(cfg)

setup, _ := zapimpl.SetupTracerProvider(cfg.OTel)
ctx, span := setup.Provider.Tracer(cfg.ServiceName).Start(context.Background(), "place-order")
defer span.End()

logger.InfoContext(ctx, "order created",
    logx.String("order_id", "1001"),
    logx.Any("amount", 99.5),
)
// Span Event: { name:"order created", attrs:{ log.level:info, order_id:"1001", amount:99.5 } }
```

装配入口与资源管理：

```go
setup, err := zapimpl.SetupTracerProvider(cfg.OTel)            // jaeger/otlp/stdout
setup, err := zapimpl.SetupTracerProviderWithMQ(cfg.OTel, mq)  // kafka（需传 MQConfig）
defer setup.Provider.Shutdown(context.Background())
```

`SetupTracerProvider*` 会自动 `otel.SetTracerProvider` + 设置全局 TextMapPropagator（TraceContext + Baggage）。

---

## 10. 导出目标

`otel.exporter` 取值与装配：

| 取值 | 说明 | 端点配置 |
|---|---|---|
| `jaeger`（默认） | Thrift 直推 collector / agent | `otel.jaeger.endpoint`（如 `http://localhost:14268/api/traces`）；缺省回退 agent UDP `localhost:6831` |
| `otlp` | OTLP HTTP 推送 | `otel.otlp.endpoint`；支持完整 URL / host:port（下表） |
| `stdout` | 本地调试，span JSON dump | 无 |
| `kafka`（`mq` 别名） | span → Kafka topic 直推 | `mq.*`（缺 `mq.brokers` 时 **fail fast**） |

**OTLP 端点格式**（`newOTLPExporter`）：

| 配置值 | 路由 | 说明 |
|---|---|---|
| `http://localhost:4318` | `WithEndpointURL` + 自动 Insecure | 完整 URL（http 非 TLS） |
| `https://host:4318` | `WithEndpointURL` | 完整 URL（TLS） |
| `localhost:4318` | `WithEndpoint` | host:port，`Insecure` 需显式配置 |

> Jaeger 容器映射：collector Thrift `:14268`（宿主常映射为 `:24268`）、OTLP HTTP `:4318`（宿主 `:24318`）、UI `:16686`（宿主 `:26686`）。示例用例端点用 `:24xxx` 宿主端口。

**Kafka 装配**（`otel.exporter: kafka`）：

```go
mqCfg := logx.MQConfig{Type: "kafka", Brokers: []string{"localhost:39092"}, Topic: "otel-traces"}
setup, err := zapimpl.SetupTracerProviderWithMQ(cfg.OTel, mqCfg)
```

---

## 11. MQ 异步推送

全链路：**span → Kafka topic（生产直推）→ 消费者 goroutine Kafka → OTLP HTTP → Jaeger**。

```
组件：
  MessageQueue          生产者/消费者抽象（内置 Kafka 实现，可扩展 Pulsar）
  NewKafkaProducer(cfg) 生产者（同步写，保证可靠投递；缺 Brokers fail fast）
  NewKafkaConsumer(cfg) 消费者（按 GroupID 位点消费）
  NewMQExporter(queue)   SpanExporter：span 序列化 JSON 写入 topic
  NewMQTracerProvider    用 MQ exporter 装配全局 Provider（强制路由 mq）
  NewMQToJaegerConsumer  消费者：读 span JSON → OTLP proto → 写 Jaeger
  RunMQLoop(ctx)         消费循环（坏消息跳过不断链，ctx 取消退出）
```

典型装配：

```go
mqCfg := logx.MQConfig{Type: "kafka", Brokers: brokers, Topic: "otel-traces",
    GroupID: "logx-demo-mq-" + suffix}          // 带随机后缀，位点全新

// 生产者直推
prod, _ := zapimpl.NewKafkaProducer(mqCfg)
setup, _ := zapimpl.NewMQTracerProvider(cfg.OTel, mqCfg)

// 消费者回写
kq, _ := zapimpl.NewKafkaConsumer(mqCfg)
consumer, _ := zapimpl.NewMQToJaegerConsumer(kq, "http://localhost:24318")
loopCtx, cancel := context.WithCancel(context.Background())
go func() { defer cancel(); _ = consumer.RunMQLoop(loopCtx) }()
```

**关键时序（生产环境注意）**：
- **先起消费循环并等 group join（约 10s）再产出 span**——新消费组起始位点为 LastOffset，span 先写会被本组永久跳过。
- `GroupID` 带随机后缀，每次运行位点全新。
- 消费端对坏消息（JSON 解析失败 / trace_id 长度非 32 hex / span_id 非 16 hex）跳过并计数，不断链；转发失败（网络/4xx）才返回 error。
- span 消息随 resource 属性传输 `service.name`，消费端回写 Jaeger 时正确归组；旧格式（无 resource）回退 `logx-mq-legacy`。

---

## 12. Hertz 请求级日志中间件

`logx/middleware`：每个请求生成 16 位 hex `request_id`，回写 `X-Request-ID` 响应头，
并通过 `common.WithLogger` 把 `Named(reqID)` 的 logger 注入 ctx。

```go
import (
    logxhertz "github.com/gospacex/logx/middleware"
    "github.com/gospacex/logx/common"
)

h := server.New(server.WithHostPorts(":8080"))
h.Use(logxhertz.Middleware(&logxhertz.MiddlewareConfig{
    Logger:        logger,        // 请求级底座；nil 时按请求回退 logx.GetGlobal()
    SlowThreshold: time.Second,   // >0 时 duration 超阈值升级 WARN + http.slow
    // DisableVisitLog: false,    // 缺省输出访问日志；true 关闭（request_id 注入仍保留）
}))

h.GET("/hello", func(ctx context.Context, c *app.RequestContext) {
    lg := common.GetLogger(ctx)          // 请求级 logger（名字 = request_id）
    _ = logxhertz.GetRequestID(c)       // 取当前请求 id
    lg.InfoContext(ctx, "business logic")
    c.String(200, "ok")
})
```

**访问日志字段**：`http.method` / `http.route` / `http.request.id` / `http.response.status_code` / `http.response.duration`。
`duration > SlowThreshold`（>0）时升级 **WARN** 并附 `http.slow: true`，消息为 `slow http request`（正常为 `http request`）。

`MiddlewareConfig` 字段：

| 字段 | 缺省 | 说明 |
|---|---|---|
| `Logger` | nil → 按请求回退全局 | 请求级 logger 底座 |
| `SlowThreshold` | 0（不标记） | 慢请求阈值 |
| `DisableVisitLog` | false（输出） | 关闭访问日志开关 |

---

## 13. context 注入与提取（common）

```go
ctx = common.WithLogger(ctx, reqLogger)   // 注入（中间件 / 框架边界调用）
lg    := common.GetLogger(ctx)            // 提取；未注入/ctx==nil 时回退全局 logger
lg, ok := common.TryGetLogger(ctx)        // 提取；未注入返回 (nil, false)
```

请求级 logger 的「名字 = request_id」使日志天然可检索、可与 `X-Request-ID` 关联。

---

## 14. 使用用例索引

完整可运行用例位于 `example/`（每目录独立 `main.go`，package main）：

| 用例 | 目录 | 前置 | 演示 |
|---|---|---|---|
| 01 基础日志 | `example/01-basic` | 无 | 7 级别、字段构造器、派生、全局函数 |
| 02 文件轮转 | `example/02-file-rotate` | 无 | lumberjack 滚动 + 压缩 + JSON + Error 堆栈 |
| 03 OTel 调用链 | `example/03-otel-tracing` | Jaeger | TracerProvider + Span Event + 双模式配置 |
| 04 MQ 异步 | `example/04-mq-kafka` | Jaeger+Kafka | Kafka 直推 + 消费者回写 Jaeger |
| 05 链式 Builder | `example/05-chained-builder` | 无 | 回退链 / 并集去重 / 归一化边界 / 无 span 降级 |
| 06 Hertz 中间件 | `example/06-http-middleware` | Jaeger | 请求级 logger + 访问日志 + 慢请求 |
| 07 OTLP 导出 | `example/07-otlp-exporter` | Jaeger(OTLP) | `otel.otlp.endpoint` 配置段 + OTLP HTTP 通路 |

运行与验证细节（前置条件 / 运行命令 / 验证方法 / 端点格式）：[docs/examples.md](examples.md)。
一键全流程：`bash example/run-demo.sh`（起环境 → 跑 7 用例 → 验证清单）。

每个用例都有对应的 `go test`（镜像其 API 契约，零外部依赖 + 门控集成测试）：

```bash
go test ./...   # 单测 + 用例测试（门控测试无 docker 时按设计 skip）
```

门控集成测试环境变量：`HUBX_JAEGER_IT=1`（03）、`LOGX_KAFKA_BROKERS`（04）、`LOGX_OTLP_ENDPOINT`（07）。

---

## 15. 常见问题与契约

**Q: `Sync()` 对 stdout 报错 `bad file descriptor`？**
预期。stdout sink 无缓冲可刷，报错正常；只有文件输出（lumberjack）`Sync` 才真正刷缓冲。

**Q: 日志 caller 指向 `zap/zap.go` 而非调用点？**
已修复。Zap 后端为 Context 通路单独维护了 caller skip +1 的实例（`write()` 多一层帧），
保证 `InfoContext` 与 `Info` 的 caller 归因一致（都指向用户调用处）。回归测试 `TestContextCallerAttribution` 覆盖。

**Q: 未注册 provider 会 panic 吗？**
不会。`GetGlobal()`/`L()` 回退 no-op logger；`New` 返回明确错误提示需 `import _ "github.com/gospacex/logx/zap"`。

**Q: `otel.exporter: kafka` 但没配 `mq.brokers`？**
装配期 **fail fast**（`otel kafka exporter: no brokers configured`），不在运行期静默丢 span。

**Q: 中文能作日志字段 key 吗？**
能。链式 `...any` 与 `logx.Fields` 的 key 均支持中文（经 `typedField` 原样保留）。

**Q: 坏键（key 位非 string）会 panic 吗？**
不会。编码为 `bad-field.<N>` 并记录两侧值，保证输出健壮。

**Q: 无 span 的 ctx 走 `WithContext` 会阻塞吗？**
不会。span 属性注入静默降级（无 recording span 直接跳过），日志照常输出。

---

## 16. 测试

```bash
# 需本机 GOROOT 正常；若 GOROOT 异常，见下
export GOROOT=/usr/local/Cellar/go/<ver>/libexec GOTOOLCHAIN=local
cd logx
go test -race ./...
```

覆盖：根包（Builder/config/Logger）、`common`、`middleware`、`zap`（含 caller 归因、Span Event、MQ roundtrip、OTLP 装配）与各用例测试。门控集成测试需起 `docker compose`（Jaeger/Kafka）。

---

*文档对应 logx 当前公开 API。API 变更请同步更新本文与 [README.md](../README.md)。*
