# 使用用例索引（example/ 01–07）

每个用例目录一个独立 `main.go`（package main），按功能矩阵 6 大类拆分，
可单独运行也可用一键脚本串起来。与 `docs/quickstart.md`（零依赖最小路径）互补：
本文档是「全量用例 + 前置条件 + 验证方法」的参考手册。

## 总览

| # | 用例目录 | 功能矩阵 | 前置条件 | 核心 API |
|---|---|---|---|---|
| 01 | `01-basic` | §1 统一接口 / §2 输出 | 无 | 级别、字段构造器、派生、全局函数 |
| 02 | `02-file-rotate` | §2 lumberjack 滚动 | 无 | 文件 Tee + 压缩轮转 |
| 03 | `03-otel-tracing` | §3 OTel（jaeger 直推） | compose：jaeger | TracerProvider + Span Event |
| 04 | `04-mq-kafka` | §4 MQ 异步 | compose：全量 | 生产者直推 + 消费者回写 |
| 05 | `05-chained-builder` | §5 链式 Builder | 无 | 键值交替 + 回退链 + span 注入 |
| 06 | `06-http-middleware` | §6 Hertz 中间件 | compose：jaeger | 请求级 logger + 访问日志 |
| 07 | `07-otlp-exporter` | §3 OTLP HTTP 通路 | compose：jaeger | `otel.otlp.endpoint` |

一键全流程：`bash example/run-demo.sh`（起环境 → 跑 7 用例 → 验证清单）。

---

## 01 · 基础日志（零依赖）

**目的**：功能矩阵 §1/§2 的最小闭环——7 级别、`*Context`、字段构造器、
`With`/`Named` 派生、全局函数、`Level`/`Sync`/`Close`。

**运行**：

```bash
cd example/01-basic && go run .          # 或从 logx 根：go run ./example/01-basic
```

**覆盖**：`logx.String/Int/Any/Err` 构造器、`logger.With(...).Named(...)`、
`logx.Info/Warn` 全局函数（经 `SetGlobal`）、stdout console 彩色输出。

**验证**：终端逐行输出；注意 `Sync()` 对 stdout sink 报
`bad file descriptor` 属预期（文件输出才有缓冲可刷）。

---

## 02 · 文件滚动轮转（零依赖）

**目的**：§2 输出——`Output=both`（stdout + 文件 Tee）、json 格式、
lumberjack 滚动（用例专用 `MaxSize=1MB` 快速触发）、压缩轮转。

**运行**：

```bash
cd example/02-file-rotate && go run .
ls logs/    # app.log + app-<时间戳>.log.gz（lumberjack 轮转文件命名带时间戳）
```

**验证**：写 4096 行 ~1KB 日志触发 4 次以上轮转；`logs/` 下出现
`app-*.log.gz` 即轮转 + 压缩成功；`Error` 级日志自动附带堆栈。

---

## 03 · OTel 调用链（jaeger 直推）

**目的**：§3 各级别日志入链（Span Event）+ 手动 root/子 span +
**双模式配置**（`HUBX_CONFIG` yaml vs 程序内构造）。

**前置**：

```bash
cd example && docker compose up -d   # 仅需 Jaeger（UI 26686 / collector 24268）
```

**运行**：

```bash
cd example/03-otel-tracing
go run .                                  # 默认：程序内构造 cfg
HUBX_CONFIG=$PWD/config.yaml go run .     # 或：读 config.yaml（logx: 段）
LOGX_JAEGER_ENDPOINT=http://host:24268/api/traces go run .   # 覆盖端点
```

**验证**：Jaeger UI（http://localhost:26686）搜 `logx-demo-otel`：
span `all-levels-demo` 含 5 个日志事件 + 子 span `deduct-stock`；
终端日志行带 `trace_id`/`span_id` 字段。

---

## 04 · MQ 异步推送（Kafka 直推 + 回写 Jaeger）

**目的**：§4 全链路——fail-fast（缺 brokers 装配期报错）、
span → Kafka topic 直推、消费者 goroutine Kafka → OTLP → Jaeger。

**前置**：

```bash
cd example && docker compose up -d   # Jaeger + Kafka（39092）
```

**运行**：

```bash
cd example/04-mq-kafka
go run .
LOGX_KAFKA_BROKERS=h1:9092,h2:9092 go run .      # 多 broker（逗号分隔）
LOGX_OTLP_ENDPOINT=http://host:24318 go run .    # 覆盖回写端点
```

**关键时序**（用例内已处理，读懂代码时注意）：

- 先起消费循环、等 group join 完成（10s）再产出 span——新消费组起始位点
  为 LastOffset，span 先写入会被本组永久跳过
- `GroupID` 带随机后缀（`logx-demo-mq-<8hex>`），每次运行位点全新
- 消费端 `RunMQLoop` 对坏消息（短 trace_id 等）跳过不断链

**验证**：终端 `span flushed to kafka`（生产直推证据）+
Jaeger UI 搜 `logx-demo-mq` 见本次 span（service.name 随消息 resource 字段还原）。

---

## 05 · 链式 Builder 全特性（零依赖）

**目的**：§5 全量特性——`L()`/`Chain()`/`Attach`、键值交替 `...any`
（中文 key、Go 类型映射、`logx.Fields` 展开、`zap.Field` 混用）、
归一化边界（落单 string 追加消息、落单非 string → `arg.<N>`、
坏键 → `bad-field.<N>` 不 panic）、Service/Operation/Tags 回退链
（显式 > `default_*` > `ServiceName`）+ Tags 并集去重、
活动 span 属性注入（`service.name`/`operation`/`log.tags`）。

**运行**：

```bash
cd example/05-chained-builder && go run .
```

**验证**（零外部依赖，`OTel.Exporter=stdout` 把 span dump 到终端）：

- 未显式 Service/Tags 时输出 `log.service=demo-svc`（配置兜底）、
  `log.tags=demo,slow,mysql`（并集去重保序）
- 终端 span JSON dump 含注入的 `service.name`/`operation` 属性
- 无 span 时 `WithContext(context.Background())` 静默降级不报错

---

## 06 · Hertz 请求级日志中间件

**目的**：§6 `logx/middleware` + `logx/common`——每请求生成 16 位 hex
`request_id`（回写 `X-Request-ID` 响应头）、`Named(request_id)` logger
经 context 注入 handler、访问日志（`http.method/route/request.id/
response.status_code/response.duration`）、慢请求（>500ms）升级 WARN +
`http.slow`、请求内 OTel span（装配失败优雅降级，日志保持可用）。

**前置**：compose 起 jaeger（同 03）。

**运行**：

```bash
cd example/06-http-middleware && go run .
LOGX_DEMO_PORT=8081 go run .    # 覆盖端口（默认 127.0.0.1:8080）

# 另一终端：
curl -i http://127.0.0.1:8080/fast   # 响应头 X-Request-ID；访问日志 INFO
curl -i http://127.0.0.1:8080/slow   # 800ms > 500ms 阈值：访问日志 WARN + http.slow
```

**验证**：日志行 logger 名 = 对应 request_id；handler 内
`common.GetLogger(ctx)` 取到请求级 logger、`logxhertz.GetRequestID(c)`
取请求 id；Jaeger UI 见 `logx-demo-http` 的 `http-handler` span。
注意：`/slow` 只注册了 GET，须真实 GET 才触发慢路径（HEAD 为 404 且不打慢日志）。

---

## 07 · OTLP HTTP 导出器（与 03 对照）

**目的**：§3 导出目标中 `otlp` 通路——验证 `logx.otel.otlp.endpoint`
配置段。与 03 的 jaeger 通路（Thrift collector）互为对照：
span → OTLP HTTP → Jaeger。

**前置**：compose 起 jaeger（OTLP HTTP 24318）。

**运行**：

```bash
cd example/07-otlp-exporter && go run .
LOGX_OTLP_ENDPOINT=http://host:4318 go run .    # 覆盖 OTLP 端点
```

**端点格式说明**（`zap/exporter.go` 的 `newOTLPExporter`）：

| 配置值 | 路由 | 说明 |
|---|---|---|
| `http://localhost:4318` | `WithEndpointURL` + 自动 Insecure | 完整 URL（http 非 TLS） |
| `https://host:4318` | `WithEndpointURL` | 完整 URL（TLS） |
| `localhost:4318` | `WithEndpoint` | host:port 形式，Insecure 需显式配置 |

**验证**：Jaeger UI 搜 `logx-demo-otlp`，span `otlp-span` 含 2 条日志事件。

---

## 公共前置与约定

- **compose 环境**（03/04/06/07 共用）：

  ```bash
  cd example && docker compose up -d
  # Jaeger UI :26686 / Thrift :24268 / OTLP HTTP :24318；Kafka :39092；ZK :32181
  ```

  一键脚本默认 `down` 保留 volumes（共享环境）；`LOGX_DEMO_CLEANUP=1` 时 `down -v`。
- **provider 注册**：所有用例空白导入 `_ "github.com/gospacex/logx/zap"`。
- **既有综合演示**：`example/main.go`（292 行混排 5 类功能、env 路由）保留为参考，
  新集成按上述分目录用例对照即可。
- **测试**：`go test -race ./...`（含 middleware / zap 包单测与 03/04 相关装配测试）。
