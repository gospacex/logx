# Verification Record: add-chained-logger-api

日期：2026-09-11 | 仓库：`/Users/hyx/work/gowork/src/hubx/logx`（非 git 仓库，commit 步骤 N/A）

## 总览

| 检查 | 命令 | 结果 |
|---|---|---|
| 构建 | `go build ./...` | PASS（0 输出） |
| 静态检查 | `go vet ./...` | PASS（0 输出） |
| 全量测试（普通） | `go test ./...` | ok `logx` 0.6s / `common` 1.1s / `zap` 1.6s，example 无测试 |
| 全量测试（race） | `go test -race ./...` | ok 三包全绿（logx 2.1s / common 1.6s / zap 2.7s），无数据竞争 |
| 定向验收用例 | 见下文各条 | 全部 PASS（Kafka 集成条件 SKIP，见遗留） |
| example 冒烟 | `cd example && LOGX_EXPORTER=stdout go run .` | PASS，JSON 行含 `log.service` / `log.tags` / `log.operation` |
| openspec 严格校验 | 见「openspec validate」节 | PASS |

定向用例执行记录（`go test -v -run 'TestNormalizedAny|TestBuilder.*|TestSpanAttrsInjected|TestNoSpanSilentFallback|TestSetupTracerProviderWithMQKafka|TestSetupTracerProviderLegacyUnchanged|TestStdoutExportIntegration' ./...`，全量输出摘要）：

```
--- PASS: TestBuilderServiceOperationTags (0.00s)
--- PASS: TestBuilderFallbackChain (0.00s)
--- PASS: TestBuilderTrailingStringJoinsMessage (0.00s)
--- PASS: TestBuilderBadKeyNoPanic (0.00s)
--- PASS: TestNormalizedAny (0.00s)
--- PASS: TestSpanAttrsInjected (0.00s)
--- PASS: TestNoSpanSilentFallback (0.00s)
--- PASS: TestSetupTracerProviderWithMQKafka (0.00s)
--- PASS: TestSetupTracerProviderLegacyUnchanged (0.00s)
--- PASS: TestStdoutExportIntegration (0.00s)
PASS  (logx, common, zap 三包均 ok)
```

example 冒烟（默认 jaeger 需 docker-compose，故按文档以 `LOGX_EXPORTER=stdout` 切换，不阻塞）：

```
$ cd example && LOGX_EXPORTER=stdout go run . | grep -m1 '"log.service"'
2026-09-11T01:41:03.056+0800  WARN  zap/zap.go:184  慢查询  {"log.service": "demo-svc", "log.tags": "demo", "latency": 800, "sql语句": "select * from order"}
```

JSON 行同时含 `log.service`、`log.operation`、`log.tags`（`grep -o '"log\.[a-z_]*"'` 三键均出现），中文 key `sql语句` 正确输出。

## 逐条 Requirement 核对（spec 9 条）

### 1. 各级别全局日志入口（ADDED）
- 实现：`builder.go:29` `func L() *Builder`；`logx.go` 全局 `Debug/Info/Warn/Error/DPanic/Panic/Fatal` 及 `*Context` 直调入口（Task 3 前已存在，本次补齐 `L()`）。
- 测试：`logx_test.go` `TestGlobalFunctions`（直调 + stub provider）、`builder_test.go` `TestBuilderServiceOperationTags`（`L().Service().Operation().Info(msg, "db", ...)` 链式全字段断言）。
- 结果：PASS。结论：**满足**。

### 2. 键值交替变参字段语法（ADDED）
- 实现：`zap/fields.go` `NormalizedAny`（string→字符串、float/int/bool/Duration/Time/error 类型映射、反射兜底、中文 key）+ `zap` 包 `init()` 经 `RegisterFieldNormalizer` 注册到根包；`builder.go:159/164`。
- 测试：`zap/zap_test.go` `TestNormalizedAny`（基本键值对含中文 key、混合类型、Fields map 展开、zap.Field 原样保留、落单字符串追加消息片段、坏键/落单非 string）；`builder_test.go` `TestBuilderTrailingStringJoinsMessage`。
- 结果：PASS。结论：**满足**。

### 3. 非法键值对不中断输出（ADDED）
- 实现：`zap/fields.go` key 位置非 string → `bad-field.<N>` 字段附原始值，不 panic。
- 测试：`builder_test.go` `TestBuilderBadKeyNoPanic`（Builder 路径 `Info("m", 42, "x")` 不 panic、消息正常）、`zap/zap_test.go` `TestNormalizedAny` 坏键用例（`NormalizedAny(42, "x")` → `bad-field.0`）。
- 结果：PASS。结论：**满足**。

### 4. 链式中间修饰方法（ADDED）
- 实现：`builder.go:47-66` `Service/Operation/Tags/WithContext` 中间方法 + `:68-72` 末端全级别 `Debug/Info/Warn/Error/DPanic(msg, fields ...any)`；中间方法仅累积不输出（`emit` 仅末端调用）。
- 测试：`builder_test.go` `TestBuilderServiceOperationTags`（`L().Service("order").Tags("slow","mysql").Warn(...)` 字段断言，tags 逗号拼接）、`TestBuilderTrailingStringJoinsMessage`。
- 结果：PASS。结论：**满足**。

### 5. 属性回退链（ADDED）
- 实现：`builder.go:114` `resolveAttrs`（链式显式 > `Default*` > `ServiceName` 兜底，Tags 并集去重）；`config.go:36-41` `DefaultService/DefaultOperation/DefaultTags`（yaml/mapstructure 键 `default_*`）。
- 测试：`builder_test.go` `TestBuilderFallbackChain`（三级回退 + 显式覆盖 + Tags 并集去重）、`config_test.go` `TestFillDefaults_ServiceDefaults`、`TestConfigYAML`（yaml 键解析）。
- 结果：PASS。结论：**满足**。

### 6. Logger 实例级链式入口（ADDED）
- 实现：`logx.go:76` `Logger` 接口新增 `Chain() *Builder`；`zap/zap.go:293` zap 后端实现（绑定实例输出目标与 cfg 回退）；`logx.go:233` noop 实现。
- 测试：`zap/zap_test.go` `TestSpanAttrsInjected` / `TestNoSpanSilentFallback`（`ln.Chain().WithContext(ctx)...` 实例路径端到端）、`builder_test.go` `captureLogger.Chain()` 桩（接口完整性）；既有 `DebugContext/InfoContext/With/Named` 行为不变由 `TestOTelOn_*` 系列（zap_test.go 既有 20+ 用例）回归。
- 结果：PASS。结论：**满足**。

### 7. 活动 span 属性注入（方案 A，ADDED）
- 实现：`zap/zap.go:303-320` `InjectSpanAttrs`——ctx 含活动 recording span 时 `span.SetAttributes(service.name, operation, log.tags)`；无 span 静默跳过。
- 测试：`zap/zap_test.go` `TestSpanAttrsInjected`（InMemory exporter 断言 span 属性 `service.name=order`、`operation`、`log.tags`）、`TestNoSpanSilentFallback`（空 ctx 不 panic、不产生新 span、正常输出）。
- 结果：PASS。结论：**满足**（方案 A 静默降级符合 spec；方案 B 不在范围）。

### 8. 兼容现有构造函数（ADDED）
- 实现：`zap/fields.go` 归一化器对 `zap.Field` 元素原样透传、`logx.Fields` map 展开。
- 测试：`zap/zap_test.go` `TestNormalizedAny` 内 zap.Field 保留 + Fields map 展开用例；回归：`TestOTelOn_AllLevels_ProduceSpanEvents`、`TestGlobalFunctions` 等既有用例全绿（`InfoContext(ctx, msg, zap.String(...))` 路径未变）。
- 结果：PASS。结论：**满足**。

### 9. （MODIFIED）导出目标配置化
- 实现：`zap/exporter.go:38` `SetupTracerProviderWithMQ` 统一入口，`exporter.go:77` `case "kafka", "mq":` 路由 + 缺 broker fail fast；`exporter_mq.go:234` `NewMQTracerProvider` 转调保留。
- 测试：`zap/zap_test.go` `TestSetupTracerProviderWithMQKafka`（kafka 路由 + 缺 broker 报错含 "brokers" + `mq` 别名等价）、`TestSetupTracerProviderLegacyUnchanged`（jaeger/otlp/stdout 既有取值行为不变）、`TestStdoutExportIntegration`（stdout JSON 断言，span 导出到 stdout）。Kafka 真连：`TestKafkaDirectPushIntegration` 条件测试，本次运行 `--- SKIP: set LOGX_KAFKA_BROKERS to enable Kafka integration test`（预期行为，docker-compose 未起）。
- 结果：PASS（Kafka 真连为条件 SKIP，见遗留）。结论：**满足**。

## openspec validate

| 命令 | 输出 |
|---|---|
| `openspec validate add-chained-logger-api --strict` | `Change 'add-chained-logger-api' is valid`（PASS） |
| `openspec validate --all --strict`（全量；本 CLI 版本裸 `--strict` 不支持，需用 `--all`/`--changes`/`--specs` 其一） | `✓ change/add-chained-logger-api`，`Totals: 1 passed, 0 failed (1 items)`（PASS） |

CLI 说明：`/usr/local/bin/openspec`（全局安装），未走 npx 回退。

## 遗留与已知边界

1. **Kafka docker-compose 手动验证**：`TestKafkaDirectPushIntegration`（zap_test.go:1065）已留好，`LOGX_KAFKA_BROKERS` 设置后自动执行；`example/docker-compose.yml` 提供 broker。本次未起 docker，SKIP。
2. **方案 B（无 span 自动建 span）**：明确不在本次范围（spec 只要求方案 A 静默降级）。
3. **Pulsar / OTLP Logs**：不在本次范围；span 导出目标仅 jaeger/kafka/otlp/stdout。
4. example 默认 `LOGX_EXPORTER` 未设时为 jaeger（需 docker-compose 起 `example/docker-compose.yml`），冒烟时按文档注释用 `LOGX_EXPORTER=stdout` 切换，避免阻塞。

## 口径说明

- `proposal.md` 中**没有**名为「验收标准」的 7 项清单（grep 确认），`tasks.md` 8.1 的 "7 项" 表述与 proposal 历史不符。
- 本次验收以 `specs/chained-logger-api/spec.md` 的 **9 条 requirement**（8 条 ADDED + 1 条 MODIFIED）逐条核对为准，即上节 1–9；对应 `tasks.md` 8.1 完成口径为「9 条 requirement 全部满足」。
