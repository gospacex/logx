# 实现计划：add-chained-logger-api

## 来源
- 提案：openspec/changes/add-chained-logger-api/proposal.md
- 设计：openspec/changes/add-chained-logger-api/design.md
- 规格：openspec/changes/add-chained-logger-api/specs/chained-logger-api/spec.md
- 任务：openspec/changes/add-chained-logger-api/tasks.md

## 实现步骤

> 按执行依赖排序：配置与解析器先行（无外部依赖），Builder 依赖解析器，span 注入依赖 Builder，导出统一独立，集成验证与文档殿后。

### Task 1: 配置扩展（Config 缺省段）
- [x] **任务完成**（与 superpowers plan `Task 1`、`tasks.md` 1.1/1.2 同步勾选）
- 目标：`logx.Config` 增加 `DefaultService`/`DefaultOperation`/`DefaultTags`（yaml/mapstructure tag + FillDefaults），表驱动单测覆盖默认填充与显式保留
- 改动文件：`config.go`、`config_test.go`
- 验证方式：`go test ./ -run TestFillDefaults` 通过；`go build ./...` 无错误

### Task 2: 键值交替字段解析器
- [x] **任务完成**（与 superpowers plan `Task 2`、`tasks.md` 2.1/2.2/2.3 同步勾选）
- 目标：根包新增 `builder.go`：`parseFields(msg string, fields ...any) (string, []zap.Field)`——键值交替扫描、类型映射（string/float/int/bool/Duration/Time/error/其它 Any）、落单 string 追加消息、落单非 string → `arg.N`、坏键 → `bad-field.N` 不 panic；`logx.Fields`（map[string]any）元素展开；`zap.Field` 元素原样保留。表驱动单测全场景
- 改动文件：`builder.go`（新增）、`builder_test.go`（新增）
- 验证方式：`go test ./ -run TestParseFields` 全部场景绿

### Task 3: Builder 与全局/实例入口
- [x] **任务完成**（与 superpowers plan `Task 3`、`tasks.md` 3.1/3.2/3.3/3.4 同步勾选）
- 目标：`logx.Builder`（`logx.L()` 全局入口 + `Logger.Chain()` 实例入口）；中间方法 Service/Operation/Tags/WithContext 不输出，末端各级别方法 Debug/Info/Warn/Error/DPanic(msg, fields...) 输出；属性回退链（显式 > Default* > ServiceName，Tags 并集去重）生成 `log.service/log.operation/log.tags` 字段；现有全局便捷函数 `logx.Info(msg, ...Field)` 等保留不动；单测覆盖链式输出、回退优先级、缺省输出
- 改动文件：`logx.go`（Builder、L()、接口扩展 Chain()、noopLogger/stubLogger 补方法）、`builder.go`、`zap/zap.go`（Chain 实现）、`builder_test.go`
- 验证方式：`go test ./...` 全绿，含新增 `TestBuilder*` 用例

### Task 4: 活动 span 属性注入（方案 A）
- [x] **任务完成**（与 superpowers plan `Task 4`、`tasks.md` 4.1/4.2 同步勾选）
- 目标：带 ctx 输出且 ctx 含 recording span 时 `span.SetAttributes(service.name, operation, log.tags 数组)`；无 span 静默降级；单测用 `sdktrace` 内存 exporter 或 mock span 断言属性
- 改动文件：`builder.go` 或 `zap/zap.go`（注入点）、`builder_test.go`/`zap_test.go`
- 验证方式：`go test ./... -run TestSpanAttributes` 断言 SetAttributes 结果；无 span 场景不 panic

### Task 5: 导出目标统一配置化
- [x] **任务完成**（与 superpowers plan `Task 5`、`tasks.md` 5.1/5.2/5.3 同步勾选）
- 目标：`buildSpanExporter` 增加 `case "kafka", "mq"`（复用 `buildProducer`/`NewMQExporter`，读 `cfg.MQ`，缺 Broker 返回明确错误）；`NewMQTracerProvider` 转调统一装配；单测覆盖路由（jaeger/otlp/stdout 不变、kafka 建 MQ exporter、mq 别名、缺 broker 报错）
- 改动文件：`zap/exporter.go`、`zap/exporter_mq.go`、`zap/zap_test.go`
- 验证方式：`go test ./zap -run TestExporterRouting` 全绿

### Task 6: 集成验证
- [x] **任务完成**（与 superpowers plan Task 6、tasks.md 6.1/6.2/6.3 同步勾选）
- 目标：stdout exporter 导出 JSON 断言新属性与 Span Event；Kafka 直推在 example/docker-compose 环境验证 span 入 topic；全量回归 `go test ./...`（含 race）
- 改动文件：`zap/zap_test.go`（集成用例，Kafka 用例用 t.Skip 条件）
- 验证方式：`go test -race ./...` 全绿；手动 `./example/example` stdout 导出含 `service.name` 属性

### Task 7: 示例与文档
- [x] **任务完成**（与 superpowers plan Task 7、tasks.md 7.1/7.2 同步勾选）
- 目标：`example/main.go` 演示新 API（各级别直调 + 链式 + 配置默认值 + kafka 切换注释示例）；`README.md` 补新 API 用法与 `default_service/default_operation/default_tags`、`exporter: kafka` 配置说明
- 改动文件：`example/main.go`、`README.md`
- 验证方式：`go build ./...` 通过；example 可运行输出新字段

### Task 8: 验收对照
- [x] **任务完成**（与 superpowers plan Task 8、tasks.md 8.1/8.2 同步勾选）
- 目标：逐条核对 proposal.md「验收标准」7 项并记录证据；`openspec validate add-chained-logger-api --strict` 通过
- 改动文件：无（仅验证记录）
- 验证方式：validate 输出 valid；7 项验收逐条满足
