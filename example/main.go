// logx/example/main.go
// logx 使用案例（配合 example/docker-compose.yml 调用链环境）
//
// 前置条件：已启动调用链环境
//
//	cd logx/example && docker compose up -d
//	# 包含：Jaeger(26686/24268/24317/24318), Kafka(39092), Zookeeper(32181)
//
// 运行：
//
//	go run .
//	LOGX_MQ_ENABLE=1 go run .   # 同时推送 MQ
//	LOGX_EXPORTER=stdout go run . # 切换 span 导出目标（stdout/otlp/kafka，kafka 需 mq.brokers）
//	LOGX_EXAMPLE_SERVER=1 go run . # 附加启动 Hertz HTTP 服务（默认跳过以便跑完即退出）
//
// 演示：
//  1. 初始化日志（logx.New）+ OTel 调用链集成（日志 → Span Event）
//  2. TracerProvider 推送到 Jaeger（默认 http://localhost:24268/api/traces）
//  3. 各级别日志（Debug/Info/Warn/Error/DPanic）均自动入链为 Span Event
//  4. 链式 API：logx.L() 全局入口 / logger.Chain() 实例入口（键值交替字段、
//     Service/Operation/Tags、配置默认值兜底、span 属性注入）
//  5. MQ 异步推送（Kafka）：span 发送到 otel-traces topic（可选，读 env 开关）
//
// 验证：
//   - 打开 http://localhost:26686 搜索服务 logx-example 查看调用链
//   - 调用 HTTP API: curl http://127.0.0.1:8080/hello
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册默认 provider（空白导入）
	zapimpl "github.com/gospacex/logx/zap"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

func main() {
	// ===== 1. 初始化日志（开启 OTel 集成）=====
	cfg := logx.DefaultConfig()
	cfg.Level = "debug"
	cfg.Format = "console"
	cfg.Output = "stdout"
	cfg.ServiceName = "logx-example"
	cfg.ServiceVersion = "1.0.0"
	cfg.Environment = "development"

	// 配置默认值：Builder 回退链兜底（显式 Service/Operation/Tags > default_* > ServiceName）
	cfg.DefaultService = "demo-svc"
	cfg.DefaultTags = []string{"demo"}

	// 开启 OTel：日志作为 Span Event 写入调用链
	cfg.OTel.Enabled = true
	// 推送到 Jaeger（由 example/docker-compose.yml 提供）；可用 LOGX_EXPORTER 切换导出目标
	// （stdout/otlp/kafka；kafka 走 Config.MQ 直推，见下方注释块）
	if exporter := os.Getenv("LOGX_EXPORTER"); exporter != "" {
		cfg.OTel.Exporter = exporter
	} else {
		cfg.OTel.Exporter = "jaeger"
	}
	cfg.OTel.Jaeger.Endpoint = "http://localhost:24268/api/traces"

	cfg.FillDefaults() // 填充其余默认值，并让 OTel 继承顶层服务名

	logger, err := logx.New(cfg)
	if err != nil {
		log.Fatalf("logx.New failed: %v", err)
	}
	defer logger.Close()
	logx.SetGlobal(logger)

	logger.Info("logx initialized",
		zap.String("service", cfg.ServiceName),
		zap.String("otel.service_name", cfg.OTel.ServiceName),
		zap.String("otel.exporter", cfg.OTel.Exporter),
		zap.String("jaeger.endpoint", cfg.OTel.Jaeger.Endpoint),
	)

	// ===== 2. 设置全局 TracerProvider（按 cfg.OTel.Exporter 路由）=====
	// kafka 直推需经 SetupTracerProviderWithMQ 传入 MQ 配置（缺 broker 会 fail fast 报错）；
	// 其余 exporter（jaeger/otlp/stdout）忽略 MQ 参数。
	setup, err := func() (*zapimpl.TracerSetup, error) {
		if cfg.OTel.Exporter == "kafka" || cfg.OTel.Exporter == "mq" {
			return zapimpl.SetupTracerProviderWithMQ(cfg.OTel, cfg.MQ)
		}
		return zapimpl.SetupTracerProvider(cfg.OTel)
	}()
	if err != nil {
		log.Printf("setup tracer provider failed: %v", err)
	} else {
		defer func() { _ = setup.Provider.Shutdown(context.Background()) }()
		logger.Info("tracer provider ready", zap.String("exporter", cfg.OTel.Exporter))
	}

	// ===== 3. 演示各级别日志入链 =====
	if setup != nil {
		demoAllLevels(logger, setup)
	}

	// ===== 3.5 演示链式 API（新）=====
	demoChainedAPI(logger)

	// ===== 4. 可选：MQ 异步推送演示（Kafka）=====
	if os.Getenv("LOGX_MQ_ENABLE") == "1" {
		runMQDemo(logger)
	}

	// ===== 5. Hertz HTTP 服务（默认跳过以便快速退出；LOGX_EXAMPLE_SERVER=1 go run . 启动）=====
	if os.Getenv("LOGX_EXAMPLE_SERVER") == "1" {
		runServer(logger)
	} else {
		log.Printf("skip hertz server (set LOGX_EXAMPLE_SERVER=1 to enable: curl http://127.0.0.1:8080/hello)")
	}
}

// demoChainedAPI 演示新链式 API：全局入口 / 实例入口 + ctx / 配置默认值兜底 / span 属性注入
func demoChainedAPI(logger logx.Logger) {
	// 1) 全局直调：logx.L() 全局入口，键值交替字段（含中文 key），无链式修饰 → 配置默认值兜底
	//    （log.service=demo-svc、log.tags=demo）
	logx.L().Warn("慢查询", "latency", 800.0, "sql语句", "select * from order")

	// 2) 全局链式：显式 Service/Operation/Tags（tags 与 DefaultTags 并集去重）
	logx.L().Service("order").Operation("/api/v1/order").Tags("slow", "mysql").
		Info("curd错误", "db", "127.0.0.1:3306")

	// 3) 实例链式 + ctx（span 属性注入：活动 span SetAttributes service.name/operation/log.tags）
	ctx, span := otel.Tracer("logx-example").Start(context.Background(), "chained-demo")
	logger.Chain().WithContext(ctx).Service("order").
		Warn("链式实例演示", "k", "v", "sql语句", "select * from order")
	span.End()

	// 4) 配置默认值演示：无链式修饰时兜底 DefaultService（demo-svc）/ DefaultTags（demo）
	logx.L().Info("无修饰输出", "k", "v")

	// kafka 直推示例（取消注释即切换；需先启动 example/docker-compose.yml 的 Kafka）：
	//
	//	cfg.OTel.Exporter = "kafka" // "mq" 为别名
	//	mq := logx.MQConfig{Type: "kafka", Brokers: []string{"localhost:39092"}, Topic: "otel-traces"}
	//	kaSetup, err := zapimpl.SetupTracerProviderWithMQ(cfg.OTel, mq)
	//	if err != nil { log.Fatalf("kafka exporter: %v", err) }
	//	defer func() { _ = kaSetup.Provider.Shutdown(context.Background()) }()
}

// demoAllLevels 演示所有级别日志自动注入调用链
// Debug/Info/Warn/Error/DPanic 均通过 Context 方法入链为 Span Event
func demoAllLevels(logger logx.Logger, setup *zapimpl.TracerSetup) {
	ctx, span := setup.Provider.Tracer("logx-example").Start(context.Background(), "all-levels-demo")
	defer span.End()

	// Debug — 调试信息，通常开发环境可见
	logger.DebugContext(ctx, "debug: validating order request",
		zap.String("order_id", "1001"),
		zap.String("user_id", "u-2001"),
	)

	// Info — 正常业务流程
	logger.InfoContext(ctx, "info: order placed",
		zap.String("order_id", "1001"),
		zap.Float64("amount", 99.5),
		zap.String("db.system", "mysql"),
		zap.String("db.statement", "INSERT INTO orders ..."),
	)
	time.Sleep(50 * time.Millisecond)

	// Warn — 可恢复的异常情况
	logger.WarnContext(ctx, "warn: slow query detected",
		zap.String("db.system", "mysql"),
		zap.String("db.statement", "SELECT * FROM stock WHERE ..."),
		zap.Duration("db.latency", 800*time.Millisecond),
		zap.Float64("threshold_ms", 500),
	)

	// 子操作 span：扣减库存
	childCtx, childSpan := setup.Provider.Tracer("logx-example").Start(ctx, "deduct-stock")
	logger.DebugContext(childCtx, "debug: cache hit, key=stock:1001",
		zap.String("cachex.redis.command", "GET"),
		zap.String("cachex.redis.key", "stock:1001"),
		zap.Int("cachex.redis.ttl", 300),
	)
	logger.InfoContext(childCtx, "info: stock deducted",
		zap.String("cachex.redis.command", "DECR"),
		zap.String("cachex.redis.key", "stock:1001"),
		zap.Int("remaining", 49),
	)
	childSpan.End()

	// Error — 不可恢复的错误（仍入链，便于排查）
	logger.ErrorContext(ctx, "error: payment gateway timeout",
		zap.String("payment.gateway", "stripe"),
		zap.String("payment.method", "card"),
		zap.Duration("timeout", 5*time.Second),
		zap.String("error.message", "context deadline exceeded"),
	)

	// DPanic — 开发模式 panic（生产模式降级为 Error，仍入链）
	logger.DPanicContext(ctx, "dpanic: unexpected nil pointer in validator",
		zap.String("component", "order-validator"),
		zap.String("field", "shipping_address"),
	)

	logger.InfoContext(ctx, "all log levels exported to trace",
		zap.Int("levels_count", 5),
		zap.String("levels", "debug,info,warn,error,dpanic"),
	)

	span.End()
	_ = setup.Provider.ForceFlush(context.Background())
	logger.Info("span exported to Jaeger; open http://localhost:26686 to view")
}

// runMQDemo 演示 MQ 异步推送：span → Kafka topic → 消费者 → Jaeger
func runMQDemo(logger logx.Logger) {
	mqCfg := logx.MQConfig{
		Type:    "kafka",
		Brokers: []string{"localhost:39092"},
		Topic:   "otel-traces",
		GroupID: "logx-otel-consumer",
	}
	log.Printf("MQ demo: pushing spans to kafka topic %q", mqCfg.Topic)

	// 使用 MQ tracer provider（span → Kafka）
	mqSetup, err := zapimpl.NewMQTracerProvider(logx.OTelConfig{
		Enabled:        true,
		Exporter:       "mq",
		ServiceName:    "logx-example-mq",
		ServiceVersion: "1.0.0",
		Environment:    "development",
	}, mqCfg)
	if err != nil {
		log.Printf("mq tracer provider failed: %v", err)
		return
	}
	defer func() { _ = mqSetup.Provider.Shutdown(context.Background()) }()

	// 创建 span，各级别日志均入链
	ctx, span := mqSetup.Provider.Tracer("logx-example-mq").Start(context.Background(), "mq-async-span")
	logger.DebugContext(ctx, "mq debug: preparing message",
		zap.String("messaging.system", "kafka"),
		zap.String("messaging.destination", mqCfg.Topic),
	)
	logger.InfoContext(ctx, "mq span event demo",
		zap.String("messaging.system", "kafka"),
		zap.String("messaging.destination", mqCfg.Topic),
		zap.Int("payload_size_bytes", 256),
	)
	logger.WarnContext(ctx, "mq warn: broker lag detected",
		zap.Int("consumer_lag", 1200),
		zap.String("messaging.destination", mqCfg.Topic),
	)
	span.End()
	_ = mqSetup.Provider.ForceFlush(context.Background())

	log.Printf("MQ demo done; span sent to kafka topic %q (consume via NewMQToJaegerConsumer)", mqCfg.Topic)
}

// runServer 启动 Hertz 服务演示请求级日志
func runServer(logger logx.Logger) {
	h := server.New(server.WithHostPorts("127.0.0.1:8080"))

	h.GET("/hello", func(ctx context.Context, c *app.RequestContext) {
		logger.InfoContext(ctx, "request received",
			zap.String("method", string(c.Method())),
			zap.String("path", string(c.Path())),
		)

		// 模拟业务逻辑 + 各级别日志
		logger.DebugContext(ctx, "debug: checking cache",
			zap.String("cache.key", "hello-response"),
		)

		result := fmt.Sprintf("hello logx at %s", time.Now().Format("15:04:05"))
		logger.InfoContext(ctx, "request completed",
			zap.Int("status", 200),
			zap.String("response", result),
		)
		c.String(consts.StatusOK, result)
	})

	log.Printf("server listening on http://127.0.0.1:8080  (try: curl http://127.0.0.1:8080/hello)")
	if err := h.Run(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
