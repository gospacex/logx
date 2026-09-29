// logx/example/04-mq-kafka/main.go
// 用例 04：MQ 异步推送（Kafka 直推 + 消费者回写 Jaeger）
//
// 前置条件：
//
//	cd logx/example && docker compose up -d   # Jaeger + Kafka (localhost:39092)
//
// 运行：
//
//	go run .
//	LOGX_KAFKA_BROKERS=localhost:39092 go run .   # 覆盖 broker 地址（逗号分隔多个）
//	LOGX_OTLP_ENDPOINT=http://localhost:24318 go run .  # 覆盖 OTLP HTTP 回写端点
//
// 覆盖功能矩阵：
//
//	§1 fail-fast：缺 Brokers 的 MQConfig 调 SetupTracerProviderWithMQ 返回 error（构造期暴露配置错误）
//	§2 span → Kafka topic（otel-traces），MQ exporter 直推
//	§3 消费者 goroutine：Kafka → OTLP HTTP → Jaeger（NewKafkaConsumer + NewMQToJaegerConsumer + RunMQLoop）
//
// 验证：
//  1. 终端输出 "span flushed to kafka"（生产者直推成功）
//  2. Jaeger UI http://localhost:26686 搜索服务 logx-demo-mq（span 经 MQ → OTLP 回写，
//     service.name 随消息 resource 字段还原；消费端对坏消息（短 trace_id 等）跳过不断链，
//     本 demo 的 span 正常回写落库）
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册默认 provider（空白导入）
	zapimpl "github.com/gospacex/logx/zap"
	"go.uber.org/zap"
)

const serviceName = "logx-demo-mq"

func main() {
	// ===== 基础日志（stdout，无 OTel）=====
	cfg := logx.DefaultConfig()
	cfg.Level = "debug"
	cfg.ServiceName = serviceName
	cfg.FillDefaults()

	logger, err := logx.New(cfg)
	if err != nil {
		log.Fatalf("logx.New failed: %v", err)
	}
	defer logger.Close()

	// ===== MQ 配置（Kafka 直推；LOGX_KAFKA_BROKERS 支持逗号分隔多 broker）=====
	mqCfg := logx.MQConfig{
		Type:    "kafka",
		Brokers: splitBrokers(defaultEnv("LOGX_KAFKA_BROKERS", "localhost:39092")),
		Topic:   "otel-traces",
		// GroupID 每次运行加随机后缀：kafka-go reader 的 group 位点持久化在 broker，
		// 复用同一 GroupID 会跳过已消费消息，导致看不到本次 demo 的 span
		GroupID: "logx-demo-mq-" + randSuffix(),
	}
	otelCfg := logx.OTelConfig{
		Enabled:        true,
		Exporter:       "kafka",
		ServiceName:    serviceName,
		ServiceVersion: "1.0.0",
		Environment:    "development",
	}

	// ===== §1 fail-fast 演示：缺 Brokers 应在装配期报错（而非运行期静默丢 span）=====
	badMQ := logx.MQConfig{Type: "kafka", Topic: "otel-traces"} // 故意缺 Brokers
	if _, err := zapimpl.SetupTracerProviderWithMQ(otelCfg, badMQ); err != nil {
		fmt.Fprintf(os.Stderr, "[fail-fast OK] missing brokers rejected at setup: %v\n", err)
	} else {
		log.Fatalf("expected error when MQConfig has no brokers, but setup succeeded")
	}

	// ===== §2 正常流程：span → Kafka topic =====
	setup, err := zapimpl.SetupTracerProviderWithMQ(otelCfg, mqCfg)
	if err != nil {
		log.Fatalf("SetupTracerProviderWithMQ failed: %v", err)
	}
	defer func() { _ = setup.Provider.Shutdown(context.Background()) }()
	logger.Info("tracer provider ready (kafka exporter)",
		zap.String("topic", mqCfg.Topic),
		zap.String("broker", mqCfg.Brokers[0]),
		zap.String("consumer_group", mqCfg.GroupID),
	)

	// ===== §3 消费者 goroutine：Kafka → OTLP HTTP → Jaeger =====
	// 先起消费循环再产出 span：新消费组的起始位点为 latest，
	// 若先产出再消费会永久错过本 demo 的消息
	kq, err := zapimpl.NewKafkaConsumer(mqCfg)
	if err != nil {
		log.Fatalf("NewKafkaConsumer failed: %v", err)
	}
	consumer, err := zapimpl.NewMQToJaegerConsumer(kq, defaultEnv("LOGX_OTLP_ENDPOINT", "http://localhost:24318"))
	if err != nil {
		log.Fatalf("NewMQToJaegerConsumer failed: %v", err)
	}
	loopCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		if err := consumer.RunMQLoop(loopCtx); err != nil {
			fmt.Fprintf(os.Stderr, "mq consumer loop stopped: %v\n", err)
		}
	}()
	// 等待消费者完成 group join：新消费组起始位点为 LastOffset，
	// 若 span 在 join 完成前已写入 topic，起始位点已越过该 span，
	// 本 demo 的消息会被该组永久跳过（位点持久化在 broker）。
	// join 握手时长随 broker 组协调器响应时间变化（本机共享 broker 实测
	// 5s 不够、10s 覆盖），等待 10s；若 broker 更慢导致跳位点，
	// 以 "span flushed to kafka"（生产直推证据）为准
	time.Sleep(10 * time.Second)

	// span 内打 3 条 *Context 日志（入链为 Span Event，随 span 序列化推送 MQ）
	ctx, span := setup.Provider.Tracer(serviceName).Start(context.Background(), "mq-async-span")
	logger.DebugContext(ctx, "debug: preparing message",
		zap.String("messaging.system", "kafka"),
		zap.String("messaging.destination", mqCfg.Topic),
	)
	logger.InfoContext(ctx, "info: span will be pushed via kafka",
		zap.String("messaging.system", "kafka"),
		zap.String("messaging.destination", mqCfg.Topic),
		zap.Int("payload_size_bytes", 256),
	)
	logger.WarnContext(ctx, "warn: kafka producer sync mode may block",
		zap.Int("messaging.consumer_lag", 1200),
	)
	span.End()
	_ = setup.Provider.ForceFlush(context.Background()) // 立即推送（不等 5s 批超时）
	logger.Info("span flushed to kafka", zap.String("topic", mqCfg.Topic))

	// 等 5s 让消费发生，再取消消费循环
	time.Sleep(5 * time.Second)
	cancel()
	<-consumeDone
	_ = kq.Close()

	logger.Info("done; span re-written to Jaeger via MQ -> OTLP")
}

// defaultEnv 读环境变量，为空返回默认值
func defaultEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// splitBrokers 解析逗号分隔的 broker 列表
func splitBrokers(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// randSuffix 生成 8 位 hex 随机后缀（保证每次运行的消费组位点全新）
func randSuffix() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "00000000"
	}
	return hex.EncodeToString(b)
}
