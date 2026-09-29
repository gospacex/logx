// logx/example/04-mq-kafka/04_test.go
// 用例 04 测试：MQ 异步推送（Kafka 直推 + 消费者回写 Jaeger）
//
// 零依赖部分（默认运行）：
//
//   - fail-fast 契约：MQConfig 缺 Brokers 时 SetupTracerProviderWithMQ 构造期报错
//   - splitBrokers 解析（逗号分隔 + 空白过滤 + 空结果）
//   - randSuffix 随机后缀（8 位 hex，非全零）
//   - NewKafkaConsumer 缺 Brokers 时 fail fast
//
// 门控集成（LOGX_KAFKA_BROKERS 非空时运行）：
//
//   - span → Kafka topic 直推 + 消费循环启动/取消（不校验消费结果，
//     消费落库属用例 03 同类 Jaeger 断言范畴，以 "span flushed to kafka" 为准）
package main

import (
	"context"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册默认 provider
	zapimpl "github.com/gospacex/logx/zap"
)

// TestExample04FailFastNoBrokers §1 契约：缺 Brokers 的 MQConfig 在装配期报错
// （main.go 演示的 fail-fast 路径，保证不在运行期静默丢 span）
func TestExample04FailFastNoBrokers(t *testing.T) {
	otelCfg := logx.OTelConfig{Enabled: true, Exporter: "kafka", ServiceName: serviceName}
	badMQ := logx.MQConfig{Type: "kafka", Topic: "otel-traces"} // 故意缺 Brokers

	_, err := zapimpl.SetupTracerProviderWithMQ(otelCfg, badMQ)
	if err == nil {
		t.Fatal("期望缺 Brokers 报错，但装配成功")
	}
	if !regexp.MustCompile(`(?i)brokers`).MatchString(err.Error()) {
		t.Errorf("错误信息应提及 brokers: %v", err)
	}

	// 旁路契约：NewKafkaConsumer 同样 fail fast
	if _, err := zapimpl.NewKafkaConsumer(badMQ); err == nil {
		t.Error("NewKafkaConsumer 缺 Brokers 应报错")
	}
}

// TestExample04SplitBrokers splitBrokers 解析边界
func TestExample04SplitBrokers(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"localhost:39092", []string{"localhost:39092"}},
		{"host1:9092,host2:9092,host3:9092", []string{"host1:9092", "host2:9092", "host3:9092"}},
		{" host1:9092 , host2:9092 ", []string{"host1:9092", "host2:9092"}}, // 空白过滤
		{"host1:9092,,host2:9092", []string{"host1:9092", "host2:9092"}},    // 空项过滤
		{",,,", []string{}}, // 全空 → 空列表
		{"", []string{}},
	}
	for _, tc := range cases {
		got := splitBrokers(tc.in)
		if len(got) != len(tc.want) {
			t.Errorf("splitBrokers(%q) 长度 = %d, want %d (%v)", tc.in, len(got), len(tc.want), got)
			continue
		}
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Errorf("splitBrokers(%q)[%d] = %q, want %q", tc.in, i, got[i], tc.want[i])
			}
		}
	}
}

// TestExample04RandSuffix 随机后缀：8 位 hex（4 字节编码）
func TestExample04RandSuffix(t *testing.T) {
	s := randSuffix()
	if !regexp.MustCompile(`^[0-9a-f]{8}$`).MatchString(s) {
		t.Fatalf("randSuffix() = %q, want 8 位 hex", s)
	}
	// 连续两次大概率不同（不强制，防 flaky）
	if s2 := randSuffix(); s == s2 {
		t.Logf("警告：连续两次 randSuffix 相同（概率 1/65536，可忽略）")
	}
}

// TestExample04KafkaIT 门控集成：真实 broker 时装配 kafka exporter 并直推 span
// （LOGX_KAFKA_BROKERS 未设置 → skip；与 zap 包既有集成测试同门控策略）
func TestExample04KafkaIT(t *testing.T) {
	brokers := os.Getenv("LOGX_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("set LOGX_KAFKA_BROKERS (docker compose up -d) to enable Kafka integration test")
	}

	mqCfg := logx.MQConfig{
		Type:    "kafka",
		Brokers: splitBrokers(brokers),
		Topic:   "otel-traces",
		GroupID: "logx-it-" + randSuffix(),
	}
	otelCfg := logx.OTelConfig{
		Enabled:        true,
		Exporter:       "kafka",
		ServiceName:    serviceName,
		ServiceVersion: "1.0.0",
		Environment:    "development",
	}

	// §2 span → Kafka 直推
	setup, err := zapimpl.SetupTracerProviderWithMQ(otelCfg, mqCfg)
	if err != nil {
		t.Fatalf("SetupTracerProviderWithMQ(kafka): %v", err)
	}
	defer func() { _ = setup.Provider.Shutdown(context.Background()) }()

	ctx, span := setup.Provider.Tracer(serviceName).Start(context.Background(), "it-mq-span")
	span.End()
	if err := setup.Provider.ForceFlush(ctx); err != nil {
		t.Errorf("ForceFlush（kafka 同步写）: %v", err)
	}

	// §3 消费循环启动/取消（不等消费结果——Jaeger 落库断言属用例 03 验证范畴）
	kq, err := zapimpl.NewKafkaConsumer(mqCfg)
	if err != nil {
		t.Fatalf("NewKafkaConsumer: %v", err)
	}
	defer func() { _ = kq.Close() }()
	consumer, err := zapimpl.NewMQToJaegerConsumer(kq, defaultEnv("LOGX_OTLP_ENDPOINT", "http://localhost:24318"))
	if err != nil {
		t.Fatalf("NewMQToJaegerConsumer: %v", err)
	}
	loopCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.RunMQLoop(loopCtx)
	}()
	time.Sleep(500 * time.Millisecond) // 等待 group join 发生（失败将记 stderr，不阻断）
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("RunMQLoop 未在 ctx 取消后 5s 内退出")
	}
}
