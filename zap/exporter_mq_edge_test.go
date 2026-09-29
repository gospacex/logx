// logx/zap/exporter_mq_edge_test.go
// MQ exporter / 消费者装配与契约边界单测：
// 构造期校验（nil queue / 缺 brokers）、方法契约（producer 不消费 / consumer 不生产）、
// buildProducer 类型路由、错误包装
package zap

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/gospacex/logx"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestNewMQExporterNilQueue 构造期 nil queue 快速失败
func TestNewMQExporterNilQueue(t *testing.T) {
	if _, err := NewMQExporter(nil); err == nil {
		t.Error("NewMQExporter(nil) 未报错, want 构造期 fail fast")
	}
}

// TestNewMQExporterKeyPrefix WithMQKeyPrefix 覆盖缺省 "otel"
func TestNewMQExporterKeyPrefix(t *testing.T) {
	// 缺省
	exp, err := NewMQExporter(&fakeQueue{})
	if err != nil {
		t.Fatal(err)
	}
	mse, ok := exp.(*mqSpanExporter)
	if !ok {
		t.Fatalf("exporter type = %T, want *mqSpanExporter", exp)
	}
	if mse.opts.KeyPrefix != "otel" {
		t.Errorf("缺省 KeyPrefix = %q, want otel", mse.opts.KeyPrefix)
	}

	// 显式覆盖
	exp2, _ := NewMQExporter(&fakeQueue{}, WithMQKeyPrefix("custom"))
	if got := exp2.(*mqSpanExporter).opts.KeyPrefix; got != "custom" {
		t.Errorf("KeyPrefix = %q, want custom", got)
	}
}

// TestMQExporterExportSpansProduceError Produce 错误被包装后返回（不吞错）
// 独立 errQueue（不复用 fakeQueue，避免嵌入含 sync.Mutex 的类型触发 vet「pass lock by value」）
type errQueue struct{}

func (errQueue) Produce(context.Context, []byte, []byte) error {
	return errors.New("produce failed: broker down")
}
func (errQueue) Consume(context.Context) ([]byte, []byte, error) {
	return nil, nil, context.Canceled
}
func (errQueue) Close() error { return nil }

func TestMQExporterExportSpansProduceError(t *testing.T) {
	exp, err := NewMQExporter(&errQueue{})
	if err != nil {
		t.Fatal(err)
	}
	// 空 spans 列表不会触达 Produce；用 in-memory 记录的真实 span 驱动 produce 错误路径
	rec := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(rec))
	_, span := tp.Tracer("t").Start(context.Background(), "op")
	span.End()
	roSpans := make([]sdktrace.ReadOnlySpan, 0, len(rec.GetSpans()))
	for _, s := range rec.GetSpans() {
		roSpans = append(roSpans, s.Snapshot())
	}
	if err := exp.ExportSpans(context.Background(), roSpans); err == nil {
		t.Error("ExportSpans 未在 produce 失败时返回 error")
	} else if !strings.Contains(err.Error(), "produce failed") {
		t.Errorf("错误未包装底层信息: %v", err)
	}
}

// TestMQExporterForceFlushNoop ForceFlush 无缓冲直返 nil（mqSpanExporter 自有方法）
func TestMQExporterForceFlushNoop(t *testing.T) {
	exp, _ := NewMQExporter(&fakeQueue{})
	mse, ok := exp.(*mqSpanExporter)
	if !ok {
		t.Fatalf("exporter type = %T, want *mqSpanExporter", exp)
	}
	if err := mse.ForceFlush(context.Background()); err != nil {
		t.Errorf("ForceFlush = %v, want nil", err)
	}
}

// TestNewKafkaBrokerFailFast 缺 brokers 时 producer/consumer 构造即报错（fail fast 契约）
func TestNewKafkaBrokerFailFast(t *testing.T) {
	if _, err := NewKafkaProducer(logx.MQConfig{Type: "kafka", Topic: "t"}); err == nil {
		t.Error("NewKafkaProducer 缺 brokers 未报错")
	}
	if _, err := NewKafkaConsumer(logx.MQConfig{Type: "kafka", Topic: "t"}); err == nil {
		t.Error("NewKafkaConsumer 缺 brokers 未报错")
	}
}

// TestKafkaMethodContracts producer 不消费 / consumer 不生产（错误契约，非 nil）
func TestKafkaMethodContracts(t *testing.T) {
	p, err := NewKafkaProducer(logx.MQConfig{Type: "kafka", Brokers: []string{"localhost:1"}, Topic: "t"})
	if err != nil {
		t.Fatalf("NewKafkaProducer: %v", err)
	}
	// 方法契约不触网：Consume 直接报错
	if _, _, err := p.Consume(context.Background()); err == nil {
		t.Error("kafkaProducer.Consume 应报错（producer 不可消费）")
	}

	c, err := NewKafkaConsumer(logx.MQConfig{Type: "kafka", Brokers: []string{"localhost:1"}, Topic: "t"})
	if err != nil {
		t.Fatalf("NewKafkaConsumer: %v", err)
	}
	if err := c.Produce(context.Background(), nil, []byte("x")); err == nil {
		t.Error("kafkaConsumer.Produce 应报错（consumer 不可生产）")
	}
}

// TestBuildProducerTypeRouting 类型路由：kafka/"" 走 NewKafkaProducer；pulsar 与未知类型报错
func TestBuildProducerTypeRouting(t *testing.T) {
	if _, err := buildProducer(logx.MQConfig{Type: "kafka", Brokers: []string{"localhost:1"}}); err != nil {
		t.Errorf("kafka: %v", err)
	}
	if _, err := buildProducer(logx.MQConfig{Type: "", Brokers: []string{"localhost:1"}}); err != nil {
		t.Errorf("缺省(\"\") 应按 kafka 处理: %v", err)
	}
	if _, err := buildProducer(logx.MQConfig{Type: "pulsar"}); err == nil {
		t.Error("pulsar 应报错（未实现）")
	}
	if _, err := buildProducer(logx.MQConfig{Type: "rabbit"}); err == nil {
		t.Error("未知类型应报错")
	}
}

// TestNewMQToJaegerConsumerEdge nil queue 报错 + endpoint 尾斜杠归一
func TestNewMQToJaegerConsumerEdge(t *testing.T) {
	if _, err := NewMQToJaegerConsumer(nil, "http://x:4318"); err == nil {
		t.Error("nil queue 未报错")
	}
	c, err := NewMQToJaegerConsumer(&fakeQueue{}, "http://x:4318/")
	if err != nil {
		t.Fatal(err)
	}
	if c.endpoint != "http://x:4318" {
		t.Errorf("endpoint = %q, want 尾斜杠归一 http://x:4318", c.endpoint)
	}
	// Close 无副作用契约
	if err := c.Close(context.Background()); err != nil {
		t.Errorf("Close = %v, want nil", err)
	}
}

// TestRunMQLoopCancelledCtx ctx 已取消时正常退出（契约：consume 报错且 ctx.Err()!=nil → 返回 nil，
// 调用方据此区分「正常停止」与「真实消费/转发错误」）
func TestRunMQLoopCancelledCtx(t *testing.T) {
	c, err := NewMQToJaegerConsumer(&fakeQueue{}, "http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 预先取消
	if err := c.RunMQLoop(ctx); err != nil {
		t.Errorf("RunMQLoop(已取消 ctx) = %v, want nil（正常退出契约）", err)
	}
}

// TestRunMQLoopConsumeError 取消外的 consume 错误 → 包装后返回（非静默）
type consumeErrQueue struct{}

func (consumeErrQueue) Produce(context.Context, []byte, []byte) error { return nil }
func (consumeErrQueue) Consume(context.Context) ([]byte, []byte, error) {
	return nil, nil, errors.New("broker unreachable")
}
func (consumeErrQueue) Close() error { return nil }

func TestRunMQLoopConsumeError(t *testing.T) {
	c, err := NewMQToJaegerConsumer(consumeErrQueue{}, "http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	err = c.RunMQLoop(context.Background())
	if err == nil {
		t.Fatal("consume 真实错误应返回 error")
	}
	if !strings.Contains(err.Error(), "broker unreachable") {
		t.Errorf("错误未包装底层信息: %v", err)
	}
}
