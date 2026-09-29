// logx/zap/coverage_edge_test.go
// 低价值但确属可测的残余分支：
// write 级别门控/default 兜底、OTel 事件注入、buildCore 未知 output 回退、
// exportOne 网络/状态码错误、kafka consumer 幂等关闭
package zap

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gospacex/logx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap/zapcore"
)

// TestWriteGateAndDefaultBranch write 的三个未覆盖分支：
// - Core 不启用该级别 → 早退（门控）
// - 未知级别 → default 兜底走 Info
// - 放行级别正常写
func TestWriteGateAndDefaultBranch(t *testing.T) {
	cfg := logx.DefaultConfig()
	cfg.Level = "error"
	cfg.Output = "stdout"
	l, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	zl := l.(*Logger)
	defer zl.Close()

	ctx := context.Background()
	// 门控：debug < error → 早退，无输出不 panic
	zl.DebugContext(ctx, "below gate")
	// default 兜底：未知级别（> FatalLevel）落 Info
	zl.write(ctx, zapcore.Level(99), "unknown level")
	// 放行：error 级别写通
	zl.ErrorContext(ctx, "pass gate")
}

// TestWriteOTelSpanEvent OTel 开启 + ctx 含 recording span →
// 日志经 OTelCore 转为同名 Span Event（spanField 注入生效）
func TestWriteOTelSpanEvent(t *testing.T) {
	l, exp, tp := newTestLogger(t, true)
	zl := l.(*Logger)

	_, span := tp.Tracer("w").Start(context.Background(), "b")
	zl.InfoContext(trace.ContextWithSpan(context.Background(), span), "branch m", logx.String("k", "v"))
	span.End()

	evs := spanEvents(exp.GetSpans(), "b")
	if len(evs) != 1 {
		t.Fatalf("span b 事件数 = %d, want 1（日志入链）", len(evs))
	}
	if evs[0].Name != "branch m" {
		t.Errorf("事件名 = %q, want 日志消息 branch m", evs[0].Name)
	}
}

// TestNewDevelopmentAndOutputFallback 两条 New/buildCore 分支：
// - Development=true 走 zap.Development() 选项（stdout 不可 close，Close 仅调用不断言）
// - 未识别 Output → 双 core 皆不建 → 回退 stdout core（case 0，设计兜底非错误）
// 回退分支验证后再用 file 输出实例确认 Close 可正常收口
func TestNewDevelopmentAndOutputFallback(t *testing.T) {
	cfg := logx.DefaultConfig()
	cfg.Development = true
	l1, err := New(cfg)
	if err != nil {
		t.Fatalf("Development=true: %v", err)
	}
	_ = l1.Close() // stdout writer 的 Sync 返回 ENXIO，仅验证调用不 panic

	cfg2 := logx.DefaultConfig()
	cfg2.Output = "invalid-backend"
	l2, err := New(cfg2)
	if err != nil {
		t.Fatalf("未识别 output 应回退 stdout 成功, got: %v", err)
	}
	_ = l2.Close()

	// 对照组：file 输出 Close 走 lumberjack 关闭，应干净返回 nil
	cfg3 := logx.DefaultConfig()
	cfg3.Output = "file"
	cfg3.FilePath = t.TempDir() + "/app.log"
	l3, err := New(cfg3)
	if err != nil {
		t.Fatalf("file 输出构造: %v", err)
	}
	if err := l3.Close(); err != nil {
		t.Errorf("file Close = %v, want nil", err)
	}
}

// TestExportOneErrorBranches exportOne 错误分支：
// - client.Do 失败（连接拒绝）→ "mq otlp post" 包装
// - 非 200/202 → "unexpected status" + 截断 body（>200B 走 truncate 尾缀）
func TestExportOneErrorBranches(t *testing.T) {
	ms := MQSpan{
		TraceID:  "0102030405060708090a0b0c0d0e0f10",
		SpanID:   "0102030405060708",
		Name:     "e",
		Resource: map[string]string{"service.name": "svc"},
	}

	c1, _ := NewMQToJaegerConsumer(&fakeQueue{}, "http://127.0.0.1:1")
	err := c1.exportOne(context.Background(), ms)
	if err == nil || !strings.Contains(err.Error(), "mq otlp post") {
		t.Errorf("Do 失败 = %v, want 包装的 post 错误", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		// 300B 触发 truncate 截断尾缀
		_, _ = w.Write([]byte(strings.Repeat("x", 300)))
	}))
	defer srv.Close()
	c2, _ := NewMQToJaegerConsumer(&fakeQueue{}, srv.URL)
	err = c2.exportOne(context.Background(), ms)
	if err == nil || !strings.Contains(err.Error(), "unexpected status 500") {
		t.Errorf("500 = %v, want unexpected status 错误", err)
	}
	if !strings.Contains(err.Error(), "(truncated)") {
		t.Errorf("错误未含截断标记: %v", err)
	}
}

// TestWriteZlCtxNilFallback write 的 zlCtx 回退分支：
// 旧派生/手工构造实例 zlCtx=nil → 回退 l.zl（保持可用）
func TestWriteZlCtxNilFallback(t *testing.T) {
	cfg := logx.DefaultConfig()
	cfg.Level = "info"
	cfg.Output = "stdout"
	base, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// 手工构造 zlCtx=nil 的派生实例，共享 base 的 core
	der := &Logger{zl: base.(*Logger).zl, cfg: cfg}
	defer der.Close()
	der.write(context.Background(), zapcore.InfoLevel, "fallback zl")
}

// TestMarshalSpanEventLoops marshalSpan 事件 / 属性 range 循环体命中：
// ReadOnlySpan 含未导出 private 方法无法包外 fake，改用 tracetest.SpanStub
// （公开实现）直接填充 Events/Attributes 后取快照驱动循环体
func TestMarshalSpanEventLoops(t *testing.T) {
	stub := tracetest.SpanStub{
		Name:       "ev-span",
		Attributes: []attribute.KeyValue{attribute.String("k", "v")},
		Events: []sdktrace.Event{
			{Name: "e1", Time: time.Unix(0, 5), Attributes: []attribute.KeyValue{attribute.String("a", "1")}},
			{Name: "e2", Time: time.Unix(0, 6)},
		},
	}
	data, err := marshalSpan(stub.Snapshot())
	if err != nil {
		t.Fatalf("marshalSpan = %v, want 成功", err)
	}
	var ms MQSpan
	if err := json.Unmarshal(data, &ms); err != nil {
		t.Fatalf("反序列化: %v", err)
	}
	if len(ms.Events) != 2 {
		t.Fatalf("Events = %d, want 2", len(ms.Events))
	}
	if ms.Events[0].Name != "e1" || ms.Events[0].Attributes["a"] != "1" {
		t.Errorf("Events[0] = %+v, want e1 + a=1", ms.Events[0])
	}
	if len(ms.Events[1].Attributes) != 0 {
		t.Errorf("Events[1] 属性 = %v, want 无", ms.Events[1].Attributes)
	}
	if ms.Attributes["k"] != "v" {
		t.Errorf("Attributes = %v, want k=v", ms.Attributes)
	}
}

// TestKafkaConsumerClose 真实 consumer 的幂等关闭（未读消息 → 关闭无阻塞不触网）
func TestKafkaConsumerClose(t *testing.T) {
	c, err := NewKafkaConsumer(logx.MQConfig{
		Type:    "kafka",
		Brokers: []string{"localhost:1"},
		Topic:   "t",
	})
	if err != nil {
		t.Fatalf("NewKafkaConsumer: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("kafkaConsumer.Close = %v, want nil", err)
	}
}

// TestMarshalSpanResourceAttrs marshalSpan 资源属性分支：
// TracerProvider 携带 resource（service.name）→ 序列化进 MQSpan.Resource
func TestMarshalSpanResourceAttrs(t *testing.T) {
	res := resource.NewSchemaless(
		attribute.String("service.name", "res-svc"),
	)
	rec := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(rec),
		sdktrace.WithResource(res),
	)
	_, span := tp.Tracer("mr").Start(context.Background(), "ops")
	span.End()

	if len(rec.GetSpans()) != 1 {
		t.Fatalf("spans = %d, want 1", len(rec.GetSpans()))
	}
	ro := rec.GetSpans()[0].Snapshot()
	data, err := marshalSpan(ro)
	if err != nil {
		t.Fatalf("marshalSpan = %v, want 成功", err)
	}
	var ms MQSpan
	if err := json.Unmarshal(data, &ms); err != nil {
		t.Fatalf("反序列化: %v", err)
	}
	if ms.Resource["service.name"] != "res-svc" {
		t.Errorf("Resource = %v, want service.name=res-svc", ms.Resource)
	}
}
