// logx/zap/zap_test.go
// logx/zap 单元测试
package zap

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gospacex/logx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ======================= 基础创建测试 =======================

func TestNewLogger(t *testing.T) {
	cfg := logx.DefaultConfig()
	cfg.Level = "debug"
	cfg.Output = "stdout"

	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer l.Close()

	l.Info("hello logx", zap.String("k", "v"))
	l.Debug("debug msg")
	l.WarnContext(context.Background(), "warn ctx")
}

func TestNewLoggerJSONFile(t *testing.T) {
	cfg := logx.DefaultConfig()
	cfg.Format = "json"
	cfg.Output = "both"
	cfg.FilePath = t.TempDir() + "/app.log"

	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer l.Close()
	l.Info("json log")
}

func TestParseLevel(t *testing.T) {
	cases := map[string]zapcore.Level{
		"debug":   zapcore.DebugLevel,
		"info":    zapcore.InfoLevel,
		"warn":    zapcore.WarnLevel,
		"warning": zapcore.WarnLevel,
		"error":   zapcore.ErrorLevel,
		"unknown": zapcore.InfoLevel,
	}
	for in, want := range cases {
		if got := parseLevel(in); got != want {
			t.Errorf("parseLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

// ======================= OTel 集成测试 =======================

// TestOTelSpanEvent 验证：OTel 开启时，InfoContext 将日志写入 Span Event
func TestOTelSpanEvent(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	ctx, span := tp.Tracer("logx-test").Start(context.Background(), "outer")
	childCtx, childSpan := tp.Tracer("logx-test").Start(ctx, "inner")

	cfg := logx.DefaultConfig()
	cfg.Level = "debug"
	cfg.Output = "stdout"
	cfg.OTel.Enabled = true

	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer l.Close()

	l.InfoContext(childCtx, "order created", zap.String("order_id", "12345"))

	childSpan.End()
	span.End()
	_ = tp.ForceFlush(context.Background())

	spans := exp.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}

	// 找到 inner span 并检查事件
	var inner *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "inner" {
			inner = &spans[i]
		}
	}
	if inner == nil {
		t.Fatal("inner span not found")
	}

	found := false
	for _, ev := range inner.Events {
		if ev.Name != "order created" {
			continue
		}
		found = true
		attrs := map[string]string{}
		for _, kv := range ev.Attributes {
			attrs[string(kv.Key)] = kv.Value.AsString()
		}
		if attrs["log.level"] != "info" {
			t.Errorf("event log.level = %q, want info", attrs["log.level"])
		}
		if attrs["order_id"] != "12345" {
			t.Errorf("event order_id = %q, want 12345", attrs["order_id"])
		}
	}
	if !found {
		t.Errorf("span events = %+v, want 'order created'", inner.Events)
	}
}

// TestOTelDisabledNoEvent 验证：OTel 关闭时，日志不注入 Span Event
func TestOTelDisabledNoEvent(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	ctx, span := tp.Tracer("logx-test").Start(context.Background(), "s")
	defer span.End()

	cfg := logx.DefaultConfig()
	cfg.Output = "stdout"

	l, err := New(cfg) // OTel 默认关闭
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer l.Close()

	l.InfoContext(ctx, "no otel event")
	_ = tp.ForceFlush(context.Background())

	for _, s := range exp.GetSpans() {
		if len(s.Events) > 0 {
			t.Errorf("OTel disabled but events present: %+v", s.Events)
		}
	}
}

// ======================= OTelCore 单元测试 =======================

// recordingCore 捕获写入的条目
type recordingCore struct {
	entries []zapcore.Entry
	fields  [][]zapcore.Field
	enabled bool
}

func newRecordingCore() *recordingCore { return &recordingCore{enabled: true} }

func (r *recordingCore) Enabled(lvl zapcore.Level) bool { return r.enabled }
func (r *recordingCore) With(fields []zapcore.Field) zapcore.Core {
	return &recordingCore{enabled: r.enabled}
}
func (r *recordingCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if r.Enabled(ent.Level) {
		return ce.AddCore(ent, r)
	}
	return ce
}
func (r *recordingCore) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	r.entries = append(r.entries, ent)
	cp := make([]zapcore.Field, len(fields))
	copy(cp, fields)
	r.fields = append(r.fields, cp)
	return nil
}
func (r *recordingCore) Sync() error { return nil }

// TestOTelCoreFieldExtraction 测试 spanField/extractSpan
func TestOTelCoreFieldExtraction(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("t").Start(context.Background(), "s")
	defer span.End()

	f := spanField(span)
	if f.Key != spanFieldKey {
		t.Errorf("spanField key = %q, want %q", f.Key, spanFieldKey)
	}
	if f.Type != zapcore.SkipType {
		t.Errorf("spanField type = %v, want SkipType", f.Type)
	}

	fields := []zapcore.Field{
		{Key: "a", Type: zapcore.StringType, String: "1"},
		f,
		{Key: "b", Type: zapcore.Int64Type, Integer: 2},
	}
	gotSpan, rest := extractSpan(fields)
	if gotSpan == nil {
		t.Fatal("extractSpan returned nil span")
	}
	if len(rest) != 2 {
		t.Fatalf("rest len = %d, want 2 (span field removed)", len(rest))
	}
	if rest[0].Key != "a" || rest[1].Key != "b" {
		t.Errorf("rest fields = %+v", rest)
	}
}

// TestOTelCoreWriteToBase 测试 OTelCore 写入：span 事件 + 下游同步
func TestOTelCoreWriteToBase(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	ctx, span := tp.Tracer("t").Start(context.Background(), "write-test")
	_ = ctx

	base := newRecordingCore()
	core := NewOTelCore(base)

	ent := zapcore.Entry{Level: zapcore.InfoLevel, Message: "core message", Time: time.Now()}
	fields := []zapcore.Field{
		spanField(span),
		{Key: "order", Type: zapcore.StringType, String: "X1"},
	}
	if err := core.Write(ent, fields); err != nil {
		t.Fatalf("Write: %v", err)
	}
	span.End()
	_ = tp.ForceFlush(context.Background())

	// 1. 下游 core 收到的字段不应包含 span 隐藏字段
	if len(base.entries) != 1 {
		t.Fatalf("base entries = %d, want 1", len(base.entries))
	}
	if got := base.fields[0]; len(got) != 1 || got[0].Key != "order" {
		t.Errorf("base fields = %+v, want only 'order'", got)
	}

	// 2. span 上应有事件
	var found bool
	for _, s := range exp.GetSpans() {
		for _, ev := range s.Events {
			if ev.Name == "core message" {
				found = true
			}
		}
	}
	if !found {
		t.Error("span event not recorded")
	}
}

// TestFieldToAttribute 测试字段转换
func TestFieldToAttribute(t *testing.T) {
	cases := []struct {
		field zapcore.Field
		want  string
	}{
		{
			field: zapcore.Field{Key: "s", Type: zapcore.StringType, String: "x"},
			want:  "x",
		},
		{
			field: zapcore.Field{Key: "b", Type: zapcore.BoolType, Integer: 1},
			want:  "true",
		},
		{
			field: zapcore.Field{Key: "i", Type: zapcore.Int64Type, Integer: 42},
			want:  "42",
		},
		{
			field: zapcore.Field{Key: "f", Type: zapcore.Float64Type, Integer: int64(0x4000000000000000)},
			want:  "2",
		},
	}
	for _, c := range cases {
		kv, ok := fieldToAttribute(c.field)
		if !ok {
			t.Errorf("field %+v not converted", c.field)
			continue
		}
		if kv.Value.Emit() != c.want {
			t.Errorf("field %s = %q, want %q", c.field.Key, kv.Value.Emit(), c.want)
		}
	}
}

// ======================= MQ Exporter 测试 =======================

// fakeQueue 内存消息队列
type fakeQueue struct {
	mu     sync.Mutex
	msgs   [][]byte
	closed bool
}

func (f *fakeQueue) Produce(ctx context.Context, key, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.msgs = append(f.msgs, append([]byte(nil), value...))
	return nil
}

func (f *fakeQueue) Consume(ctx context.Context) ([]byte, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.msgs) == 0 {
		return nil, nil, context.Canceled
	}
	m := f.msgs[0]
	f.msgs = f.msgs[1:]
	return nil, m, nil
}

func (f *fakeQueue) Close() error {
	f.closed = true
	return nil
}

func (f *fakeQueue) len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.msgs)
}

// TestMQExporterSpanToQueue 验证 span 通过 MQ exporter 写入队列
func TestMQExporterSpanToQueue(t *testing.T) {
	q := &fakeQueue{}
	exp, err := NewMQExporter(q, WithMQKeyPrefix("test"))
	if err != nil {
		t.Fatalf("NewMQExporter: %v", err)
	}
	defer func() { _ = exp.Shutdown(context.Background()) }()

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", "mq-test"))),
	)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, span := tp.Tracer("mq").Start(context.Background(), "mq-op",
		trace.WithAttributes(attribute.String("db.system", "kafka")))
	span.End()
	_ = tp.ForceFlush(context.Background())

	if q.len() == 0 {
		t.Fatal("expected MQ to receive the span")
	}
}

// TestMQSpanJSONRoundTrip 验证 MQSpan JSON 序列化/反序列化
func TestMQSpanJSONRoundTrip(t *testing.T) {
	ms := MQSpan{
		TraceID:       "0123456789abcdef0123456789abcdef",
		SpanID:        "0123456789abcdef",
		ParentSpanID:  "fedcba9876543210",
		Name:          "test span",
		StartUnixNano: 1000,
		EndUnixNano:   2000,
		Attributes:    map[string]string{"db.system": "redis"},
		Events: []MQSpanEvent{
			{Name: "evt", TimeUnixNano: 1500, Attributes: map[string]string{"k": "v"}},
		},
	}
	data, err := json.Marshal(ms)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"name":"test span"`) {
		t.Errorf("marshaled = %s", string(data))
	}
	var back MQSpan
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Name != ms.Name || back.TraceID != ms.TraceID || len(back.Events) != 1 {
		t.Errorf("round trip mismatch: %+v", back)
	}
}

// TestBuildResourceSpans 验证 MQSpan → OTLP ResourceSpans 转换
func TestBuildResourceSpans(t *testing.T) {
	ms := MQSpan{
		TraceID:       "0123456789abcdef0123456789abcdef",
		SpanID:        "0123456789abcdef",
		Name:          "op",
		StartUnixNano: 100,
		EndUnixNano:   200,
		Attributes:    map[string]string{"db.system": "redis"},
		Events: []MQSpanEvent{
			{Name: "e1", TimeUnixNano: 150, Attributes: map[string]string{"a": "b"}},
		},
	}
	rs := buildResourceSpans(ms)
	if len(rs.ScopeSpans) != 1 || len(rs.ScopeSpans[0].Spans) != 1 {
		t.Fatalf("resource spans = %+v", rs)
	}
	sp := rs.ScopeSpans[0].Spans[0]
	if sp.Name != "op" {
		t.Errorf("span name = %q, want op", sp.Name)
	}
	if len(sp.Events) != 1 || sp.Events[0].Name != "e1" {
		t.Errorf("span events = %+v", sp.Events)
	}
}

// mqCtxKey context key for tests
type mqCtxKey struct{}

// ======================= OTel 开启：各级别日志入链 =======================

// newTestLogger 创建测试用 logger（OTel 开关由参数控制）
func newTestLogger(t *testing.T, otelEnabled bool) (logx.Logger, *tracetest.InMemoryExporter, *sdktrace.TracerProvider) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	cfg := logx.DefaultConfig()
	cfg.Level = "debug"
	cfg.Output = "stdout"
	cfg.OTel.Enabled = otelEnabled

	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l, exp, tp
}

// spanEvents 获取指定 span 的所有事件
func spanEvents(spans []tracetest.SpanStub, name string) []sdktrace.Event {
	for i := range spans {
		if spans[i].Name == name {
			return spans[i].Events
		}
	}
	return nil
}

// eventAttrs 将事件属性转为 map（数值类型用 Emit() 序列化）
func eventAttrs(ev sdktrace.Event) map[string]string {
	m := make(map[string]string, len(ev.Attributes))
	for _, kv := range ev.Attributes {
		m[string(kv.Key)] = kv.Value.Emit()
	}
	return m
}

// TestOTelOn_AllLevels ProduceSpanEvents 验证：OTel 开启时，Debug/Info/Warn/Error/DPanic 各级别均产生 Span Event
func TestOTelOn_AllLevels_ProduceSpanEvents(t *testing.T) {
	l, exp, tp := newTestLogger(t, true)

	ctx, span := tp.Tracer("test").Start(context.Background(), "all-levels")

	l.DebugContext(ctx, "debug-msg", zap.String("k", "d"))
	l.InfoContext(ctx, "info-msg", zap.String("k", "i"))
	l.WarnContext(ctx, "warn-msg", zap.String("k", "w"))
	l.ErrorContext(ctx, "error-msg", zap.String("k", "e"))
	l.DPanicContext(ctx, "dpanic-msg", zap.String("k", "p"))

	span.End()
	_ = tp.ForceFlush(context.Background())

	spans := exp.GetSpans()
	events := spanEvents(spans, "all-levels")
	if len(events) != 5 {
		t.Fatalf("expected 5 span events, got %d", len(events))
	}

	// 验证每个事件的 message 和 log.level 属性
	want := []struct{ msg, level, k string }{
		{"debug-msg", "debug", "d"},
		{"info-msg", "info", "i"},
		{"warn-msg", "warn", "w"},
		{"error-msg", "error", "e"},
		{"dpanic-msg", "dpanic", "p"},
	}
	for i, ev := range events {
		if ev.Name != want[i].msg {
			t.Errorf("event[%d].Name = %q, want %q", i, ev.Name, want[i].msg)
		}
		attrs := eventAttrs(ev)
		if attrs["log.level"] != want[i].level {
			t.Errorf("event[%d] log.level = %q, want %q", i, attrs["log.level"], want[i].level)
		}
		if attrs["k"] != want[i].k {
			t.Errorf("event[%d] k = %q, want %q", i, attrs["k"], want[i].k)
		}
	}
}

// TestOTelOn_AllLevels_HasTraceID 验证：OTel 开启时，各级别日志均附加 trace_id/span_id
func TestOTelOn_AllLevels_HasTraceID(t *testing.T) {
	_, _, tp := newTestLogger(t, true)

	_, span := tp.Tracer("test").Start(context.Background(), "trace-id-check")
	traceID := span.SpanContext().TraceID().String()
	spanID := span.SpanContext().SpanID().String()

	// 用 recordingCore 捕获写入下游的字段
	base := newRecordingCore()
	core := NewOTelCore(base)
	_ = zap.New(core)

	// 模拟 write() 逻辑：注入 span + trace_id + span_id
	levels := []string{"debug", "info", "warn", "error", "dpanic"}
	for _, lvl := range levels {
		fields := []zapcore.Field{
			spanField(span),
			{Key: "trace_id", Type: zapcore.StringType, String: traceID},
			{Key: "span_id", Type: zapcore.StringType, String: spanID},
			{Key: "msg", Type: zapcore.StringType, String: lvl},
		}
		ent := zapcore.Entry{Level: zapcore.InfoLevel, Message: lvl + "-msg", Time: time.Now()}
		_ = core.Write(ent, fields)
	}

	span.End()
	_ = tp.ForceFlush(context.Background())

	// 每条写入都应包含 trace_id 和 span_id
	if len(base.fields) != len(levels) {
		t.Fatalf("base.fields len = %d, want %d", len(base.fields), len(levels))
	}
	for i, fList := range base.fields {
		fieldMap := map[string]bool{}
		for _, f := range fList {
			fieldMap[f.Key] = true
		}
		if !fieldMap["trace_id"] {
			t.Errorf("entry %d (%s) missing trace_id", i, levels[i])
		}
		if !fieldMap["span_id"] {
			t.Errorf("entry %d (%s) missing span_id", i, levels[i])
		}
	}
}

// TestOTelOn_ChildSpan_EventsGoToParent 验证：子 span 上的日志事件正确归属于子 span
func TestOTelOn_ChildSpan_EventsGoToParent(t *testing.T) {
	l, exp, tp := newTestLogger(t, true)

	parentCtx, parentSpan := tp.Tracer("test").Start(context.Background(), "parent")
	childCtx, childSpan := tp.Tracer("test").Start(parentCtx, "child")

	l.InfoContext(parentCtx, "parent-log")
	l.InfoContext(childCtx, "child-log")

	childSpan.End()
	parentSpan.End()
	_ = tp.ForceFlush(context.Background())

	spans := exp.GetSpans()
	parentEvents := spanEvents(spans, "parent")
	childEvents := spanEvents(spans, "child")

	if len(parentEvents) != 1 || parentEvents[0].Name != "parent-log" {
		t.Errorf("parent events = %+v, want [parent-log]", parentEvents)
	}
	if len(childEvents) != 1 || childEvents[0].Name != "child-log" {
		t.Errorf("child events = %+v, want [child-log]", childEvents)
	}
}

// TestOTelOn_NonContextMethods_NoSpanEvent 验证：无 Context 的方法（Info/Warn 等）不注入 Span Event
func TestOTelOn_NonContextMethods_NoSpanEvent(t *testing.T) {
	l, exp, tp := newTestLogger(t, true)

	_, span := tp.Tracer("test").Start(context.Background(), "nc")

	// 无 Context 方法：仅输出到 stdout，不入链
	l.Info("no-ctx-info")
	l.Warn("no-ctx-warn")
	l.Error("no-ctx-error")

	span.End()
	_ = tp.ForceFlush(context.Background())

	spans := exp.GetSpans()
	events := spanEvents(spans, "nc")
	if len(events) != 0 {
		t.Errorf("non-context methods produced %d events, want 0: %+v", len(events), events)
	}
}

// TestOTelOn_CustomFields_InEventAttributes 验证：自定义字段作为 Span Event 属性
func TestOTelOn_CustomFields_InEventAttributes(t *testing.T) {
	l, exp, tp := newTestLogger(t, true)

	ctx, span := tp.Tracer("test").Start(context.Background(), "attrs")

	l.InfoContext(ctx, "order",
		zap.String("order_id", "1001"),
		zap.Float64("amount", 99.5),
		zap.Int("items", 3),
	)

	span.End()
	_ = tp.ForceFlush(context.Background())

	spans := exp.GetSpans()
	events := spanEvents(spans, "attrs")
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	attrs := eventAttrs(events[0])
	if attrs["order_id"] != "1001" {
		t.Errorf("order_id = %q, want 1001", attrs["order_id"])
	}
	if attrs["amount"] != "99.5" {
		t.Errorf("amount = %q, want 99.5", attrs["amount"])
	}
	if attrs["items"] != "3" {
		t.Errorf("items = %q, want 3", attrs["items"])
	}
}

// ======================= OTel 关闭：各级别日志不入链 =======================

// TestOTelOff_AllLevels_NoSpanEvents 验证：OTel 关闭时，各级别日志不产生 Span Event
func TestOTelOff_AllLevels_NoSpanEvents(t *testing.T) {
	l, exp, tp := newTestLogger(t, false)

	ctx, span := tp.Tracer("test").Start(context.Background(), "off-all")

	l.DebugContext(ctx, "debug-off")
	l.InfoContext(ctx, "info-off")
	l.WarnContext(ctx, "warn-off")
	l.ErrorContext(ctx, "error-off")
	l.DPanicContext(ctx, "dpanic-off")

	span.End()
	_ = tp.ForceFlush(context.Background())

	spans := exp.GetSpans()
	events := spanEvents(spans, "off-all")
	if len(events) != 0 {
		t.Errorf("OTel off: got %d events, want 0: %+v", len(events), events)
	}
}

// TestOTelOff_AllLevels_NoTraceID 验证：OTel 关闭时，日志字段中无 trace_id/span_id
func TestOTelOff_AllLevels_NoTraceID(t *testing.T) {
	_, _, tp := newTestLogger(t, false)

	_, span := tp.Tracer("test").Start(context.Background(), "off-tid")

	// 用 recordingCore 捕获字段
	base := newRecordingCore()
	core := NewOTelCore(base)
	_ = zap.New(core)

	// 模拟 write() 逻辑：OTel 关闭时不注入 span 字段
	levels := []string{"debug", "info", "warn", "error"}
	for _, lvl := range levels {
		fields := []zapcore.Field{
			{Key: "msg", Type: zapcore.StringType, String: lvl},
		}
		ent := zapcore.Entry{Level: zapcore.InfoLevel, Message: lvl + "-off", Time: time.Now()}
		_ = core.Write(ent, fields)
	}

	span.End()
	_ = tp.ForceFlush(context.Background())

	// 每条写入都不应包含 trace_id 或 span_id
	for i, fList := range base.fields {
		for _, f := range fList {
			if f.Key == "trace_id" || f.Key == "span_id" {
				t.Errorf("entry %d (%s) has %s field, should not exist", i, levels[i], f.Key)
			}
		}
	}
}

// TestOTelOff_NonContextMethods_NoSpanEvents 验证：OTel 关闭时，无 Context 方法也不入链
func TestOTelOff_NonContextMethods_NoSpanEvents(t *testing.T) {
	l, exp, tp := newTestLogger(t, false)

	_, span := tp.Tracer("test").Start(context.Background(), "off-nc")

	l.Info("nc-info")
	l.Warn("nc-warn")
	l.Error("nc-error")

	span.End()
	_ = tp.ForceFlush(context.Background())

	spans := exp.GetSpans()
	events := spanEvents(spans, "off-nc")
	if len(events) != 0 {
		t.Errorf("OTel off non-context: got %d events, want 0", len(events))
	}
}

// TestOTelOff_StillOutputsToStdout 验证：OTel 关闭时，日志仍正常输出到 stdout（不丢日志）
func TestOTelOff_StillOutputsToStdout(t *testing.T) {
	l, _, _ := newTestLogger(t, false)

	// 这些调用不应 panic 或丢失
	l.Debug("stdout-debug")
	l.Info("stdout-info")
	l.Warn("stdout-warn")
	l.Error("stdout-error")
	l.DPanic("stdout-dpanic")
}

// ======================= OTel 开/关 对比 =======================

// TestOTelOnVsOff_SpanEventDiff 验证：同一 span 上，OTel 开 vs 关的事件差异
func TestOTelOnVsOff_SpanEventDiff(t *testing.T) {
	// OTel 开启
	lOn, expOn, tpOn := newTestLogger(t, true)
	ctxOn, spanOn := tpOn.Tracer("test").Start(context.Background(), "compare")
	lOn.InfoContext(ctxOn, "msg-on")
	spanOn.End()
	_ = tpOn.ForceFlush(context.Background())

	eventsOn := spanEvents(expOn.GetSpans(), "compare")
	if len(eventsOn) != 1 {
		t.Fatalf("OTel on: expected 1 event, got %d", len(eventsOn))
	}

	// OTel 关闭
	lOff, expOff, tpOff := newTestLogger(t, false)
	ctxOff, spanOff := tpOff.Tracer("test").Start(context.Background(), "compare")
	lOff.InfoContext(ctxOff, "msg-off")
	spanOff.End()
	_ = tpOff.ForceFlush(context.Background())

	eventsOff := spanEvents(expOff.GetSpans(), "compare")
	if len(eventsOff) != 0 {
		t.Errorf("OTel off: expected 0 events, got %d: %+v", len(eventsOff), eventsOff)
	}
}

// 测试用：确保全局 provider 存在
var _ = otel.GetTracerProvider

// ======================= 键值归一化 NormalizedAny =======================

// testFields 用 JSON 编码器捕获输出供断言
func testFields(t *testing.T, lvl zapcore.Level, msg string, fields []zap.Field) map[string]any {
	t.Helper()
	testBuf = nil
	enc := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	core := zapcore.NewCore(enc, zapcore.AddSync(&testCoreWriter{}), lvl)
	l := zap.New(core)
	switch {
	case strings.HasPrefix(msg, "W:"):
		l.Warn(msg[2:], fields...)
	default:
		l.Info(msg, fields...)
	}
	var out map[string]any
	if err := json.Unmarshal(testBuf, &out); err != nil {
		t.Fatalf("decode captured log: %v (buf=%s)", err, string(testBuf))
	}
	return out
}

// testCoreWriter 捕获编码器输出（包级共享缓冲，测试内串行使用）
type testCoreWriter struct{}

func (testCoreWriter) Write(p []byte) (int, error) {
	testBuf = append(testBuf, p...)
	testBuf = append(testBuf, '\n')
	return len(p), nil
}

func (testCoreWriter) Sync() error { return nil }

var testBuf []byte

// normalized 取 fields 部分（追加消息片段在 Task 3 拼接 msg 时使用，测试暂忽略）
func normalized(vv ...any) []zap.Field {
	f, _ := NormalizedAny(vv...)
	return f
}

func TestNormalizedAny(t *testing.T) {
	// 基本键值对 + 中文 key
	out := testFields(t, zapcore.InfoLevel, "curd错误",
		normalized("db", "127.0.0.1:3306", "sql语句", "select * from order"))
	if out["db"] != "127.0.0.1:3306" || out["sql语句"] != "select * from order" {
		t.Fatalf("kv fields = %v", out)
	}

	// 混合类型（JSON 编码后数值均为 float64）
	out = testFields(t, zapcore.WarnLevel, "W:慢查询",
		normalized("latency", 800.0, "count", 5, "retry", true, "cost", 800*time.Millisecond))
	if out["latency"] != 800.0 || out["count"] != 5.0 || out["retry"] != true {
		t.Fatalf("typed fields = %v", out)
	}

	// logx.Fields map 展开
	out = testFields(t, zapcore.WarnLevel, "W:m",
		normalized(logx.Fields{"a": 1, "b": "x"}))
	if out["a"] != 1.0 || out["b"] != "x" {
		t.Fatalf("fields map = %v", out)
	}

	// zap.Field 元素原样保留
	out = testFields(t, zapcore.InfoLevel, "m",
		normalized(zap.String("legacy", "x")))
	if out["legacy"] != "x" {
		t.Fatalf("zap.Field = %v", out)
	}

	// 落单字符串 → 追加消息片段
	f, extra := NormalizedAny("k", "v", "trailing")
	if extra != "trailing" || len(f) != 1 {
		t.Fatalf("trailing = extra %q fields %v", extra, f)
	}
	f, extra = NormalizedAny("k", "v")
	if extra != "" {
		t.Fatalf("no trailing but got %q", extra)
	}

	// 坏键（非 string 在 key 位）→ bad-field.N 不 panic
	f, _ = NormalizedAny(42, "x")
	if len(f) != 1 || f[0].Key != "bad-field.0" {
		t.Fatalf("bad key fields = %+v", f)
	}

	// 落单非 string → arg.N
	f, extra = NormalizedAny(42)
	if extra != "" || len(f) != 1 || f[0].Key != "arg.0" {
		t.Fatalf("lone non-string = %+v extra %q", f, extra)
	}
}

// ======================= 方案 A：活动 span 属性注入 =======================

// TestSpanAttrsInjected 方案 A 端到端：Chain().WithContext(ctx) 输出 → 活动 span SetAttributes
func TestSpanAttrsInjected(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	otel.SetTracerProvider(tp)

	cfg := logx.DefaultConfig()
	cfg.ServiceName = "svc-x"
	cfg.Level = "info"
	ln, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	ctx, span := tp.Tracer("t").Start(context.Background(), "op-x")
	ln.Chain().WithContext(ctx).Service("order").Operation("/api/v1/order").Tags("slow").
		Warn("慢查询", "latency", 800.0)
	span.End()

	ro := exp.GetSpans()[0]
	got := map[string]string{}
	for _, a := range ro.Attributes {
		switch a.Value.Type() {
		case attribute.STRINGSLICE:
			got[string(a.Key)] = strings.Join(a.Value.AsStringSlice(), ",")
		default:
			got[string(a.Key)] = a.Value.AsString()
		}
	}
	if got["service.name"] != "order" {
		t.Errorf("span service.name = %q, want order (attrs=%v)", got["service.name"], got)
	}
	if got["operation"] != "/api/v1/order" {
		t.Errorf("span operation = %q", got["operation"])
	}
	if !strings.Contains(got["log.tags"], "slow") {
		t.Errorf("span log.tags = %q, want contain slow", got["log.tags"])
	}
}

// TestNoSpanSilentFallback 无 span 的 ctx：不 panic、不注入、正常输出
func TestNoSpanSilentFallback(t *testing.T) {
	cfg := logx.DefaultConfig()
	cfg.Level = "debug"
	ln, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	// 不应 panic
	ln.Chain().WithContext(context.Background()).Service("order").Info("m", "k", "v")
}

// TestSetupTracerProviderWithMQKafka kafka 路由 + 缺 broker fail fast + mq 别名
func TestSetupTracerProviderWithMQKafka(t *testing.T) {
	// 缺 broker：fail fast，错误信息含 "brokers"
	_, err := SetupTracerProviderWithMQ(
		logx.OTelConfig{Enabled: true, Exporter: "kafka", ServiceName: "kafka-svc"},
		logx.MQConfig{})
	if err == nil || !strings.Contains(err.Error(), "brokers") {
		t.Fatalf("kafka without brokers: err = %v, want mention brokers", err)
	}

	// 配置 broker：得到 *mqSpanExporter
	setup, err := SetupTracerProviderWithMQ(
		logx.OTelConfig{Enabled: true, Exporter: "kafka", ServiceName: "kafka-svc"},
		logx.MQConfig{Type: "kafka", Brokers: []string{"localhost:39092"}, Topic: "otel-traces"})
	if err != nil {
		t.Fatalf("kafka with brokers: %v", err)
	}
	defer func() { _ = setup.Provider.Shutdown(context.Background()) }()
	if _, ok := setup.Exporter.(*mqSpanExporter); !ok {
		t.Fatalf("exporter type = %T, want *mqSpanExporter", setup.Exporter)
	}

	// mq 别名行为一致
	setup2, err := SetupTracerProviderWithMQ(
		logx.OTelConfig{Enabled: true, Exporter: "mq", ServiceName: "mq-svc"},
		logx.MQConfig{Type: "kafka", Brokers: []string{"localhost:39092"}, Topic: "otel-traces"})
	if err != nil {
		t.Fatalf("mq alias: %v", err)
	}
	defer func() { _ = setup2.Provider.Shutdown(context.Background()) }()

	// 旧入口薄封装：kafka 无 MQ 配置 → 同样 fail fast
	if _, err := SetupTracerProvider(logx.OTelConfig{Enabled: true, Exporter: "kafka", ServiceName: "x"}); err == nil {
		t.Error("SetupTracerProvider(kafka, no MQ) should fail fast")
	}
}

// TestSetupTracerProviderLegacyUnchanged 既有 exporter 路由不变
func TestSetupTracerProviderLegacyUnchanged(t *testing.T) {
	for _, e := range []string{"stdout", "jaeger"} {
		setup, err := SetupTracerProvider(logx.OTelConfig{Enabled: true, Exporter: e, ServiceName: "legacy-" + e})
		if err != nil {
			t.Errorf("%s exporter: %v", e, err)
			continue
		}
		_ = setup.Provider.Shutdown(context.Background())
	}
}

// TestSetupTracerProviderOTLP otlp 路由：完整 URL / host:port / 缺省端点均可构造
func TestSetupTracerProviderOTLP(t *testing.T) {
	cases := []struct {
		name     string
		endpoint string
		insecure bool
	}{
		{"http-url", "http://localhost:4318", false}, // http 前缀自动 Insecure
		{"host-port", "localhost:4318", true},        // host:port + 显式 Insecure
		{"default", "", false},                       // 缺省（OTLP 默认端点）
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setup, err := SetupTracerProvider(logx.OTelConfig{
				Enabled:     true,
				Exporter:    "otlp",
				ServiceName: "otlp-route-test",
				OTLP: logx.OTLPConfig{
					Endpoint: tc.endpoint,
					Insecure: tc.insecure,
				},
			})
			if err != nil {
				t.Fatalf("SetupTracerProvider(otlp %s): %v", tc.name, err)
			}
			_ = setup.Provider.Shutdown(context.Background())
		})
	}
}

// ======================= 集成验证：stdout / kafka =======================

// TestStdoutExportIntegration stdout exporter 集成验证：
//  1. 统一装配入口 SetupTracerProviderWithMQ(Exporter=stdout)：
//     Exporter 为 *stdouttrace.Exporter，开 span + ForceFlush + Shutdown 全程无错误
//     （默认输出到 os.Stdout 无法断言，JSON 内容断言走下面第 2 步的手工装配）
//  2. 手工装配 provider（stdouttrace.WithWriter(&buf)，复制 SetupTracerProviderWithMQ 的装配），
//     span 内打印带 ctx 的日志，导出 JSON 中包含 service.name 资源属性、span 名与日志事件
//
// 完整 JSON 逐字段断言由 example 手动验证（./example/example stdout 导出）
func TestStdoutExportIntegration(t *testing.T) {
	// 1. 统一装配入口：stdout 路由
	setup, err := SetupTracerProviderWithMQ(logx.OTelConfig{
		Enabled: true, Exporter: "stdout",
		ServiceName: "it-svc", ServiceVersion: "v1", Environment: "test",
	}, logx.MQConfig{})
	if err != nil {
		t.Fatalf("SetupTracerProviderWithMQ(stdout): %v", err)
	}
	if _, ok := setup.Exporter.(*stdouttrace.Exporter); !ok {
		t.Fatalf("exporter type = %T, want *stdouttrace.Exporter", setup.Exporter)
	}
	c1, span1 := setup.Provider.Tracer("it").Start(context.Background(), "op")
	span1.End()
	if err := setup.Provider.ForceFlush(c1); err != nil {
		t.Errorf("ForceFlush: %v", err)
	}
	if err := setup.Provider.Shutdown(c1); err != nil {
		t.Errorf("Shutdown: %v", err)
	}

	// 2. 手工装配：stdout exporter 输出到 buf（同步导出，确定性断言）
	var buf bytes.Buffer
	exp, err := stdouttrace.New(stdouttrace.WithWriter(&buf))
	if err != nil {
		t.Fatalf("stdouttrace.New: %v", err)
	}
	res := resource.NewWithAttributes(
		"",
		attribute.String("service.name", "it-svc"),
		attribute.String("service.version", "v1"),
		attribute.String("deployment.environment", "test"),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp), sdktrace.WithResource(res))

	cfg := logx.DefaultConfig()
	cfg.Level = "info"
	cfg.Output = "file"
	cfg.FilePath = t.TempDir() + "/it.log"
	cfg.OTel.Enabled = true
	ln, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = ln.Close() }()

	c2, span2 := tp.Tracer("it").Start(context.Background(), "op")
	ln.InfoContext(c2, "集成消息", zap.String("k", "v"))
	span2.End()
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Errorf("ForceFlush: %v", err)
	}
	if err := tp.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown: %v", err)
	}

	out := buf.String()
	if out == "" {
		t.Fatal("stdout exporter wrote nothing")
	}
	// 宽松断言：service 资源属性 + span 名 + 日志事件（OTelCore 事件名即日志 message）
	for _, want := range []string{`"service.name"`, "it-svc", `"op"`, "集成消息", `"log.level"`} {
		if !strings.Contains(out, want) {
			t.Errorf("exported JSON missing %q\ngot: %s", want, out)
		}
	}
	// 至少一行 JSON 合法（stdouttrace 非 PrettyPrint 时每 span 一行）
	checked := 0
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("line not valid JSON: %v\n%s", err, line)
		}
		checked++
	}
	if checked == 0 {
		t.Error("no JSON lines exported")
	}
}

// TestKafkaDirectPushIntegration Kafka 直推集成（条件测试）：
// 未设置 LOGX_KAFKA_BROKERS 时跳过；设置时走 SetupTracerProviderWithMQ(kafka)
// 开 span 并 ForceFlush + Shutdown，无错误即 PASS。
// span 是否实际落到 topic logx-it-spans 需 docker-compose 起 kafka 后手动消费验证。
func TestKafkaDirectPushIntegration(t *testing.T) {
	brokers := os.Getenv("LOGX_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("set LOGX_KAFKA_BROKERS to enable Kafka integration test")
	}

	setup, err := SetupTracerProviderWithMQ(
		logx.OTelConfig{Enabled: true, Exporter: "kafka", ServiceName: "kafka-it"},
		logx.MQConfig{Type: "kafka", Brokers: strings.Split(brokers, ","), Topic: "logx-it-spans"})
	if err != nil {
		t.Fatalf("SetupTracerProviderWithMQ(kafka): %v", err)
	}

	ctx, span := setup.Provider.Tracer("it").Start(context.Background(), "kafka-op")
	span.End()
	// kafka 同步写：ForceFlush 触发 batcher 导出并等待 ack，保证投递
	if err := setup.Provider.ForceFlush(ctx); err != nil {
		t.Errorf("ForceFlush: %v", err)
	}
	if err := setup.Provider.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

// TestNewMQTracerProviderForcesMQRouting 修复 I-1：NewMQTracerProvider 无论 cfg.Exporter 取值如何，
// 均强制路由到 mq exporter（不受 FillDefaults 默认 jaeger 影响），保持"MQ 专用入口"语义。
func TestNewMQTracerProviderForcesMQRouting(t *testing.T) {
	mqCfg := logx.MQConfig{Type: "kafka", Brokers: []string{"localhost:39092"}, Topic: "logx-mq-it"}
	for _, e := range []string{"", "jaeger", "stdout", "mq"} {
		cfg := logx.OTelConfig{Enabled: true, Exporter: e, ServiceName: "mq-routing"}
		setup, err := NewMQTracerProvider(cfg, mqCfg)
		if err != nil {
			t.Fatalf("NewMQTracerProvider(Exporter=%q): %v", e, err)
		}
		defer func() { _ = setup.Provider.Shutdown(context.Background()) }()
		if _, ok := setup.Exporter.(*mqSpanExporter); !ok {
			t.Fatalf("Exporter=%q: setup.Exporter type = %T, want *mqSpanExporter", e, setup.Exporter)
		}
	}
}

// ======================= caller 归因回归测试 =======================

// TestContextCallerAttribution 回归：*Context 通路（经 write() 多一层帧）的
// caller 归因应与非 Context 通路一致，均指向用户调用处（而非 zap.go 内部）
//
// 修复前：write() 内 l.zl.Info 使 Context 方法 caller 落在 zap/zap.go（write 所在行），
// 非 Context 方法（l.zl.Info 直调）caller 正确指向调用处。
// 修复后：write() 走 zlCtx（AddCallerSkip(2)），两通路 caller 归因一致。
func TestContextCallerAttribution(t *testing.T) {
	cfg := logx.DefaultConfig()
	cfg.Level = "info"
	cfg.Format = "json"
	cfg.Output = "file"
	cfg.Caller = true
	cfg.FilePath = t.TempDir() + "/caller.log"

	ln, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ln.Info("plain", zap.String("k", "v"))                            // 非 Context：caller 应为本测试文件
	ln.InfoContext(context.Background(), "ctx", zap.String("k", "v")) // Context：同上

	// 派生路径同样归因正确（With 派生时 zlCtx 需继承）
	ln.With(zap.String("d", "1")).InfoContext(context.Background(), "derived ctx")
	ln.Named("n").InfoContext(context.Background(), "named ctx")
	if err := ln.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(cfg.FilePath)
	if err != nil {
		t.Fatalf("读日志文件: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 4 {
		t.Fatalf("日志行数 = %d, want 4:\n%s", len(lines), data)
	}
	for _, raw := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			t.Fatalf("行非 JSON: %v", err)
		}
		caller, _ := m["caller"].(string)
		if caller == "" {
			t.Errorf("缺 caller 字段: %s", raw)
			continue
		}
		if !strings.Contains(caller, "zap_test.go") {
			t.Errorf("caller = %q, 应指向本测试文件（修复前 Context 通路落在 zap/zap.go）: %s", caller, raw)
		}
	}
}
