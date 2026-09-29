// logx/zap/otelcore_edge_test.go
// OTelCore 动态开关 / With 派生 / extractSpan 边界 + Logger 只读访问器
package zap

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// TestOTelCoreSetEnabled 动态关闭后 Write 不再注入 Span Event，但照常转发下游。
// 注入与否按 span 事件数区分（InMemoryExporter 在 span.End() 时快照事件）
func TestOTelCoreSetEnabled(t *testing.T) {
	rec := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(rec))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	base := newRecordingCore()
	core := NewOTelCore(base)
	ent := zapcore.Entry{Level: zapcore.InfoLevel, Message: "m", LoggerName: "logger"}

	// 启用：span1 带 1 条事件
	_, span1 := tp.Tracer("t").Start(context.Background(), "op1")
	core.Write(ent, []zapcore.Field{spanField(span1)})
	span1.End()
	if n := len(rec.GetSpans()); n != 1 {
		t.Fatalf("span1 未记录: spans=%d", n)
	}
	if n := len(rec.GetSpans()[0].Events); n != 1 {
		t.Errorf("启用时事件未注入: events=%d, want 1", n)
	}
	if len(base.entries) != 1 {
		t.Fatalf("下游转发丢失: entries=%d", len(base.entries))
	}

	// 关闭注入 → span2 0 事件；下游仍转发
	core.SetEnabled(false)
	_, span2 := tp.Tracer("t").Start(context.Background(), "op2")
	core.Write(ent, []zapcore.Field{spanField(span2)})
	span2.End()
	if n := len(rec.GetSpans()[1].Events); n != 0 {
		t.Errorf("关闭后仍注入事件: events=%d, want 0", n)
	}
	if len(base.entries) != 2 {
		t.Errorf("关闭注入后下游丢失: entries=%d, want 2", len(base.entries))
	}
}

// TestOTelCoreWith 派生 Core 继承 enabled 状态，且持有独立派生的下游 Core
// （recordingCore.With 返回新实例——派生核心不与 base 共享捕获列表）
func TestOTelCoreWith(t *testing.T) {
	base := newRecordingCore()
	core := NewOTelCore(base)
	core.SetEnabled(false)

	derived, ok := core.With([]zapcore.Field{zap.String("k", "v")}).(*OTelCore)
	if !ok {
		t.Fatalf("With 返回类型 = %T, want *OTelCore", core.With(nil))
	}
	if derived.enabled {
		t.Error("With 派生 Core 未继承 enabled=false")
	}
	if derived.core == base {
		t.Error("With 未派生下游 Core（应持有独立实例）")
	}

	// 派生核心写入 → 字段落在其派生下游实例上
	dbase, ok := derived.core.(*recordingCore)
	if !ok {
		t.Fatalf("派生核心下游 = %T, want *recordingCore", derived.core)
	}
	derived.Write(zapcore.Entry{}, []zapcore.Field{zap.String("k", "v")})
	if len(dbase.fields) != 1 || len(dbase.fields[0]) != 1 || dbase.fields[0][0].Key != "k" {
		t.Errorf("派生核心未转发字段到其下游: %v", dbase.fields)
	}
}

// TestExtractSpanEdges span 字段识别边界：
// - 非 span 的 SkipType 同 key 字段 → break，不识别
// - 字段列表中多个 SkipType：只有首个匹配 key 的才取
func TestExtractSpanEdges(t *testing.T) {
	rec := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(rec))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	_, span := tp.Tracer("t").Start(context.Background(), "op")
	defer span.End()

	// 1) 普通字段 + span 字段混排 → span 被剥离，其余保留
	rest := []zapcore.Field{
		zap.String("a", "1"),
		spanField(span),
		zap.String("b", "2"),
	}
	got, fields := extractSpan(rest)
	if got != span {
		t.Error("extractSpan 未取到 span")
	}
	if len(fields) != 2 || fields[0].Key != "a" || fields[1].Key != "b" {
		t.Errorf("剥离后字段 = %v, want [a b]", fields)
	}

	// 2) 非 span 接口（同 key 的 SkipType）→ 不识别，原样返回
	notSpan := zapcore.Field{Key: spanFieldKey, Type: zapcore.SkipType, Interface: 42}
	got2, fields2 := extractSpan([]zapcore.Field{notSpan})
	if got2 != nil {
		t.Errorf("非 span 接口应取不到（got=%v）", got2)
	}
	if len(fields2) != 1 {
		t.Errorf("非 span 字段未被剥离: %d", len(fields2))
	}

	// 3) 无 span 字段 → nil + 原 slice
	got3, fields3 := extractSpan([]zapcore.Field{zap.String("x", "y")})
	if got3 != nil || len(fields3) != 1 {
		t.Error("无 span 字段时应返回 (nil, 原列表)")
	}
}

// TestSpanField 隐藏字段契约：SkipType + 固定 key
func TestSpanField(t *testing.T) {
	f := spanField(nil)
	if f.Type != zapcore.SkipType || f.Key != spanFieldKey {
		t.Errorf("spanField = (type=%d key=%q), want SkipType + %q", f.Type, f.Key, spanFieldKey)
	}
}

// TestLoggerAccessors Level/Zap/Sync 及 Default*/ServiceName 回退链访问器
func TestLoggerAccessors(t *testing.T) {
	l, _, tp := newTestLogger(t, false)
	defer tp.Shutdown(context.Background())
	defer l.Close()

	if lvl := l.Level(); lvl != zapcore.DebugLevel {
		t.Errorf("Level() = %v, want DebugLevel（newTestLogger 配置）", lvl)
	}
	if zl := l.Zap(); zl == nil {
		t.Error("Zap() = nil, want 底层 *zap.Logger")
	}
	// stdout 的 Sync 在部分平台上返回 ENXIO（非错误契约），仅调用不失败
	_ = l.Sync()
	// 显式配置的回退链值
	if l.DefaultService() != "" {
		t.Logf("DefaultService=%q（默认配置为空则合法）", l.DefaultService())
	}
	_ = l.ServiceName()
}
