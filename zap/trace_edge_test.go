// logx/zap/trace_edge_test.go
// traceFields 分支补全（remote span ctx / 无效 ctx）、Panic 级别真实 panic 通路、
// exportOne 202 可接受状态、buildResourceSpans 事件与父 span 转换
package zap

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

// TestTraceFieldsBranches traceFields 全分支：
// - OTel 未开启 / ctx 为 nil / ctx 无 span → nil
// - remote span ctx → trace_id + span_id + remote=true 三字段
func TestTraceFieldsBranches(t *testing.T) {
	if f := traceFields(nil, true); f != nil {
		t.Errorf("traceFields(nil) = %v, want nil", f)
	}
	if f := traceFields(context.Background(), false); f != nil {
		t.Errorf("traceFields(OTel 未开启) = %v, want nil", f)
	}
	if f := traceFields(context.Background(), true); f != nil {
		t.Errorf("traceFields(无 span ctx) = %v, want nil", f)
	}

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		SpanID:     trace.SpanID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), sc)
	fields := traceFields(ctx, true)
	if len(fields) != 3 {
		t.Fatalf("remote span ctx 字段数 = %d, want 3 (trace_id/span_id/remote)", len(fields))
	}
	if fields[0].Key != "trace_id" || fields[0].String != "0102030405060708090a0b0c0d0e0f10" {
		t.Errorf("fields[0] = key=%s val=%q, want trace_id", fields[0].Key, fields[0].String)
	}
	if fields[1].Key != "span_id" || fields[1].String != "0102030405060708" {
		t.Errorf("fields[1] = key=%s val=%q, want span_id", fields[1].Key, fields[1].String)
	}
	if fields[2].Key != "remote" || fields[2].Integer != 1 {
		t.Errorf("fields[2] = key=%s int=%d, want remote=true", fields[2].Key, fields[2].Integer)
	}
}

// TestLoggerPanicPanics Panic 级日志通路真实 panic（zap zapcore.PanicLevel → panic 契约）
// 子测试内 recover 兜底，不影响主测试进程
func TestLoggerPanicPanics(t *testing.T) {
	l, _, tp := newTestLogger(t, false)
	defer tp.Shutdown(context.Background())
	defer l.Close()

	t.Run("non-context", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("l.Panic 未 panic")
			}
		}()
		l.Panic("panic m")
	})

	t.Run("context", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("l.PanicContext 未 panic")
			}
		}()
		l.PanicContext(context.Background(), "panic ctx m")
	})
}

// TestExportOneAcceptedStatus exportOne 对 202 与 200 同等视为成功（OTLP partial_success 通路）
func TestExportOneAcceptedStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	c, err := NewMQToJaegerConsumer(&fakeQueue{}, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	ms := MQSpan{
		TraceID:  "0102030405060708090a0b0c0d0e0f10",
		SpanID:   "0102030405060708",
		Name:     "accepted",
		Resource: map[string]string{"service.name": "svc"},
	}
	if err := c.exportOne(context.Background(), ms); err != nil {
		t.Errorf("exportOne(202) = %v, want nil（202 视为成功）", err)
	}
}

// TestBuildResourceSpansEventsAndParent MQSpan 事件 / 父 span / 资源属性完整转换
func TestBuildResourceSpansEventsAndParent(t *testing.T) {
	ms := MQSpan{
		TraceID:      "0102030405060708090a0b0c0d0e0f10",
		SpanID:       "0102030405060708",
		ParentSpanID: "ffeeddccbbaa9988",
		Name:         "op",
		Attributes:   map[string]string{"k": "v"},
		Resource:     map[string]string{"service.name": "svc"},
		Events: []MQSpanEvent{
			{Name: "ev1", TimeUnixNano: 1, Attributes: map[string]string{"a": "1"}},
			{Name: "ev2", TimeUnixNano: 2},
		},
	}
	rs := buildResourceSpans(ms)
	span := rs.ScopeSpans[0].Spans[0]

	if got := string(span.ParentSpanId); got != "\xff\xee\xdd\xcc\xbb\xaa\x99\x88" {
		t.Errorf("ParentSpanId 字节 = %q, want 8 字节", got)
	}
	if len(span.Events) != 2 {
		t.Fatalf("Events = %d, want 2", len(span.Events))
	}
	if span.Events[0].Name != "ev1" || len(span.Events[0].Attributes) != 1 {
		t.Errorf("Events[0] = %+v, want ev1 + 1 属性", span.Events[0])
	}
	if span.Events[1].Name != "ev2" || len(span.Events[1].Attributes) != 0 {
		t.Errorf("Events[1] = %+v, want ev2 无属性", span.Events[1])
	}
	// 资源与 span 属性各 1 键
	if len(rs.Resource.Attributes) != 1 {
		t.Errorf("Resource.Attributes = %d 键, want 1", len(rs.Resource.Attributes))
	}
	if len(span.Attributes) != 1 {
		t.Errorf("span.Attributes = %d 键, want 1", len(span.Attributes))
	}
}
