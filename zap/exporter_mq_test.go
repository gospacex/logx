// logx/zap/exporter_mq_test.go
// MQ 消费者回写链路（RunMQLoop）单元测试：坏消息跳过、4xx 错误体
package zap

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gospacex/logx"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// TestRunMQLoopSkipsMalformed 坏消息（JSON 解析失败 / 短 trace_id）跳过不断链，
// 合法消息正常转发（回归：topic 上的杂消息曾导致 400 中断整个回写循环）
func TestRunMQLoopSkipsMalformed(t *testing.T) {
	posts := 0
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		posts++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	good := MQSpan{
		TraceID:    "0102030405060708090a0b0c0d0e0f10",
		SpanID:     "0102030405060708",
		Name:       "good-span",
		Events:     []MQSpanEvent{},
		Attributes: map[string]string{},
		Resource:   map[string]string{"service.name": "svc"},
	}
	goodJSON, err := json.Marshal(good)
	if err != nil {
		t.Fatal(err)
	}
	shortID := MQSpan{
		TraceID:  "abc", // 非 32 hex：Jaeger 会 400 "invalid TraceID length"
		SpanID:   "def",
		Name:     "short-id",
		Resource: map[string]string{"service.name": "svc"},
	}
	shortJSON, _ := json.Marshal(shortID)

	q := &fakeQueue{}
	_ = q.Produce(context.Background(), nil, []byte(`{"garbage":`)) // 1) 坏 JSON
	_ = q.Produce(context.Background(), nil, shortJSON)             // 2) 短 trace_id
	_ = q.Produce(context.Background(), nil, goodJSON)              // 3) 合法
	// 4) 再垫一条坏 JSON 在合法消息之后：验证跳过「不中断」而非「只跳前置」
	_ = q.Produce(context.Background(), nil, []byte(`not-json`))

	c, err := NewMQToJaegerConsumer(q, srv.URL)
	if err != nil {
		t.Fatalf("NewMQToJaegerConsumer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 队列耗尽后 fakeQueue.Consume 返回 context.Canceled → 循环以 consume 错误退出，属预期
	_ = c.RunMQLoop(ctx)

	mu.Lock()
	got := posts
	mu.Unlock()
	if got != 1 {
		t.Errorf("OTLP posts = %d, want 1（合法消息应转发，坏消息应跳过）", got)
	}
}

// TestRunMQLoopOTLP400ErrorCarriesBody 4xx 时错误信息携带截断响应体（定位 partial_success 原因）
func TestRunMQLoopOTLP400ErrorCarriesBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"unmarshal: invalid TraceID length":true}`))
	}))
	defer srv.Close()

	good := MQSpan{
		TraceID:  "0102030405060708090a0b0c0d0e0f10",
		SpanID:   "0102030405060708",
		Name:     "x",
		Resource: map[string]string{"service.name": "svc"},
	}
	data, _ := json.Marshal(good)
	q := &fakeQueue{}
	_ = q.Produce(context.Background(), nil, data)

	c, err := NewMQToJaegerConsumer(q, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = c.RunMQLoop(context.Background())
	if err == nil {
		t.Fatal("expected error on 4xx")
	}
	if got := err.Error(); !strings.Contains(got, "unexpected status 400") || !strings.Contains(got, "invalid TraceID length") {
		t.Errorf("error = %q, want contains 'unexpected status 400' + 截断响应体", got)
	}
}

// TestValidSpanIDs 长度校验（16/8 字节 hex 定长）
func TestValidSpanIDs(t *testing.T) {
	cases := []struct {
		in   MQSpan
		want bool
	}{
		{MQSpan{TraceID: "00000000000000000000000000000000", SpanID: "0000000000000000"}, true},
		{MQSpan{TraceID: "0102030405060708090a0b0c0d0e0f10", SpanID: "0102030405060708"}, true},
		{MQSpan{TraceID: "abc", SpanID: "def"}, false}, // 短 ID（历史坏消息形态）
		{MQSpan{TraceID: "", SpanID: ""}, false},
	}
	for i, tc := range cases {
		if got := validSpanIDs(tc.in); got != tc.want {
			t.Errorf("case %d: validSpanIDs = %v, want %v", i, got, tc.want)
		}
	}
}

// TestNewOTLPExporterRouting 端点形态路由：http:// URL 自动 Insecure，host:port 走 WithEndpoint
// （构造级断言：三种形态均可装配，Shutdown 无错误）
func TestNewOTLPExporterRouting(t *testing.T) {
	forms := []struct {
		name     string
		endpoint string
		insecure bool
	}{
		{"http-url", "http://127.0.0.1:1", false}, // http 前缀 → 自动 Insecure
		{"https-url", "https://collector:4318", false},
		{"host-port", "localhost:4318", true},
		{"empty", "", false},
	}
	for _, tc := range forms {
		t.Run(tc.name, func(t *testing.T) {
			cfg := logx.OTelConfig{
				Enabled:     true,
				Exporter:    "otlp",
				ServiceName: "otlp-routing-test",
				OTLP:        logx.OTLPConfig{Endpoint: tc.endpoint, Insecure: tc.insecure},
			}
			setup, err := SetupTracerProvider(cfg)
			if err != nil {
				t.Fatalf("SetupTracerProvider(otlp %s): %v", tc.name, err)
			}
			if err := setup.Provider.Shutdown(context.Background()); err != nil {
				t.Errorf("Shutdown(%s): %v", tc.name, err)
			}
		})
	}
}

// TestBuildResourceSpansResource MQSpan.Resource 还原 + 旧格式消息回退 logx-mq-legacy
func TestBuildResourceSpansResource(t *testing.T) {
	// 新格式：Resource 非空 → 原样还原 service.name
	ms := MQSpan{
		TraceID:  "0102030405060708090a0b0c0d0e0f10",
		SpanID:   "0102030405060708",
		Name:     "s",
		Resource: map[string]string{"service.name": "svc-x", "deployment.environment": "dev"},
	}
	rs := buildResourceSpans(ms)
	got := resourceAttr(rs.Resource)
	if got["service.name"] != "svc-x" || got["deployment.environment"] != "dev" {
		t.Errorf("resource = %v, want svc-x/dev 原样还原", got)
	}

	// 旧格式（无 Resource 字段）：回退 logx-mq-legacy，避免落入 OTLPResourceNoServiceName
	legacy := MQSpan{TraceID: "0102030405060708090a0b0c0d0e0f10", SpanID: "0102030405060708", Name: "s"}
	legacyGot := resourceAttr(buildResourceSpans(legacy).Resource)
	if legacyGot["service.name"] != "logx-mq-legacy" {
		t.Errorf("legacy fallback service.name = %q, want logx-mq-legacy", legacyGot["service.name"])
	}
}

// resourceAttr 提取 OTLP Resource 属性为 map
func resourceAttr(res *resourcepb.Resource) map[string]string {
	m := map[string]string{}
	if res == nil {
		return m
	}
	for _, kv := range res.Attributes {
		if sv := kv.Value.GetStringValue(); sv != "" {
			m[kv.Key] = sv
		}
	}
	return m
}

// TestIdToBytes 16/8 字节 hex 定长转换 + 空/全零形态
func TestIdToBytes(t *testing.T) {
	if b := idToBytes(""); b != nil {
		t.Errorf("empty id = %v, want nil", b)
	}
	if b := idToBytes("0000000000000000"); b != nil {
		t.Errorf("all-zero trace id = %v, want nil（与无 parent 语义一致）", b)
	}
	got := idToBytes("0102030405060708090a0b0c0d0e0f10")
	if len(got) != 16 {
		t.Fatalf("trace id bytes = %d, want 16", len(got))
	}
	for i, want := range []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0x10} {
		if got[i] != want {
			t.Errorf("byte[%d] = 0x%02x, want 0x%02x", i, got[i], want)
			break
		}
	}
}

// TestTruncate 错误信息截断
func TestTruncate(t *testing.T) {
	if got := truncate([]byte("short"), 200); got != "short" {
		t.Errorf("short = %q, want 原样", got)
	}
	if got := truncate([]byte("0123456789"), 4); got != "0123...(truncated)" {
		t.Errorf("truncated = %q, want 前 4 字符 + 标记", got)
	}
}
