// logx/example/06-http-middleware/06_test.go
// 用例 06 测试：Hertz 请求级日志中间件（不起真实 listener，用引擎 ServeHTTP 驱动）
//
// 复用 main.go 的 handler() 与常量（同包），验证 main.go 演示的契约：
//
//   - X-Request-ID 生成并回写响应头（16 字符 hex）
//   - 请求级 logger 经 common.GetLogger(ctx) 提取（== 配置的 stub，Named(reqID)）
//   - 快速请求访问日志 INFO；慢请求（800ms > 500ms 阈值）升级 WARN + http.slow
//   - 未挂中间件时 GetRequestID 返回 ""
package main

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/gospacex/logx"
	"github.com/gospacex/logx/common"
	logxhertz "github.com/gospacex/logx/middleware"
	_ "github.com/gospacex/logx/zap" // 注册默认 provider（与用例 06 一致）
)

// ---------- 记录桩（零 IO） ----------

type recEntry struct {
	level zapcore.Level
	msg   string
	keys  []string
}

type recLogger struct {
	mu       sync.Mutex
	entries  []recEntry
	lastName string
}

var _ logx.Logger = (*recLogger)(nil)

func newRecLogger() *recLogger { return &recLogger{} }

func (r *recLogger) record(lvl zapcore.Level, msg string, fields ...logx.Field) {
	keys := make([]string, 0, len(fields))
	for _, f := range fields {
		keys = append(keys, f.Key)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, recEntry{level: lvl, msg: msg, keys: keys})
}

func (r *recLogger) snapshot() []recEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recEntry, len(r.entries))
	copy(out, r.entries)
	return out
}

func (r *recLogger) namedForTest() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastName
}

func (r *recLogger) Debug(_ string, _ ...logx.Field)  {}
func (r *recLogger) Info(_ string, _ ...logx.Field)   {}
func (r *recLogger) Warn(_ string, _ ...logx.Field)   {}
func (r *recLogger) Error(_ string, _ ...logx.Field)  {}
func (r *recLogger) DPanic(_ string, _ ...logx.Field) {}
func (r *recLogger) Panic(_ string, _ ...logx.Field)  {}
func (r *recLogger) Fatal(_ string, _ ...logx.Field)  {}
func (r *recLogger) DebugContext(_ context.Context, msg string, f ...logx.Field) {
	r.record(zapcore.DebugLevel, msg, f...)
}
func (r *recLogger) InfoContext(_ context.Context, msg string, f ...logx.Field) {
	r.record(zapcore.InfoLevel, msg, f...)
}
func (r *recLogger) WarnContext(_ context.Context, msg string, f ...logx.Field) {
	r.record(zapcore.WarnLevel, msg, f...)
}
func (r *recLogger) ErrorContext(_ context.Context, msg string, f ...logx.Field) {
	r.record(zapcore.ErrorLevel, msg, f...)
}
func (r *recLogger) DPanicContext(_ context.Context, msg string, f ...logx.Field) {
	r.record(zapcore.DPanicLevel, msg, f...)
}
func (r *recLogger) PanicContext(_ context.Context, msg string, f ...logx.Field) {}
func (r *recLogger) FatalContext(_ context.Context, msg string, f ...logx.Field) {}

func (r *recLogger) Named(name string) logx.Logger {
	r.mu.Lock()
	r.lastName = name
	r.mu.Unlock()
	return r
}
func (r *recLogger) With(_ ...logx.Field) logx.Logger                           { return r }
func (r *recLogger) Level() zapcore.Level                                       { return zapcore.DebugLevel }
func (r *recLogger) Sync() error                                                { return nil }
func (r *recLogger) Close() error                                               { return nil }
func (r *recLogger) Zap() *zap.Logger                                           { return zap.NewNop() }
func (r *recLogger) Chain() *logx.Builder                                       { return (&logx.Builder{}).Attach(r) }
func (r *recLogger) DefaultService() string                                     { return "" }
func (r *recLogger) DefaultOperation() string                                   { return "" }
func (r *recLogger) DefaultTags() []string                                      { return nil }
func (r *recLogger) ServiceName() string                                        { return "rec" }
func (r *recLogger) InjectSpanAttrs(_ context.Context, _, _ string, _ []string) {}

// ---------- 测试引擎（不起 listener，不占端口） ----------

// newTestEngine 镜像 main.go 的服务装配（hertz 引擎 + 中间件 + /fast、/slow 路由），
// 以 base logger 替换 TracerProvider（测试无 OTel 装配，用全局 no-op tracer）
func newTestEngine(stub *recLogger) *server.Hertz {
	tracer := otel.Tracer(tracerInstrumentation) // 未装配 → 全局 no-op
	h := server.New(server.WithHostPorts("127.0.0.1:18096"))
	h.Use(logxhertz.Middleware(&logxhertz.MiddlewareConfig{
		Logger:        stub,
		SlowThreshold: slowThreshold, // 与 main.go 常量一致（500ms）
	}))
	h.GET("/fast", handler(tracer, "fast"))
	h.GET("/slow", handler(tracer, "slow"))
	return h
}

func doGet(t *testing.T, h *server.Hertz, path string) *app.RequestContext {
	t.Helper()
	req := protocol.NewRequest(http.MethodGet, "http://127.0.0.1:18096"+path, nil)
	rc := app.NewContext(0)
	req.CopyTo(&rc.Request)
	h.ServeHTTP(context.Background(), rc)
	return rc
}

// ---------- 用例 ----------

// TestExample06FastRequest INFO §1：快速请求 → 200 + X-Request-ID + INFO 访问日志
func TestExample06FastRequest(t *testing.T) {
	stub := newRecLogger()
	h := newTestEngine(stub)
	rc := doGet(t, h, "/fast")

	if rc.Response.StatusCode() != consts.StatusOK {
		t.Fatalf("状态码 = %d, want 200", rc.Response.StatusCode())
	}
	if string(rc.Response.Body()) != "ok" {
		t.Errorf("body = %q, want ok", rc.Response.Body())
	}
	reqID := logxhertz.GetRequestID(rc)
	if len(reqID) != 16 {
		t.Errorf("请求 id = %q, want 16 字符 hex", reqID)
	}
	if got := rc.Response.Header.Get("X-Request-ID"); got != reqID {
		t.Errorf("X-Request-ID 响应头 = %q, want %q", got, reqID)
	}

	// handler 内 common.GetLogger 提取的请求级 logger == 配置 stub
	h.GET("/probe", func(ctx context.Context, c *app.RequestContext) {
		got := common.GetLogger(ctx)
		if got != logx.Logger(stub) {
			t.Errorf("handler ctx 中的请求级 logger = %T, want %T", got, stub)
		}
		c.String(consts.StatusOK, "ok")
	})
	_ = doGet(t, h, "/probe")

	// 访问日志：INFO + http.* 字段（fast 路径不含 http.slow）
	// 注意：桩还记录了 handler 内的 "fast handler done"（keys=[request_id]），
	// 访问日志按 msg 定位（中间件契约消息）
	entries := stub.snapshot()
	visit := findByMsg(entries, "http request")
	if visit == nil {
		t.Fatalf("未找到访问日志（msg=%q，已有: %v）", "http request", entryMsgs(entries))
	}
	if visit.level != zapcore.InfoLevel {
		t.Errorf("快速请求访问日志级别 = %v, want INFO", visit.level)
	}
	for _, k := range []string{"http.method", "http.route", "http.request.id", "http.response.status_code"} {
		if !containsStr(visit.keys, k) {
			t.Errorf("访问日志缺字段 %q（keys: %v）", k, visit.keys)
		}
	}
	if containsStr(visit.keys, "http.slow") {
		t.Error("快速请求不应有 http.slow 字段")
	}
}

// TestExample06SlowRequest WARN §2：慢请求（slowSleep 800ms > slowThreshold 500ms）
// 访问日志升级为 WARN + http.slow 字段（main.go 演示的核心断言）
func TestExample06SlowRequest(t *testing.T) {
	stub := newRecLogger()
	h := newTestEngine(stub)
	rc := doGet(t, h, "/slow")

	if rc.Response.StatusCode() != consts.StatusOK {
		t.Fatalf("状态码 = %d, want 200", rc.Response.StatusCode())
	}
	entries := stub.snapshot()
	visit := findByMsg(entries, "slow http request")
	if visit == nil {
		t.Fatalf("未找到慢请求访问日志（msg=%q，已有: %v）", "slow http request", entryMsgs(entries))
	}
	if visit.level != zapcore.WarnLevel {
		t.Errorf("慢请求访问日志级别 = %v, want WARN（duration > 500ms）", visit.level)
	}
	if !containsStr(visit.keys, "http.slow") {
		t.Errorf("慢请求访问日志缺 http.slow 字段（keys: %v）", visit.keys)
	}
}

// TestExample06NoMiddlewareNoRequestID §3 边界：未挂中间件的 ctx 上 GetRequestID 返回 ""
func TestExample06NoMiddlewareNoRequestID(t *testing.T) {
	rc := app.NewContext(0)
	if got := logxhertz.GetRequestID(rc); got != "" {
		t.Errorf("未挂中间件 GetRequestID = %q, want 空", got)
	}
	// nil 入参契约
	if got := logxhertz.GetRequestID(nil); got != "" {
		t.Errorf("nil RequestContext GetRequestID = %q, want 空", got)
	}
}

// containsStr []string 是否包含 want
func containsStr(in []string, want string) bool {
	for _, s := range in {
		if s == want {
			return true
		}
	}
	return false
}

// findByMsg 按消息取桩记录（访问日志消息常量在 middleware 包内部，此处以字面量匹配）
func findByMsg(entries []recEntry, msg string) *recEntry {
	for i := range entries {
		if entries[i].msg == msg {
			return &entries[i]
		}
	}
	return nil
}

// entryMsgs 快照消息列表（诊断用）
func entryMsgs(entries []recEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.msg)
	}
	return out
}
