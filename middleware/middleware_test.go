// logx/middleware/middleware_test.go
// 表驱动测试：stub logger（零 IO）+ hertz 引擎 ServeHTTP（不起真实 listener、不占端口）
package middleware

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/gospacex/logx"
	"github.com/gospacex/logx/common"
)

// ---------- stub logger：记录级别/消息/字段 key 集合与 Named 调用名 ----------

type logEntry struct {
	level zapcore.Level
	msg   string
	keys  []string
}

type stubLogger struct {
	mu       sync.Mutex
	entries  []logEntry
	lastName string
}

var _ logx.Logger = (*stubLogger)(nil)

func newStub() *stubLogger { return &stubLogger{} }

func (s *stubLogger) record(level zapcore.Level, msg string, fields ...logx.Field) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, logEntry{level: level, msg: msg, keys: fieldKeys(fields)})
}

func (s *stubLogger) snapshot() []logEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]logEntry, len(s.entries))
	copy(out, s.entries)
	return out
}

func (s *stubLogger) NamedForTest() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastName
}

func fieldKeys(fields []logx.Field) []string {
	keys := make([]string, 0, len(fields))
	for _, f := range fields {
		keys = append(keys, f.Key)
	}
	return keys
}

// 非 Context 各级别不记录（中间件只走 Context 通路）
func (s *stubLogger) Debug(_ string, _ ...logx.Field)  {}
func (s *stubLogger) Info(_ string, _ ...logx.Field)   {}
func (s *stubLogger) Warn(_ string, _ ...logx.Field)   {}
func (s *stubLogger) Error(_ string, _ ...logx.Field)  {}
func (s *stubLogger) DPanic(_ string, _ ...logx.Field) {}
func (s *stubLogger) Panic(_ string, _ ...logx.Field)  {}
func (s *stubLogger) Fatal(_ string, _ ...logx.Field)  {}

// Context 各级别统一记录
func (s *stubLogger) DebugContext(_ context.Context, msg string, fields ...logx.Field) {
	s.record(zapcore.DebugLevel, msg, fields...)
}
func (s *stubLogger) InfoContext(_ context.Context, msg string, fields ...logx.Field) {
	s.record(zapcore.InfoLevel, msg, fields...)
}
func (s *stubLogger) WarnContext(_ context.Context, msg string, fields ...logx.Field) {
	s.record(zapcore.WarnLevel, msg, fields...)
}
func (s *stubLogger) ErrorContext(_ context.Context, msg string, fields ...logx.Field) {
	s.record(zapcore.ErrorLevel, msg, fields...)
}
func (s *stubLogger) DPanicContext(_ context.Context, msg string, fields ...logx.Field) {
	s.record(zapcore.DPanicLevel, msg, fields...)
}
func (s *stubLogger) PanicContext(_ context.Context, msg string, fields ...logx.Field) {
	s.record(zapcore.PanicLevel, msg, fields...)
}
func (s *stubLogger) FatalContext(_ context.Context, msg string, fields ...logx.Field) {
	s.record(zapcore.FatalLevel, msg, fields...)
}

// 派生方法：stub 恒为自身
func (s *stubLogger) Named(name string) logx.Logger {
	s.mu.Lock()
	s.lastName = name
	s.mu.Unlock()
	return s
}
func (s *stubLogger) With(_ ...logx.Field) logx.Logger                           { return s }
func (s *stubLogger) Level() zapcore.Level                                       { return zapcore.InfoLevel }
func (s *stubLogger) Sync() error                                                { return nil }
func (s *stubLogger) Close() error                                               { return nil }
func (s *stubLogger) Zap() *zap.Logger                                           { return zap.NewNop() }
func (s *stubLogger) Chain() *logx.Builder                                       { return &logx.Builder{} }
func (s *stubLogger) DefaultService() string                                     { return "" }
func (s *stubLogger) DefaultOperation() string                                   { return "" }
func (s *stubLogger) DefaultTags() []string                                      { return nil }
func (s *stubLogger) ServiceName() string                                        { return "" }
func (s *stubLogger) InjectSpanAttrs(_ context.Context, _, _ string, _ []string) {}

// ---------- 测试辅助 ----------

// runRequest 构建 hertz 引擎（不 Run、不占端口），对 /ping 发起一次请求。
// cfg.Logger 为 stub 时返回该 stub 与 handler 内断言错误列表；否则 stub 为 nil。
func runRequest(t *testing.T, cfg *MiddlewareConfig, handlerDelay time.Duration) (*app.RequestContext, *stubLogger, []string) {
	t.Helper()

	var stub *stubLogger
	if cfg != nil {
		if s, ok := cfg.Logger.(*stubLogger); ok {
			stub = s
		}
	}

	h := server.New(server.WithHostPorts("127.0.0.1:18090"))
	h.Use(Middleware(cfg))

	handlerErrs := make([]string, 0, 4)
	h.GET("/ping", func(ctx context.Context, c *app.RequestContext) {
		if stub != nil {
			got, ok := common.TryGetLogger(ctx)
			if !ok {
				handlerErrs = append(handlerErrs, "handler ctx 未注入请求级 logger")
			} else if got != logx.Logger(stub) {
				handlerErrs = append(handlerErrs, "handler ctx 中的 logger 与配置传入的 logger 不一致")
			}
		}
		reqID := GetRequestID(c)
		if reqID == "" {
			handlerErrs = append(handlerErrs, "handler 内 GetRequestID 为空")
		} else if hdr := c.Response.Header.Get(headerRequestID); hdr != reqID {
			handlerErrs = append(handlerErrs, "handler 内 X-Request-ID 响应头与请求 id 不一致: "+hdr)
		}
		time.Sleep(handlerDelay)
		c.String(consts.StatusOK, "ok")
	})

	req := protocol.NewRequest(http.MethodGet, "http://127.0.0.1:18090/ping", nil)
	rc := app.NewContext(0)
	req.CopyTo(&rc.Request)
	h.ServeHTTP(context.Background(), rc)
	return rc, stub, handlerErrs
}

func assertInKeys(t *testing.T, keys []string, want string) {
	t.Helper()
	for _, k := range keys {
		if k == want {
			return
		}
	}
	t.Errorf("字段 %q 未出现在日志中: %v", want, keys)
}

func assertNotInKeys(t *testing.T, keys []string, notWant string) {
	t.Helper()
	for _, k := range keys {
		if k == notWant {
			t.Errorf("字段 %q 不应出现在日志中: %v", notWant, keys)
		}
	}
}

var visitLogKeys = []string{
	"http.method", "http.route", "http.request.id",
	"http.response.status_code", "http.response.duration",
}

// ---------- 用例 ----------

// 用例 1：命中 handler——响应头 X-Request-ID、handler 内 GetLogger 为注入桩且 Named == reqID
func TestMiddlewareVisitLog(t *testing.T) {
	stub := newStub()
	cfg := &MiddlewareConfig{Logger: stub, SlowThreshold: 0}
	rc, _, handlerErrs := runRequest(t, cfg, time.Millisecond)

	for _, e := range handlerErrs {
		t.Errorf("handler 断言失败: %s", e)
	}
	if got := rc.Response.StatusCode(); got != consts.StatusOK {
		t.Errorf("响应状态码 = %d, want %d", got, consts.StatusOK)
	}
	if string(rc.Response.Body()) != "ok" {
		t.Errorf("响应 body = %q, want %q", rc.Response.Body(), "ok")
	}

	reqID := GetRequestID(rc)
	if len(reqID) != 16 {
		t.Errorf("请求 id = %q, want 16 字符 hex", reqID)
	}
	if got := rc.Response.Header.Get(headerRequestID); got != reqID {
		t.Errorf("X-Request-ID = %q, want %q", got, reqID)
	}
	if got := stub.NamedForTest(); got != reqID {
		t.Errorf("Named = %q, want 请求 id %q", got, reqID)
	}

	entries := stub.snapshot()
	if len(entries) != 1 {
		t.Fatalf("日志条数 = %d, want 1", len(entries))
	}
	visit := entries[0]
	if visit.level != zapcore.InfoLevel {
		t.Errorf("级别 = %v, want %v", visit.level, zapcore.InfoLevel)
	}
	if visit.msg != msgVisitLog {
		t.Errorf("消息 = %q, want %q", visit.msg, msgVisitLog)
	}
	for _, want := range visitLogKeys {
		assertInKeys(t, visit.keys, want)
	}
	assertNotInKeys(t, visit.keys, "http.slow")
}

// 用例 2：慢请求（handler sleep 60ms，阈值 10ms）——桩记录 Warn + http.slow 字段
func TestMiddlewareSlowRequestWarns(t *testing.T) {
	stub := newStub()
	cfg := &MiddlewareConfig{Logger: stub, SlowThreshold: 10 * time.Millisecond}
	rc, _, handlerErrs := runRequest(t, cfg, 60*time.Millisecond)

	for _, e := range handlerErrs {
		t.Errorf("handler 断言失败: %s", e)
	}
	reqID := GetRequestID(rc)
	if got := stub.NamedForTest(); got != reqID {
		t.Errorf("Named = %q, want 请求 id %q", got, reqID)
	}

	entries := stub.snapshot()
	if len(entries) != 1 {
		t.Fatalf("日志条数 = %d, want 1", len(entries))
	}
	slow := entries[0]
	if slow.level != zapcore.WarnLevel {
		t.Errorf("级别 = %v, want %v", slow.level, zapcore.WarnLevel)
	}
	if slow.msg != msgSlow {
		t.Errorf("消息 = %q, want %q", slow.msg, msgSlow)
	}
	for _, want := range append(append([]string{}, visitLogKeys...), "http.slow") {
		assertInKeys(t, slow.keys, want)
	}
}

// 用例 3：快请求（不 sleep，阈值 10ms）——桩记录 Info、无 http.slow
func TestMiddlewareFastRequestInfos(t *testing.T) {
	stub := newStub()
	cfg := &MiddlewareConfig{Logger: stub, SlowThreshold: 10 * time.Millisecond}
	_, _, handlerErrs := runRequest(t, cfg, 0)

	for _, e := range handlerErrs {
		t.Errorf("handler 断言失败: %s", e)
	}
	entries := stub.snapshot()
	if len(entries) != 1 {
		t.Fatalf("日志条数 = %d, want 1", len(entries))
	}
	fast := entries[0]
	if fast.level != zapcore.InfoLevel {
		t.Errorf("级别 = %v, want %v", fast.level, zapcore.InfoLevel)
	}
	if fast.msg != msgVisitLog {
		t.Errorf("消息 = %q, want %q", fast.msg, msgVisitLog)
	}
	assertNotInKeys(t, fast.keys, "http.slow")
}

// 用例 4：nil 配置——不 panic，访问日志回退全局 logger（无 provider 时为 noop，不产生输出）
func TestMiddlewareNilConfigNoPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("nil 配置 panic: %v", r)
		}
	}()
	rc, stub, handlerErrs := runRequest(t, nil, 0)

	if stub != nil {
		t.Fatalf("nil 配置不应使用桩 logger")
	}
	for _, e := range handlerErrs {
		t.Errorf("handler 断言失败: %s", e)
	}
	if got := rc.Response.StatusCode(); got != consts.StatusOK {
		t.Errorf("响应状态码 = %d, want %d", got, consts.StatusOK)
	}
	if got := rc.Response.Header.Get(headerRequestID); len(got) != 16 {
		t.Errorf("X-Request-ID = %q, want 16 字符 hex", got)
	}
}

// 显式 DisableVisitLog=true——不输出访问日志（request_id 注入仍保留）
func TestMiddlewareVisitLogDisabled(t *testing.T) {
	stub := newStub()
	cfg := &MiddlewareConfig{Logger: stub, DisableVisitLog: true}
	rc, _, _ := runRequest(t, cfg, 0)

	entries := stub.snapshot()
	if len(entries) != 0 {
		t.Errorf("DisableVisitLog=true 时日志条数 = %d, want 0", len(entries))
	}
	if got := rc.Response.Header.Get(headerRequestID); len(got) != 16 {
		t.Errorf("关闭访问日志后 X-Request-ID 仍应注入，got %q", got)
	}
}

// GetRequestID 边界：未设置时返回 ""，nil ctx 不 panic
func TestGetRequestID(t *testing.T) {
	if got := GetRequestID(nil); got != "" {
		t.Errorf("nil RequestContext = %q, want \"\"", got)
	}
	rc := app.NewContext(0)
	if got := GetRequestID(rc); got != "" {
		t.Errorf("未设置 = %q, want \"\"", got)
	}
}

// 配置助手函数：nil/缺省/显式各形态的解析结果（cfgSlowThreshold/cfgEmitVisitLog/cfgBaseLogger）
func TestConfigHelpers(t *testing.T) {
	// cfgSlowThreshold：nil → 0；显式负值原样返回（负值语义 = 不标记慢请求，由比较 threshold>0 决定）
	if got := cfgSlowThreshold(nil); got != 0 {
		t.Errorf("nil SlowThreshold = %v, want 0", got)
	}
	if got := cfgSlowThreshold(&MiddlewareConfig{SlowThreshold: -1 * time.Second}); got != -1*time.Second {
		t.Errorf("negative SlowThreshold = %v, want -1s 原样", got)
	}
	if got := cfgSlowThreshold(&MiddlewareConfig{SlowThreshold: 500 * time.Millisecond}); got != 500*time.Millisecond {
		t.Errorf("SlowThreshold = %v, want 500ms", got)
	}

	// cfgEmitVisitLog：nil 配置 → 输出（缺省）；DisableVisitLog=true → 关闭
	if !cfgEmitVisitLog(nil) {
		t.Error("nil 配置 EmitVisitLog = false, want true（缺省输出）")
	}
	if !cfgEmitVisitLog(&MiddlewareConfig{}) {
		t.Error("零值配置 EmitVisitLog = false, want true（缺省输出）")
	}
	if cfgEmitVisitLog(&MiddlewareConfig{DisableVisitLog: true}) {
		t.Error("DisableVisitLog=true 时 EmitVisitLog = true, want false")
	}

	// cfgBaseLogger：nil 配置 / nil Logger → 全局回退；显式 stub → 用 stub
	stub := newStub()
	if got := cfgBaseLogger(nil); got == nil {
		t.Error("nil 配置 BaseLogger = nil, want 全局回退非 nil")
	}
	if got := cfgBaseLogger(&MiddlewareConfig{Logger: stub}); got != logx.Logger(stub) {
		t.Errorf("显式 stub BaseLogger = %v, want stub", got)
	}
	if got := cfgBaseLogger(&MiddlewareConfig{}); got == nil {
		t.Error("零值配置 BaseLogger = nil, want 全局回退非 nil")
	}
}

// 请求 id 唯一性
func TestNewRequestIDUnique(t *testing.T) {
	seen := make(map[string]struct{}, 256)
	for i := 0; i < 255; i++ {
		id := newRequestID()
		if len(id) != 16 {
			t.Fatalf("请求 id = %q, want 16 字符 hex", id)
		}
		if !isHex(id) {
			t.Fatalf("请求 id = %q, 非 hex", id)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("重复请求 id: %s", id)
		}
		seen[id] = struct{}{}
	}
}

func isHex(s string) bool {
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return len(s) > 0
}
