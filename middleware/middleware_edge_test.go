// logx/middleware/middleware_edge_test.go
// newRequestID 契约：16 字符 hex 定长、多值互异；
// GetRequestID nil 接收者安全（middleware.go:85 判空分支）
package middleware

import (
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
)

// TestNewRequestIDFormat 16 字符 hex（0-f），同批次互异（128bit 随机熵）
func TestNewRequestIDFormat(t *testing.T) {
	const n = 64
	seen := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		id := newRequestID()
		if len(id) != 16 {
			t.Fatalf("request id 长度 = %d (%q), want 16 hex 字符", len(id), id)
		}
		for _, c := range id {
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
				t.Fatalf("request id 含非 hex 字符: %q", id)
			}
		}
		if seen[id] {
			t.Fatalf("request id 重复（随机源缺陷）: %s", id)
		}
		seen[id] = true
	}
}

// TestGetRequestIDNil c 为 nil 时返回空串（判空分支，不 panic）
func TestGetRequestIDNil(t *testing.T) {
	if id := GetRequestID(nil); id != "" {
		t.Errorf("GetRequestID(nil) = %q, want 空串", id)
	}
}

// TestGetRequestIDNotSet 已构造 ctx 但未写入 requestIDKey → 空串
func TestGetRequestIDNotSet(t *testing.T) {
	rc := app.NewContext(0)
	if id := GetRequestID(rc); id != "" {
		t.Errorf("未写入 requestIDKey 时 = %q, want 空串", id)
	}
}
