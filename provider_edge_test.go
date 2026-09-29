// logx/provider_edge_test.go
// provider 注册/新实例边界：New 无 provider 错误分支、ReadConfig 未知 remote provider
// 错误传播（命中 configx「unsupported remote config provider」确定性分支，不触网）
package logx

import (
	"strings"
	"testing"

	"github.com/gospacex/configx"
)

// TestNewNoProvider New 的 no-provider 错误分支（构造期 fail fast + 导入提示）
// 临时清空 providers 并指向未注册名，defer 恢复全局状态
func TestNewNoProvider(t *testing.T) {
	providersMu.Lock()
	oldProviders := providers
	oldDefault := defaultProvider
	providers = map[string]Provider{}
	defaultProvider = "unregistered-name"
	providersMu.Unlock()
	defer func() {
		providersMu.Lock()
		providers = oldProviders
		defaultProvider = oldDefault
		providersMu.Unlock()
	}()

	_, err := New(DefaultConfig())
	if err == nil {
		t.Fatal("New(未注册 provider) 未报错, want fail fast")
	}
	if !strings.Contains(err.Error(), "no logger provider registered") {
		t.Errorf("错误 = %q, want 含 no-provider 提示（引导 blank import）", err.Error())
	}
}

// TestReadConfigUnsupportedRemoteProvider ReadConfig 错误传播：
// 未知 remote provider 命中 configx 的 unsupported 分支（确定性、离线安全），
// 错误被 ReadConfig 原样上抛（不静默吞错）
func TestReadConfigUnsupportedRemoteProvider(t *testing.T) {
	err := ReadConfig(configx.ConfigOptions{
		Remote: configx.RemoteConfig{Enabled: true, Provider: "bogus-provider"},
	})
	if err == nil {
		t.Fatal("ReadConfig(未知 remote provider) 未报错")
	}
}
