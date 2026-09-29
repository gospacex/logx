// logx/builder_ext_test.go
// 外部测试包：blank import logx/zap 触发其 init
// （注册 provider 与 ...any 归一化器），使同测试二进制内的
// package logx 内部测试（builder_test.go）经 Builder 末端路径
// 验证真实的键值交替归一化行为。
package logx_test

import _ "github.com/gospacex/logx/zap"
