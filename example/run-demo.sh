#!/usr/bin/env bash
# logx/example/run-demo.sh
# 一键演示：起调用链环境（jaeger + kafka）→ 跑 7 个用例 → 验证
# 用法：bash example/run-demo.sh   （从 logx 根目录或 example/ 均可）
set -euo pipefail

cd "$(dirname "$0")"   # example/

# 本机 GOROOT 环境变量损坏时（报 cannot find GOROOT）的兜底前缀
# 正常环境无需前缀：检测后自动跳过
if GOTOOLCHAIN=local go version >/dev/null 2>&1; then
  GO() { go "$@"; }
else
  GO() { GOROOT=/usr/local/Cellar/go/1.27.1/libexec GOTOOLCHAIN=local /usr/local/bin/go "$@"; }
fi

# 清理策略：默认 down（保留 volumes，不破坏共享的 kafka 数据与兄弟 SDK 演示数据）；
# LOGX_DEMO_CLEANUP=1 时 down -v 彻底清理
CLEANUP=${LOGX_DEMO_CLEANUP:-0}
if [ "$CLEANUP" = "1" ]; then
  trap 'echo "==> 清理环境 (down -v)..."; docker compose down -v >/dev/null 2>&1 || true' EXIT
else
  trap 'echo "==> 清理环境 (down，保留 volumes；LOGX_DEMO_CLEANUP=1 彻底清理)"; docker compose down >/dev/null 2>&1 || true' EXIT
fi

echo "==> 1/4 启动调用链环境（jaeger + kafka）"
docker compose up -d

wait_for() { # $1=URL $2=名称
  echo "    等待 $2 ..."
  for _ in $(seq 1 60); do
    if curl -fs "$1" >/dev/null 2>&1; then echo "    $2 ready"; return; fi
    sleep 1
  done
  echo "    [warn] $2 60s 内未就绪，继续（用例可能需手动重试）"
}
wait_for http://localhost:26686 jaeger-ui
# kafka 就绪：compose healthcheck 完成即可，这里用端口探测兜底
for _ in $(seq 1 60); do
  nc -z localhost 39092 2>/dev/null && break || sleep 1
done

echo "==> 2/4 零依赖用例（01/02/05）"
GO run ./01-basic
GO run ./02-file-rotate
ls logs/  # 轮转证据：app.log + app.log.N.gz
GO run ./05-chained-builder

echo "==> 3/4 调用链用例（03 otel / 04 mq-kafka / 07 otlp）"
GO run ./03-otel-tracing
GO run ./04-mq-kafka
GO run ./07-otlp-exporter

echo "==> 4/4 Hertz 中间件用例（06）"
GO build -o /tmp/logx-demo-http ./06-http-middleware
/tmp/logx-demo-http >/tmp/logx-demo-http.out 2>&1 &
DEMO_PID=$!
sleep 2
curl -s http://127.0.0.1:8080/fast >/dev/null
# X-Request-ID 头检查用 /fast（HEAD 可用）；/slow 只注册了 GET，
# 必须真实 GET 触发 800ms 慢路径，否则访问日志为 404 且不会升级为 WARN
curl -sI http://127.0.0.1:8080/fast | grep -i 'x-request-id' && echo "    [ok] X-Request-ID 响应头存在"
curl -s http://127.0.0.1:8080/slow >/dev/null   # 真实 GET /slow（800ms > 500ms 阈值）
sleep 3   # 留时间给 /slow 的 WARN 访问日志与 Jaeger 批量导出（5s 批超时前需 ForceFlush，此处靠常驻进程持续 flush）
kill "$DEMO_PID" 2>/dev/null || true
wait "$DEMO_PID" 2>/dev/null || true
grep -q 'WARN' /tmp/logx-demo-http.out && grep 'slow http request' /tmp/logx-demo-http.out | head -1 && echo "    [ok] /slow 触发 WARN 访问日志"

echo ""
echo "================ 验证清单 ================"
echo " [1] 轮转文件：ls logs/（应有 app-<时间戳>.log.gz 压缩轮转文件）"
echo " [2] Jaeger 服务落库确认："
svcs=$(curl -s --max-time 10 http://localhost:26686/api/services || echo '')
echo "     $svcs" | grep -o 'logx-demo-otel' >/dev/null && echo "     [ok] logx-demo-otel 已落库"
echo "     $svcs" | grep -q 'logx-demo-http' && echo "     [ok] logx-demo-http 已落库"
echo "     $svcs" | grep -q 'logx-demo-mq' && echo "     [ok] logx-demo-mq 已落库" || echo "     [info] logx-demo-mq 未归组（旧消息/OTLP partial-success 环境，以 'span flushed to kafka' 输出为准）"
echo "     $svcs" | grep -q 'logx-demo-otlp' && echo "     [ok] logx-demo-otlp 已落库（OTLP HTTP 通路）" || echo "     [warn] logx-demo-otlp 未落库（OTLP HTTP 通路，可手动重试 07 用例）"
echo " [3] UI 人工确认：http://localhost:26686"
echo " [4] 04 用例终端 'span flushed to kafka' 即生产者直推成功"
echo "==========================================="
