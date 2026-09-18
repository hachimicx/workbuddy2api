// proxyenv.go 把生效的代理配置下发到 python 脚本子进程。
//
// 为什么需要：开学季 / 夜猫子任务由 internal/scheduler 拉起 scripts/*.py 子进程，
// 脚本内用 urllib 自己出网（urlopen 走 http.client，**不读** Go 侧的 Transport 配置）。
// 于是「网关走了代理、脚本还在直连」——上游不可直连的网络里脚本必失败，且形成一条
// 绕过代理的直连路径（与 upstream.proxy 的承诺相悖）。
//
// 生效配置的优先级（与网关其余部分一致：显式 env > config）：
//  1. WB2A_PROXY（显式环境变量，容器/命令行临时覆盖）
//  2. Config.ProxyURL（config.json 的 upstream.proxy，经 main 注入）
//
// 两者都空 → environ 原样返回，**零行为变更**：既不覆盖用户已有的 HTTP_PROXY，
// 也不凭空造出代理。
package scheduler

import (
	"os"
	"strings"
)

// 下发用的标准环境变量名（python urllib / requests 均识别这一组）。
const (
	envHTTPProxy  = "HTTP_PROXY"
	envHTTPSProxy = "HTTPS_PROXY"
	envNoProxy    = "NO_PROXY"
)

// proxyEnv 在生效代理非空时把代理配置追加到子进程环境；否则原样返回。
//
// 语义与 Go 侧 upstream.NewProxyFunc 对齐：
//   - 生效代理为空 → 不下发（子进程继承既有环境，行为与改动前逐字一致）
//   - 绕过清单非空 → 下发 NO_PROXY（urllib 的 no_proxy 语义同为逗号分隔、
//     支持域名/CIDR/"*"）
//
// 已存在的同名键会被追加的副本覆盖（exec 取最后一个），故无需先过滤原环境。
func proxyEnv(environ []string) []string {
	proxy, noProxy := effectiveProxy()
	if proxy == "" {
		return environ
	}
	out := make([]string, 0, len(environ)+3)
	out = append(out, environ...)
	out = append(out, envHTTPProxy+"="+proxy, envHTTPSProxy+"="+proxy)
	if noProxy != "" {
		out = append(out, envNoProxy+"="+noProxy)
	}
	return out
}

// effectiveProxy 解析生效的代理 URL 与绕过清单（WB2A_* 环境变量优先，其次 config）。
// 包级函数便于测试直接断言优先级，无需构造 Scheduler。
func effectiveProxy() (proxy, noProxy string) {
	proxy = strings.TrimSpace(os.Getenv("WB2A_PROXY"))
	noProxy = strings.TrimSpace(os.Getenv("WB2A_NO_PROXY"))
	if proxy == "" {
		proxy = strings.TrimSpace(scriptProxyURL)
	}
	if noProxy == "" {
		noProxy = strings.TrimSpace(scriptNoProxy)
	}
	return proxy, noProxy
}

// scriptProxyURL / scriptNoProxy 由 cmd/server 经 SetScriptProxy 注入的 config 值。
//
// 进程启动期写一次、之后只读（脚本任务在启动后才会触发），无需加锁；
// 默认空 = 不下发（未接线的调用方/测试行为与引入前一致）。
var (
	scriptProxyURL string
	scriptNoProxy  string
)

// SetScriptProxy 注入 config 的 upstream.proxy / upstream.no_proxy，供脚本子进程下发。
// 只在进程启动期调用一次（cmd/server 构造 Scheduler 前）。
func SetScriptProxy(proxyURL, noProxy string) {
	scriptProxyURL = proxyURL
	scriptNoProxy = noProxy
}
