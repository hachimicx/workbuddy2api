package main

import (
	"net/http"
	"testing"

	"workbuddy2api/internal/config"
)

// TestNewUpstreamGlobalEnabled registry：cmd/activity 与 cmd/signin、cmd/trial、cmd/credit
// 同风格显式接线 GlobalEnabled=true。activity 虽是 CN 任务中心的上报工具（global 账号已被
// scheduler 的 IsGlobal 门控跳过），但工具自身构造 global 账号请求时不得误打 CN base——
// 开关显式开启把该路径收敛为「不正确但不会跨域误打」（与 signin/trial 同逃生门式接线）。
func TestNewUpstreamGlobalEnabled(t *testing.T) {
	up := newUpstream(&cfgFile{Schedule: config.DefaultSchedule()})
	if !up.GlobalEnabled {
		t.Error("newUpstream should set GlobalEnabled=true (explicit wiring, uniform with signin/trial/credit)")
	}
}

// TestNewUpstreamAppliesTimeout 配置的 upstream.timeout_seconds 仍生效（接线抽出不破坏原行为）。
func TestNewUpstreamAppliesTimeout(t *testing.T) {
	c := &cfgFile{Schedule: config.DefaultSchedule()}
	c.Upstream.TimeoutSeconds = 7
	up := newUpstream(c)
	if up.HTTP.Timeout == 0 {
		t.Fatal("HTTP.Timeout should be set when timeout_seconds > 0")
	}
	if up.HTTP.Timeout.String() != "7s" {
		t.Errorf("HTTP.Timeout=%v want 7s", up.HTTP.Timeout)
	}
}

// TestNewUpstreamProxyWiring 出站代理接线：config upstream.proxy 优先，缺省回落
// WB2A_PROXY 环境变量（一次性工具不强制写 config）；非法配置告警后直连不中断上报。
func TestNewUpstreamProxyWiring(t *testing.T) {
	// 1. config 显式配置生效。
	c := &cfgFile{Schedule: config.DefaultSchedule()}
	c.Upstream.Proxy = "127.0.0.1:7890"
	c.Upstream.NoProxy = "localhost"
	tr, ok := newUpstream(c).HTTP.Transport.(*http.Transport)
	if !ok || tr.Proxy == nil {
		t.Fatal("config upstream.proxy must be wired to Transport.Proxy")
	}
	req, _ := http.NewRequest("GET", "https://copilot.tencent.com/x", nil)
	if u, _ := tr.Proxy(req); u == nil || u.Host != "127.0.0.1:7890" {
		t.Errorf("proxy=%v want 127.0.0.1:7890", u)
	}

	// 2. config 缺省时回落 WB2A_PROXY 环境变量。
	t.Setenv("WB2A_PROXY", "socks5h://127.0.0.1:1080")
	tr2, _ := newUpstream(&cfgFile{Schedule: config.DefaultSchedule()}).HTTP.Transport.(*http.Transport)
	if tr2.Proxy == nil {
		t.Fatal("WB2A_PROXY env must be honored when config proxy is empty")
	}

	// 3. 非法配置：告警后直连（不 panic、不中断）。
	t.Setenv("WB2A_PROXY", "ftp://127.0.0.1:21")
	tr3, _ := newUpstream(&cfgFile{Schedule: config.DefaultSchedule()}).HTTP.Transport.(*http.Transport)
	if tr3.Proxy != nil {
		t.Error("invalid proxy must fall back to direct connection")
	}

	// 4. 都未配置：直连。
	t.Setenv("WB2A_PROXY", "")
	tr4, _ := newUpstream(&cfgFile{Schedule: config.DefaultSchedule()}).HTTP.Transport.(*http.Transport)
	if tr4.Proxy != nil {
		t.Error("no proxy configured must stay direct")
	}
}
