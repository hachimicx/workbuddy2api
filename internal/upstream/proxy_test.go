package upstream

import (
	"context"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestNewProxyFuncEmptyIsDirect 空 proxy = 直连：返回 nil 钩子（调用方保持 Transport.Proxy
// 为 nil，即不读 HTTP_PROXY/HTTPS_PROXY 环境变量的既有语义）。
func TestNewProxyFuncEmptyIsDirect(t *testing.T) {
	fn, err := NewProxyFunc("", "")
	if err != nil {
		t.Fatalf("empty proxy must not error: %v", err)
	}
	if fn != nil {
		t.Error("empty proxy must yield nil hook (direct, env vars ignored)")
	}
	// 只有空白字符同样视为未配置。
	if fn, err := NewProxyFunc("   ", "  "); err != nil || fn != nil {
		t.Errorf("blank proxy: fn!=nil=%v err=%v want nil,nil", fn != nil, err)
	}
}

// TestNewProxyFuncIgnoresProxyEnvVars 默认直连不读 HTTP_PROXY/HTTPS_PROXY 环境变量：
// 容器里残留一个环境变量不得改变出站路径（与 net/http 零值 Transport 语义一致）。
func TestNewProxyFuncIgnoresProxyEnvVars(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	up := New()
	tr, _ := up.HTTP.Transport.(*http.Transport)
	if tr.Proxy != nil {
		t.Error("New() must leave Transport.Proxy nil: HTTP_PROXY env vars must not steer outbound traffic")
	}
}

// TestParseProxyURL 归一化：省略 scheme / 省略端口 / 大小写 / 用户凭据。
func TestParseProxyURL(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "http://127.0.0.1:7890", want: "http://127.0.0.1:7890"},
		{in: "127.0.0.1:7890", want: "http://127.0.0.1:7890"},        // 省略 scheme
		{in: "http://proxy.local", want: "http://proxy.local:80"},    // 省略端口
		{in: "https://proxy.local", want: "https://proxy.local:443"}, // https 默认端口
		{in: "socks5://127.0.0.1", want: "socks5://127.0.0.1:1080"},  // socks5 默认端口
		{in: "socks5h://127.0.0.1:1080", want: "socks5h://127.0.0.1:1080"},
		{in: "SOCKS5://127.0.0.1:1080", want: "socks5://127.0.0.1:1080"}, // scheme 归一为小写
		{in: "http://u:p%40ss@127.0.0.1:7890", want: "http://u:p%40ss@127.0.0.1:7890"},
		{in: "ftp://127.0.0.1:21", wantErr: true}, // 不支持的 scheme
		{in: "http://", wantErr: true},            // 缺主机
		{in: "://127.0.0.1", wantErr: true},       // 非法 URL
	}
	for _, c := range cases {
		u, err := parseProxyURL(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseProxyURL(%q) want error, got %v", c.in, u)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseProxyURL(%q): %v", c.in, err)
			continue
		}
		if u.String() != c.want {
			t.Errorf("parseProxyURL(%q)=%q want %q", c.in, u.String(), c.want)
		}
	}
}

// TestProxyBypassMatch no_proxy 匹配语义逐条对齐 net/http 的 NO_PROXY。
func TestProxyBypassMatch(t *testing.T) {
	b, err := newProxyBypass("localhost, .internal.example, *.corp.example, api.foo.com, 10.0.0.0/8, 192.168.1.7, [::1], example.org:8443")
	if err != nil {
		t.Fatalf("newProxyBypass: %v", err)
	}
	cases := []struct {
		host, port string
		want       bool
	}{
		{"localhost", "80", true},             // 域名自身
		{"localhost", "443", true},            // 端口无关
		{"app.internal.example", "443", true}, // 前导点：只匹配子域
		{"internal.example", "443", false},    // 前导点不匹配自身
		{"a.corp.example", "443", true},       // *.corp.example 同 .corp.example
		{"corp.example", "443", false},
		{"api.foo.com", "443", true}, // 无前导点：自身 + 子域
		{"x.api.foo.com", "443", true},
		{"other.foo.com", "443", false},
		{"10.1.2.3", "443", true}, // CIDR
		{"11.1.2.3", "443", false},
		{"192.168.1.7", "443", true}, // 单 IP
		{"192.168.1.8", "443", false},
		{"::1", "443", true},                  // IPv6 字面量
		{"example.org", "8443", true},         // host:port 端口一致
		{"example.org", "443", false},         // 端口不一致 → 不绕过
		{"copilot.tencent.com", "443", false}, // 未命中 → 走代理
		{"localhost", "80", true},
	}
	for _, c := range cases {
		if got := b.match(c.host, c.port); got != c.want {
			t.Errorf("match(%q,%q)=%v want %v", c.host, c.port, got, c.want)
		}
	}
}

// TestProxyBypassWildcardAll "*" 全部绕过。
func TestProxyBypassWildcardAll(t *testing.T) {
	b, err := newProxyBypass("*")
	if err != nil {
		t.Fatal(err)
	}
	if !b.match("copilot.tencent.com", "443") {
		t.Error(`"*" must bypass every host`)
	}
	// 空清单：全部走代理。
	empty, err := newProxyBypass("")
	if err != nil {
		t.Fatal(err)
	}
	if empty.match("copilot.tencent.com", "443") {
		t.Error("empty no_proxy must not bypass anything")
	}
	// nil 接收者（未配置代理）恒不命中。
	var nilBypass *proxyBypass
	if nilBypass.match("a", "80") {
		t.Error("nil bypass must not match")
	}
}

// TestNewProxyBypassInvalid 非法条目 fail fast（静默忽略会让人误以为「已绕过」）。
// 注：", ,"（全是空条目）与 "10.0.0.1/8"（ParseCIDR 接受并归一到网段）与 net/http 的
// NO_PROXY 同口径，均**不**报错。
func TestNewProxyBypassInvalid(t *testing.T) {
	for _, in := range []string{"10.0.0.0/33", "example.com:abc"} {
		if _, err := newProxyBypass(in); err == nil {
			t.Errorf("newProxyBypass(%q) want error", in)
		}
	}
	for _, in := range []string{"", ", ,", "10.0.0.1/8"} {
		if _, err := newProxyBypass(in); err != nil {
			t.Errorf("newProxyBypass(%q) unexpected error: %v", in, err)
		}
	}
}

// TestProxyFuncRoutesThroughHTTPProxy 端到端：SetProxy 挂上钩子后请求真的走代理。
// 目标用 http:// 域（代理收到绝对 URI 请求），域名不可解析——若没走代理必然 DNS 失败。
func TestProxyFuncRoutesThroughHTTPProxy(t *testing.T) {
	var hits int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		if !r.URL.IsAbs() {
			t.Errorf("proxy got non-absolute URL %q", r.URL)
		}
		io.WriteString(w, "proxied")
	}))
	defer proxy.Close()

	up := New()
	if err := up.SetProxy(proxy.URL, ""); err != nil {
		t.Fatalf("SetProxy: %v", err)
	}
	// HTTP 与 ChatHTTP 共享同一 Transport：钩子对两条出站路径同时生效。
	if tr, _ := up.ChatHTTP.Transport.(*http.Transport); tr.Proxy == nil {
		t.Fatal("ChatHTTP transport must share the proxy hook")
	}
	resp, err := up.HTTP.Get("http://chat.example.invalid/v1/chat")
	if err != nil {
		t.Fatalf("request through proxy: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "proxied" || atomic.LoadInt64(&hits) != 1 {
		t.Fatalf("body=%q hits=%d want proxied/1", body, atomic.LoadInt64(&hits))
	}
}

// TestProxyFuncNoProxyBypasses 命中 no_proxy 时直连（代理不被触碰）。
func TestProxyFuncNoProxyBypasses(t *testing.T) {
	var hits int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		io.WriteString(w, "proxied")
	}))
	defer proxy.Close()

	up := New()
	if err := up.SetProxy(proxy.URL, "chat.example.invalid"); err != nil {
		t.Fatalf("SetProxy: %v", err)
	}
	// 绕过 → 直连不可解析域名 → 请求失败，且代理零命中。
	if resp, err := up.HTTP.Get("http://chat.example.invalid/v1/chat"); err == nil {
		resp.Body.Close()
		t.Fatal("bypassed host must be dialed directly (DNS failure expected)")
	}
	if n := atomic.LoadInt64(&hits); n != 0 {
		t.Errorf("proxy hits=%d want 0 (no_proxy must bypass)", n)
	}
}

// TestSetProxySocks5Hook socks5 钩子原样返回（域名交代理远端解析，无本地 DNS）。
func TestSetProxySocks5Hook(t *testing.T) {
	up := New()
	if err := up.SetProxy("socks5h://127.0.0.1:1080", ""); err != nil {
		t.Fatalf("SetProxy: %v", err)
	}
	tr, _ := up.HTTP.Transport.(*http.Transport)
	req := httptest.NewRequest("POST", "https://copilot.tencent.com/v2/chat/completions", nil)
	u, err := tr.Proxy(req)
	if err != nil {
		t.Fatalf("proxy hook: %v", err)
	}
	if u == nil || u.Scheme != "socks5h" || u.Host != "127.0.0.1:1080" {
		t.Fatalf("proxy=%v want socks5h://127.0.0.1:1080", u)
	}
}

// TestSetProxyInvalid 非法配置不落半可用钩子。
func TestSetProxyInvalid(t *testing.T) {
	up := New()
	if err := up.SetProxy("ftp://127.0.0.1:21", ""); err == nil {
		t.Error("unsupported scheme must error")
	}
	if err := up.SetProxy("http://127.0.0.1:7890", "example.com:abc"); err == nil {
		t.Error("invalid no_proxy port must error")
	}
	// 失败后 Transport.Proxy 保持未设置（不静默回落成半配置状态）。
	if tr, _ := up.HTTP.Transport.(*http.Transport); tr.Proxy != nil {
		t.Error("failed SetProxy must leave Transport.Proxy nil")
	}
}

// TestSetProxyRejectsNonTransport 测试注入的自定义 RoundTripper 不支持代理：明确报错，
// 而不是「配了代理但没生效」。
func TestSetProxyRejectsNonTransport(t *testing.T) {
	up := &Client{HTTP: &http.Client{Transport: rtFunc(func(*http.Request) (*http.Response, error) {
		return nil, nil
	})}}
	if err := up.SetProxy("http://127.0.0.1:7890", ""); err == nil {
		t.Error("non-*http.Transport must be rejected")
	}
	if err := (&Client{}).SetProxy("http://127.0.0.1:7890", ""); err == nil {
		t.Error("nil HTTP client must be rejected")
	}
}

// TestSetProxyFromEnv 一次性工具的 env 接线（WB2A_PROXY / WB2A_NO_PROXY）。
func TestSetProxyFromEnv(t *testing.T) {
	t.Setenv("WB2A_PROXY", "127.0.0.1:7890")
	t.Setenv("WB2A_NO_PROXY", "localhost")
	up := New()
	if err := up.SetProxyFromEnv(); err != nil {
		t.Fatalf("SetProxyFromEnv: %v", err)
	}
	tr, _ := up.HTTP.Transport.(*http.Transport)
	if tr.Proxy == nil {
		t.Fatal("env proxy must be wired")
	}
	if u, _ := tr.Proxy(httptest.NewRequest("GET", "https://copilot.tencent.com/x", nil)); u == nil || u.Host != "127.0.0.1:7890" {
		t.Errorf("proxy=%v want 127.0.0.1:7890", u)
	}
	if u, _ := tr.Proxy(httptest.NewRequest("GET", "https://localhost/x", nil)); u != nil {
		t.Errorf("localhost must bypass, got %v", u)
	}
	// 无 env：直连，不报错。
	t.Setenv("WB2A_PROXY", "")
	t.Setenv("WB2A_NO_PROXY", "")
	up2 := New()
	if err := up2.SetProxyFromEnv(); err != nil {
		t.Fatalf("empty env must not error: %v", err)
	}
	if tr2, _ := up2.HTTP.Transport.(*http.Transport); tr2.Proxy != nil {
		t.Error("empty env must leave Proxy nil")
	}
}

// TestMaskProxyURLCredentials 日志脱敏：密码永不落盘，用户名/主机保留。
func TestMaskProxyURLCredentials(t *testing.T) {
	cases := map[string]string{
		"":                                 "",
		"socks5://u:secret@127.0.0.1:1080": "socks5://u:xxxxx@127.0.0.1:1080",
		"http://127.0.0.1:7890":            "http://127.0.0.1:7890",
		"http://user@127.0.0.1:7890":       "http://user@127.0.0.1:7890",
	}
	for in, want := range cases {
		if got := MaskProxyURL(in); got != want {
			t.Errorf("MaskProxyURL(%q)=%q want %q", in, got, want)
		}
	}
	if got := MaskProxyURL("http://%zz"); got != "(invalid)" {
		t.Errorf("invalid URL masking=%q want (invalid)", got)
	}
	if strings.Contains(MaskProxyURL("http://u:secret@h:1"), "secret") {
		t.Error("password must never appear in masked URL")
	}
}

// TestPortOfURL 端口归一（no_proxy 的 host:port 条目比对口径）。
func TestPortOfURL(t *testing.T) {
	cases := []struct{ raw, want string }{
		{"https://a.com/x", "443"},
		{"http://a.com/x", "80"},
		{"https://a.com:8443/x", "8443"},
		{"socks5://a.com/x", ""},
	}
	for _, c := range cases {
		u, err := url.Parse(c.raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := portOfURL(u); got != c.want {
			t.Errorf("portOfURL(%q)=%q want %q", c.raw, got, c.want)
		}
	}
	if got := portOfURL(nil); got != "" {
		t.Errorf("portOfURL(nil)=%q want empty", got)
	}
}

// TestNewProxyDialerEmptyIsNil 空 proxy = 直连（nil 拨号器，调用方保持默认 Dialer）。
func TestNewProxyDialerEmptyIsNil(t *testing.T) {
	for _, in := range []string{"", "   "} {
		d, err := NewProxyDialer(in)
		if err != nil || d != nil {
			t.Errorf("NewProxyDialer(%q)=(%v,%v) want nil,nil", in, d != nil, err)
		}
	}
}

// TestNewProxyDialerInvalid 与 NewProxyFunc 共用同一份校验（scheme / 主机）。
func TestNewProxyDialerInvalid(t *testing.T) {
	for _, in := range []string{"ftp://127.0.0.1:21", "http://"} {
		if _, err := NewProxyDialer(in); err == nil {
			t.Errorf("NewProxyDialer(%q) want error", in)
		}
	}
}

// TestNewProxyDialerSocks5EndToEnd socks5 拨号器真的把连接交给 SOCKS 代理：
// 用一个最小 SOCKS5 服务端（无认证，记录目标地址后回 CONNECT 成功）验证
// ——「域名以 FQDN 交给代理远端解析」是本功能的硬要求（本地不发 DNS）。
func TestNewProxyDialerSocks5EndToEnd(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	targets := make(chan string, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		// 握手：客户端发 VER=5 NMETHODS N METHODS
		hdr := make([]byte, 2)
		if _, err := io.ReadFull(c, hdr); err != nil {
			return
		}
		methods := make([]byte, hdr[1])
		if _, err := io.ReadFull(c, methods); err != nil {
			return
		}
		c.Write([]byte{5, 0}) // 无需认证
		// 请求：VER CMD RSV ATYP ADDR PORT
		req := make([]byte, 4)
		if _, err := io.ReadFull(c, req); err != nil {
			return
		}
		var host string
		switch req[3] {
		case 3: // FQDN：本测试断言的关键分支
			l := make([]byte, 1)
			io.ReadFull(c, l)
			name := make([]byte, l[0])
			io.ReadFull(c, name)
			host = string(name)
		case 1:
			ip := make([]byte, 4)
			io.ReadFull(c, ip)
			host = net.IP(ip).String()
		default:
			host = "unexpected-atyp"
		}
		port := make([]byte, 2)
		io.ReadFull(c, port)
		targets <- net.JoinHostPort(host, strconv.Itoa(int(port[0])<<8|int(port[1])))
		c.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0}) // 成功（BND.ADDR 占位）
		time.Sleep(50 * time.Millisecond)
	}()

	d, err := NewProxyDialer("socks5h://" + ln.Addr().String())
	if err != nil {
		t.Fatalf("NewProxyDialer: %v", err)
	}
	conn, err := d(context.Background(), "tcp", "copilot.tencent.com:443")
	if err != nil {
		t.Fatalf("dial through socks5: %v", err)
	}
	conn.Close()

	select {
	case got := <-targets:
		if got != "copilot.tencent.com:443" {
			t.Errorf("socks target=%q want copilot.tencent.com:443 (FQDN, remote-resolved)", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("socks server never received a request")
	}
}

// TestNewProxyDialerHTTPConnectEndToEnd http 代理走 CONNECT 隧道（Redis 这类裸 TCP
// 目标不能靠 http.Transport 的 Proxy 钩子——它只对 http/https 目标建隧道）。
func TestNewProxyDialerHTTPConnectEndToEnd(t *testing.T) {
	targets := make(chan string, 1)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targets <- r.Host
		if r.Method != http.MethodConnect {
			t.Errorf("method=%s want CONNECT", r.Method)
		}
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("hijack unsupported")
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			return
		}
		conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
		conn.Write([]byte("tunnel-payload"))
		conn.Close()
	}))
	defer proxy.Close()

	d, err := NewProxyDialer(proxy.URL)
	if err != nil {
		t.Fatalf("NewProxyDialer: %v", err)
	}
	conn, err := d(context.Background(), "tcp", "upstash.example:6379")
	if err != nil {
		t.Fatalf("dial through CONNECT: %v", err)
	}
	defer conn.Close()
	buf := make([]byte, len("tunnel-payload"))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read tunneled bytes: %v", err)
	}
	if string(buf) != "tunnel-payload" {
		t.Errorf("tunnel payload=%q", buf)
	}
	select {
	case got := <-targets:
		if got != "upstash.example:6379" {
			t.Errorf("CONNECT target=%q want upstash.example:6379", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("proxy never received CONNECT")
	}
}

// TestNewProxyDialerConnectRejected 代理拒绝 CONNECT 时明确报错，不把错误页当隧道。
func TestNewProxyDialerConnectRejected(t *testing.T) {
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, "denied")
	}))
	defer proxy.Close()

	d, err := NewProxyDialer(proxy.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d(context.Background(), "tcp", "upstash.example:6379"); err == nil {
		t.Fatal("rejected CONNECT must return an error")
	}
}

// TestNewProxyDialerRejectsNonTCP CONNECT 隧道只支持 tcp。
func TestNewProxyDialerRejectsNonTCP(t *testing.T) {
	d, err := NewProxyDialer("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d(context.Background(), "udp", "x:1"); err == nil {
		t.Error("udp must be rejected for CONNECT tunnels")
	}
}

// TestNewProxyDialerHTTPProxyAuth 代理凭据走 Proxy-Authorization（Basic）。
func TestNewProxyDialerHTTPProxyAuth(t *testing.T) {
	got := make(chan string, 1)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Get("Proxy-Authorization")
		hj, _ := w.(http.Hijacker)
		conn, _, err := hj.Hijack()
		if err != nil {
			return
		}
		conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
		conn.Close()
	}))
	defer proxy.Close()

	host := strings.TrimPrefix(proxy.URL, "http://")
	d, err := NewProxyDialer("http://user:s3cret@" + host)
	if err != nil {
		t.Fatalf("NewProxyDialer: %v", err)
	}
	conn, err := d(context.Background(), "tcp", "upstash.example:6379")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.Close()
	select {
	case auth := <-got:
		want := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:s3cret"))
		if auth != want {
			t.Errorf("Proxy-Authorization=%q want %q", auth, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("proxy never received the CONNECT")
	}
}

// TestProxyConnectUsesRemoteResolution CONNECT 请求行里必须是**原始主机名**（不经本地
// DNS）：这是「域名交代理远端解析」的可观测契约，也是 socks5 FQDN 分支的 HTTP 侧对应物。
func TestProxyConnectUsesRemoteResolution(t *testing.T) {
	reqLine := make(chan string, 1)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqLine <- r.Method + " " + r.Host
		hj, _ := w.(http.Hijacker)
		conn, _, err := hj.Hijack()
		if err != nil {
			return
		}
		conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
		conn.Close()
	}))
	defer proxy.Close()

	d, err := NewProxyDialer(proxy.URL)
	if err != nil {
		t.Fatal(err)
	}
	// 不可解析域名：若本地解析过就一定失败。
	conn, err := d(context.Background(), "tcp", "definitely-not-resolvable.invalid:6379")
	if err != nil {
		t.Fatalf("dial must succeed without local DNS: %v", err)
	}
	conn.Close()
	select {
	case line := <-reqLine:
		if line != "CONNECT definitely-not-resolvable.invalid:6379" {
			t.Errorf("request line=%q want CONNECT with raw hostname", line)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no CONNECT received")
	}
}
