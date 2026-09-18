// proxy.go 出站代理（upstream.proxy / upstream.no_proxy）。
//
// 为什么需要：上游（copilot.tencent.com / www.workbuddy.ai）在部分网络环境下不可直连，
// 部署侧需要一个「把全部出站请求交给本地代理（Clash / mihomo / v2ray / 公司网关）」的开关。
//
// 设计纪律（与本仓库既有约定一致）：
//   - 默认直连：proxy 为空时 Transport.Proxy 保持 nil——**不**读 HTTP_PROXY/HTTPS_PROXY
//     环境变量（零值 *http.Transport 的既有语义），避免容器里一个残留环境变量把出站
//     路径整体改道。要环境变量行为就显式配置 WB2A_PROXY（见 SetProxyFromEnv）。
//   - 只动 Proxy 钩子：连接层加固参数（禁 h2 / Dial 超时 / TLS 握手超时 /
//     ResponseHeaderTimeout / 连接池容量）一律不碰，见 transport.go。
//   - fail fast：URL / no_proxy 非法在启动期报错，而不是等每个请求都失败。
package upstream

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"golang.org/x/net/proxy"
)

// NewProxyFunc 构造 http.Transport.Proxy 钩子。
//
// proxyURL 语义（与 net/http 的 Transport.Proxy 一致）：
//   - 空串 → 返回 (nil, nil)：**直连**（调用方把 tr.Proxy 置 nil 即还原默认）。
//   - 支持 http / https / socks5 / socks5h 四种 scheme；socks5 与 socks5h 等价
//     （域名交代理远端解析，见 net/http 文档），不走本地 DNS。
//   - 省略 scheme 时按 http 处理（"127.0.0.1:7890" / "user:pass@host:8080" 简写）。
//   - 省略端口时按 scheme 补默认端口（http 80 / https 443 / socks5 1080）。
//
// noProxy 为逗号分隔的绕过清单，匹配语义与 net/http 的 NO_PROXY 逐条对齐：
// 域名（匹配自身与全部子域）、.域名（只匹配子域）、*.域名（同 .域名）、
// IP 字面量、CIDR 网段、host:port（端口也须一致）、"*"（全部绕过）。
//
// 校验失败返回错误（不返回半可用钩子）；成功返回的钩子对同一 URL 是纯函数、并发安全。
func NewProxyFunc(proxyURL, noProxy string) (func(*http.Request) (*url.URL, error), error) {
	raw := strings.TrimSpace(proxyURL)
	if raw == "" {
		return nil, nil
	}
	u, err := parseProxyURL(raw)
	if err != nil {
		return nil, err
	}
	bypass, err := newProxyBypass(noProxy)
	if err != nil {
		return nil, err
	}
	return func(req *http.Request) (*url.URL, error) {
		if bypass.match(req.URL.Hostname(), portOfURL(req.URL)) {
			return nil, nil // 命中绕过清单 → 直连
		}
		return u, nil
	}, nil
}

// SetProxy 按代理 URL 配置该客户端出站 Transport 的 Proxy 钩子。
//
// HTTP 与 ChatHTTP 共享同一个 *http.Transport（见 New），因此一次调用同时覆盖
// 短 RPC 与聊天 SSE 两条出站路径；代理只改「连到哪」，超时/连接池语义不变。
// 约定在启动期（请求开始前）调用，避免运行中改 Transport 字段。
func (c *Client) SetProxy(proxyURL, noProxy string) error {
	tr, err := c.outboundTransport()
	if err != nil {
		return err
	}
	fn, err := NewProxyFunc(proxyURL, noProxy)
	if err != nil {
		return err
	}
	tr.Proxy = fn
	return nil
}

// SetProxyFromEnv 从 WB2A_PROXY / WB2A_NO_PROXY 环境变量配置出站代理。
//
// 供不读 config.json 的一次性工具（cmd/signin、cmd/credit、cmd/trial）复用同一份
// 代理语义；网关（cmd/server）走 config 的 upstream.proxy / upstream.no_proxy
// （applyEnv 已把同名环境变量合并进 config，故两侧口径一致）。
func (c *Client) SetProxyFromEnv() error {
	return c.SetProxy(os.Getenv("WB2A_PROXY"), os.Getenv("WB2A_NO_PROXY"))
}

// MaskProxyURL 打日志用的脱敏代理 URL（user:xxxxx@host:port）。
// 解析失败返回 "(invalid)"，空串返回 ""；密码永不落到日志里（替换成固定占位符
// 而非空串，以便日志能看出「确实配了凭据」）。
func MaskProxyURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "(invalid)"
	}
	if u.User != nil {
		if _, hasPass := u.User.Password(); hasPass {
			u.User = url.UserPassword(u.User.Username(), proxyPasswordMask)
		}
	}
	return u.String()
}

// proxyPasswordMask 日志里替代代理密码的固定占位符（url.UserPassword 会对
// 非法 userinfo 字符做百分号转义，故不能用 "***"——会被编码成 %2A%2A%2A）。
const proxyPasswordMask = "xxxxx"

// DialContextFunc 通用拨号器签名（net.Dialer.DialContext 同形），供非 HTTP 出站
// （Redis / TLS 等）复用同一份代理配置。
//
// 域名一律交代理**远端解析**（http/https 走 CONNECT 的 authority、socks5 走
// FQDN 地址类型），本地不发 DNS——受限网络下既避免解析污染，也避免解析请求本身
// 成为泄漏面。
type DialContextFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// NewProxyDialer 按代理 URL 构造通用拨号器（nil = 直连）。
//
// 与 NewProxyFunc 的区别：后者是 HTTP 语义的 Proxy 钩子（只能挂在 http.Transport
// 上），本函数产出的是「拿到 net.Conn」的拨号器，供 redis.Client.Dialer 这类非 HTTP
// 出站使用。两者共用 parseProxyURL 的归一与校验（scheme / 默认端口）。
func NewProxyDialer(proxyURL string) (DialContextFunc, error) {
	raw := strings.TrimSpace(proxyURL)
	if raw == "" {
		return nil, nil
	}
	u, err := parseProxyURL(raw)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "socks5", "socks5h":
		// socks5 与 socks5h 等价：x/net/proxy 对非 IP 主机发 FQDN，远端解析。
		var auth *proxy.Auth
		if ui := u.User; ui != nil {
			pw, _ := ui.Password()
			auth = &proxy.Auth{User: ui.Username(), Password: pw}
		}
		d, err := proxy.SOCKS5("tcp", u.Host, auth, newDialer())
		if err != nil {
			return nil, fmt.Errorf("upstream.proxy: 构造 socks5 拨号器失败: %w", err)
		}
		cd, ok := d.(proxy.ContextDialer)
		if !ok {
			return nil, fmt.Errorf("upstream.proxy: socks5 拨号器不支持 context（%T）", d)
		}
		return cd.DialContext, nil
	default:
		// http / https：自建 CONNECT 隧道。
		//
		// 为什么不用 http.Transport 代劳：它的 Proxy 钩子只对 http/https **目标**建隧道
		// （见 transport.go connectMethodForRequest），而 Redis 是裸 TCP；而且它的
		// DialContext 字段是**目标**拨号钩子，直接调它拨的是目标不是代理（会把目标
		// 域名交给本地 DNS，正好是本功能要避免的泄漏）。故这里自己拨代理 + 自建隧道。
		return newHTTPProxyDialer(u), nil
	}
}

// newHTTPProxyDialer 构造「经 HTTP(S) 代理的 CONNECT 隧道」拨号器。
//
// 流程：拨代理（https 代理先做 TLS）→ 写 CONNECT <目标> → 校验 200 → 交出连接。
// 目标地址**原样写进 CONNECT 请求行**（域名不经本地 DNS），与 socks5 的 FQDN
// 地址类型同一语义：解析一律在代理侧发生。
func newHTTPProxyDialer(u *url.URL) DialContextFunc {
	proxyAuth := proxyAuthHeader(u)
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		switch network {
		case "tcp", "tcp4", "tcp6":
		default:
			return nil, fmt.Errorf("upstream.proxy: CONNECT 隧道只支持 tcp，收到 network=%q", network)
		}
		conn, err := newDialer().DialContext(ctx, "tcp", u.Host)
		if err != nil {
			return nil, fmt.Errorf("upstream.proxy: 连接代理 %s 失败: %w", u.Host, err)
		}
		if u.Scheme == "https" {
			// https 代理：先与代理建 TLS（超时与主 Transport 同款）。
			hctx, cancel := context.WithTimeout(ctx, tlsHandshakeTimeout)
			defer cancel()
			tlsConn := tls.Client(conn, &tls.Config{ServerName: u.Hostname()})
			if err := tlsConn.HandshakeContext(hctx); err != nil {
				conn.Close()
				return nil, fmt.Errorf("upstream.proxy: 与代理 %s 的 TLS 握手失败: %w", u.Host, err)
			}
			conn = tlsConn
		}
		return proxyConnect(conn, addr, proxyAuth)
	}
}

// proxyAuthHeader 代理凭据的 Proxy-Authorization 头值（无凭据返回空串）。
// 密码进 URL 的 userinfo（config 里的 proxy 字段），只在此处转成 Basic 头。
func proxyAuthHeader(u *url.URL) string {
	if u.User == nil {
		return ""
	}
	pw, _ := u.User.Password()
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(u.User.Username()+":"+pw))
}

// proxyConnect 在已连到代理的连接上完成 CONNECT 握手。
//
// 代理返回非 200 时关连接并报错（不把代理的错误页当隧道用）。
// 200 之后代理若多吐了字节（非规范行为），用 bufferedConn 接回读路径，不丢数据。
func proxyConnect(conn net.Conn, addr, proxyAuth string) (net.Conn, error) {
	var sb strings.Builder
	sb.WriteString("CONNECT " + addr + " HTTP/1.1\r\nHost: " + addr + "\r\n")
	if proxyAuth != "" {
		sb.WriteString("Proxy-Authorization: " + proxyAuth + "\r\n")
	}
	sb.WriteString("\r\n")
	if _, err := io.WriteString(conn, sb.String()); err != nil {
		conn.Close()
		return nil, fmt.Errorf("upstream.proxy: 写 CONNECT 请求失败: %w", err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("upstream.proxy: 读 CONNECT 响应失败: %w", err)
	}
	if resp.Body != nil {
		resp.Body.Close()
	}
	if resp.StatusCode != http.StatusOK {
		conn.Close()
		return nil, fmt.Errorf("upstream.proxy: CONNECT %s 被代理拒绝: %s", addr, resp.Status)
	}
	if br.Buffered() > 0 {
		return &bufferedConn{Conn: conn, r: br}, nil
	}
	return conn, nil
}

// bufferedConn 把 CONNECT 响应读取后残留的缓冲字节接回连接读路径。
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) { return c.r.Read(b) }

// outboundTransport 取出站 Transport。HTTP 与 ChatHTTP 共享同一实例（New），
// 故只需断言 HTTP 侧；测试注入的自定义 RoundTripper（roundTripFn 等）不支持代理，
// 返回明确错误而非静默忽略成「配了代理但没生效」。
func (c *Client) outboundTransport() (*http.Transport, error) {
	if c == nil || c.HTTP == nil {
		return nil, fmt.Errorf("upstream: HTTP client 未初始化，无法配置出站代理")
	}
	tr, ok := c.HTTP.Transport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("upstream: 出站 Transport 类型为 %T，不支持配置出站代理", c.HTTP.Transport)
	}
	return tr, nil
}

// parseProxyURL 解析并归一化代理 URL（scheme / 主机 / 默认端口三处归一）。
func parseProxyURL(raw string) (*url.URL, error) {
	// 无 "://" 视为省略 scheme：url.Parse("127.0.0.1:7890") 会把 "127.0.0.1"
	// 解析成 scheme、端口解析成 Opaque（经典陷阱），故先补齐再解析。
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("upstream.proxy: 非法 URL %q: %w", raw, err)
	}
	scheme := strings.ToLower(u.Scheme)
	switch scheme {
	case "http", "https", "socks5", "socks5h":
	default:
		return nil, fmt.Errorf("upstream.proxy: 不支持的 scheme %q（支持 http / https / socks5 / socks5h）", u.Scheme)
	}
	if u.Hostname() == "" {
		return nil, fmt.Errorf("upstream.proxy: 缺少主机名：%q", raw)
	}
	if u.Port() == "" {
		u.Host = net.JoinHostPort(u.Hostname(), defaultProxyPort(scheme))
	}
	u.Scheme = scheme
	return u, nil
}

// defaultProxyPort 省略端口时的默认值（与各 scheme 的常规监听端口一致）。
func defaultProxyPort(scheme string) string {
	switch scheme {
	case "https":
		return "443"
	case "socks5", "socks5h":
		return "1080"
	default:
		return "80"
	}
}

// portOfURL 取请求 URL 的端口（省略时按 scheme 补默认），供 no_proxy 的 host:port
// 条目比对。未知 scheme 且未写端口时返回 ""（端口条件不满足，按不匹配处理）。
func portOfURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	if p := u.Port(); p != "" {
		return p
	}
	switch strings.ToLower(u.Scheme) {
	case "http":
		return "80"
	case "https":
		return "443"
	default:
		return ""
	}
}

// proxyBypass no_proxy 绕过清单（不可变，构造后只读，可并发 match）。
type proxyBypass struct {
	all     bool // "*"：全部绕过
	entries []proxyBypassEntry
}

// proxyBypassEntry 单条绕过规则。三类互斥：CIDR / IP / 域名。
type proxyBypassEntry struct {
	host          string // 小写且带前导点（".example.com"）
	hostMatchSelf bool   // "example.com" 形态：匹配自身；".example.com" 只匹配子域
	port          string // 非空则端口也须一致
	ip            net.IP
	ipnet         *net.IPNet
}

// newProxyBypass 解析 no_proxy（逗号分隔，空 = 无规则）。
// 规则按 net/http 的 NO_PROXY 语义逐条对齐；明显非法（CIDR 写坏、端口非数字、
// 空主机）返回错误——静默忽略会让人误以为「已经绕过了」。
func newProxyBypass(noProxy string) (*proxyBypass, error) {
	b := &proxyBypass{}
	for _, item := range strings.Split(noProxy, ",") {
		p := strings.ToLower(strings.TrimSpace(item))
		if p == "" {
			continue
		}
		if p == "*" {
			b.all = true
			return b, nil
		}
		// CIDR 网段
		if _, ipnet, err := net.ParseCIDR(p); err == nil {
			b.entries = append(b.entries, proxyBypassEntry{ipnet: ipnet})
			continue
		}
		if strings.Contains(p, "/") {
			return nil, fmt.Errorf("upstream.no_proxy: 非法网段 %q（应为 CIDR，如 10.0.0.0/8）", item)
		}
		host, port := splitHostPortLoose(p)
		if port != "" {
			if _, err := strconv.Atoi(port); err != nil {
				return nil, fmt.Errorf("upstream.no_proxy: 非法端口 %q（条目 %q）", port, item)
			}
		}
		if host == "" {
			return nil, fmt.Errorf("upstream.no_proxy: 空主机（条目 %q）", item)
		}
		// IP 字面量（v4/v6）
		if ip := net.ParseIP(host); ip != nil {
			b.entries = append(b.entries, proxyBypassEntry{ip: ip, port: port})
			continue
		}
		// 域名：*.foo.com → .foo.com（只匹配子域）；foo.com → .foo.com（自身 + 子域）
		if strings.HasPrefix(host, "*.") {
			host = host[1:]
		}
		matchSelf := false
		if !strings.HasPrefix(host, ".") {
			matchSelf = true
			host = "." + host
		}
		b.entries = append(b.entries, proxyBypassEntry{host: host, hostMatchSelf: matchSelf, port: port})
	}
	return b, nil
}

// match 判断 host:port 是否命中绕过清单（b 为 nil 表示未启用代理，恒不命中）。
func (b *proxyBypass) match(host, port string) bool {
	if b == nil {
		return false
	}
	if b.all {
		return true
	}
	host = strings.ToLower(strings.TrimSpace(host))
	parsed := net.ParseIP(host)
	for _, e := range b.entries {
		if e.port != "" && e.port != port {
			continue
		}
		switch {
		case e.ipnet != nil:
			if parsed != nil && e.ipnet.Contains(parsed) {
				return true
			}
		case e.ip != nil:
			if parsed != nil && e.ip.Equal(parsed) {
				return true
			}
		default:
			// 域名匹配自身（"foo.com"）或任意子域（后缀 ".foo.com"）；IP 目标不匹配域名规则。
			if parsed == nil && (strings.HasSuffix(host, e.host) || (e.hostMatchSelf && host == e.host[1:])) {
				return true
			}
		}
	}
	return false
}

// splitHostPortLoose 宽松拆分 host[:port]：无端口、裸 IPv6、[v6]:port 三种形态都吃。
func splitHostPortLoose(s string) (host, port string) {
	if h, p, err := net.SplitHostPort(s); err == nil {
		return h, p
	}
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		return s[1 : len(s)-1], ""
	}
	return s, ""
}
