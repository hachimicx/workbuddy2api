package scheduler

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeScriptExec 记录命令构建参数并按需模拟执行失败，替代真实 exec 拉起 python3 子进程。
type fakeScriptExec struct {
	lastName string
	lastArgs []string
	lastDir  string
	lastEnv  []string
	runN     int
	err      error
}

func (f *fakeScriptExec) SetDir(dir string)   { f.lastDir = dir }
func (f *fakeScriptExec) SetEnv(env []string) { f.lastEnv = env }
func (f *fakeScriptExec) Run() error          { f.runN++; return f.err }

// installFakeExec 替换 newScriptCmd，测试结束还原。
func installFakeExec(t *testing.T) *fakeScriptExec {
	t.Helper()
	f := &fakeScriptExec{}
	orig := newScriptCmd
	newScriptCmd = func(name string, args ...string) scriptRunner {
		f.lastName, f.lastArgs = name, args
		return f
	}
	t.Cleanup(func() { newScriptCmd = orig })
	return f
}

func equalArgs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestNextWakeSchoolSlot 开学季任务在 school_hours（默认 12 点）处有独立时点。
func TestNextWakeSchoolSlot(t *testing.T) {
	s := New(Config{
		CheckinHours:      []int{9},
		TravelDisabled:    true,
		ActivityDisabled:  true,
		KeepaliveDisabled: true,
		SchoolHours:       []int{12},
	})
	at, kinds := s.nextWake(time.Date(2026, 9, 14, 11, 0, 0, 0, time.Local))
	if want := time.Date(2026, 9, 14, 12, 0, 0, 0, time.Local); !at.Equal(want) {
		t.Errorf("next=%v want %v（school 12:00 独立时点）", at, want)
	}
	if len(kinds) != 1 || kinds[0] != taskSchool {
		t.Errorf("kinds=%v want [school]", kinds)
	}
}

// TestNextWakeCatSlot 夜猫子任务在 cat_hours（默认 1 点）处有独立时点。
func TestNextWakeCatSlot(t *testing.T) {
	s := New(Config{
		CheckinHours:      []int{9},
		TravelDisabled:    true,
		ActivityDisabled:  true,
		KeepaliveDisabled: true,
		CatHours:          []int{1},
	})
	at, kinds := s.nextWake(time.Date(2026, 9, 14, 23, 0, 0, 0, time.Local))
	if want := time.Date(2026, 9, 15, 1, 0, 0, 0, time.Local); !at.Equal(want) {
		t.Errorf("next=%v want %v（cat 01:00 独立时点）", at, want)
	}
	if len(kinds) != 1 || kinds[0] != taskCat {
		t.Errorf("kinds=%v want [cat]", kinds)
	}
}

// TestNextWakeSchoolCatDisabled 显式禁用 school/cat 后排程只剩签到时点（互不影响）。
func TestNextWakeSchoolCatDisabled(t *testing.T) {
	s := New(Config{
		CheckinHours:      []int{21},
		TravelDisabled:    true,
		ActivityDisabled:  true,
		KeepaliveDisabled: true,
		SchoolHours:       []int{12},
		CatHours:          []int{1},
		SchoolDisabled:    true,
		CatDisabled:       true,
	})
	at, kinds := s.nextWake(time.Date(2026, 9, 14, 8, 0, 0, 0, time.Local))
	if want := time.Date(2026, 9, 14, 21, 0, 0, 0, time.Local); !at.Equal(want) {
		t.Errorf("next=%v want %v（school/cat 禁用 → 只有签到 21:00）", at, want)
	}
	if len(kinds) != 1 || kinds[0] != taskCheckin {
		t.Errorf("kinds=%v want [checkin]", kinds)
	}
}

// TestRunSchoolNowBuildsCommand RunSchoolNow 构造
// python3 scripts/school_open_day_2026.py ALL --run --yes，工作目录设为仓库根。
func TestRunSchoolNowBuildsCommand(t *testing.T) {
	// 防环境泄漏：WB2A_PYTHON 若在测试机已设置会改写 pythonCmd()，使默认值断言失败。
	t.Setenv("WB2A_PYTHON", "")
	f := installFakeExec(t)
	s := New(Config{})
	s.RunSchoolNow()
	if f.lastName != "python3" {
		t.Errorf("name=%q want python3", f.lastName)
	}
	want := []string{"scripts/school_open_day_2026.py", "ALL", "--run", "--yes"}
	if !equalArgs(f.lastArgs, want) {
		t.Errorf("args=%v want %v", f.lastArgs, want)
	}
	if f.lastDir != repoRoot() {
		t.Errorf("dir=%q want repo root %q", f.lastDir, repoRoot())
	}
	if _, err := os.Stat(filepath.Join(f.lastDir, "scripts", "school_open_day_2026.py")); err != nil {
		t.Errorf("仓库根 %q 内应有 scripts/school_open_day_2026.py: %v", f.lastDir, err)
	}
}

// TestRunCatNowBuildsCommand RunCatNow 构造
// python3 scripts/task_runner.py ALL --yes --only black_cat，工作目录为仓库根。
func TestRunCatNowBuildsCommand(t *testing.T) {
	t.Setenv("WB2A_PYTHON", "")
	f := installFakeExec(t)
	s := New(Config{})
	s.RunCatNow()
	want := []string{"scripts/task_runner.py", "ALL", "--yes", "--only", "black_cat"}
	if f.lastName != "python3" || !equalArgs(f.lastArgs, want) {
		t.Errorf("cmd=%s %v want python3 %v", f.lastName, f.lastArgs, want)
	}
	if f.lastDir != repoRoot() {
		t.Errorf("dir=%q want repo root %q", f.lastDir, repoRoot())
	}
}

// TestDispatchSchoolCatAndFailureWarnsOnly dispatch 把 school/cat 分发给对应脚本；
// 脚本失败只记 WARN（不 panic/不向上抛），且不影响后续任务继续分发。
func TestDispatchSchoolCatAndFailureWarnsOnly(t *testing.T) {
	t.Setenv("WB2A_PYTHON", "")
	f := installFakeExec(t)
	f.err = errors.New("boom boom")
	s := New(Config{})

	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(os.Stderr)
		log.SetFlags(log.LstdFlags)
	})

	s.dispatch(context.Background(), taskSchool)
	if f.runN != 1 || f.lastArgs[0] != "scripts/school_open_day_2026.py" {
		t.Errorf("dispatch(school) 未执行: runN=%d last=%v", f.runN, f.lastArgs)
	}
	s.dispatch(context.Background(), taskCat)
	if f.runN != 2 || f.lastArgs[0] != "scripts/task_runner.py" {
		t.Errorf("dispatch(cat) 未执行: runN=%d last=%v", f.runN, f.lastArgs)
	}
	out := buf.String()
	if !strings.Contains(out, "WARN") || !strings.Contains(out, "scripts/school_open_day_2026.py") {
		t.Errorf("school 失败未按 WARN 记录:\n%s", out)
	}
	if !strings.Contains(out, "scripts/task_runner.py") {
		t.Errorf("cat 失败未按 WARN 记录:\n%s", out)
	}
}

// TestPythonCmd WB2A_PYTHON 覆盖解释器名：缺省/空白回落 "python3"（保持
// 容器与既有测试的行为不变），显式设置时取其值（Windows 等仅有 python 的环境）。
func TestPythonCmd(t *testing.T) {
	t.Setenv("WB2A_PYTHON", "")
	if got := pythonCmd(); got != "python3" {
		t.Errorf("default pythonCmd()=%q want python3", got)
	}

	t.Setenv("WB2A_PYTHON", "   ")
	if got := pythonCmd(); got != "python3" {
		t.Errorf("blank pythonCmd()=%q want python3", got)
	}

	t.Setenv("WB2A_PYTHON", "python")
	if got := pythonCmd(); got != "python" {
		t.Errorf("override pythonCmd()=%q want python", got)
	}

	t.Setenv("WB2A_PYTHON", "  /usr/bin/python3.10  ")
	if got := pythonCmd(); got != "/usr/bin/python3.10" {
		t.Errorf("trim pythonCmd()=%q want /usr/bin/python3.10", got)
	}
}

// TestProxyEnv 子进程代理下发：WB2A_PROXY 非空 → HTTP_PROXY/HTTPS_PROXY(/NO_PROXY)
// 追加到子进程环境；未设置 → environ 原样返回（零行为变更，不覆盖用户既有 HTTP_PROXY）。
func TestProxyEnv(t *testing.T) {
	base := []string{"PATH=/bin", "HTTP_PROXY=http://user-set:1"}

	t.Setenv("WB2A_PROXY", "")
	t.Setenv("WB2A_NO_PROXY", "localhost")
	if got := proxyEnv(base); len(got) != len(base) {
		t.Errorf("WB2A_PROXY unset must leave environ untouched, got %v", got)
	}

	t.Setenv("WB2A_PROXY", "socks5h://127.0.0.1:1080")
	t.Setenv("WB2A_NO_PROXY", "localhost,.corp.example")
	got := proxyEnv(base)
	// 原有条目保留（exec 取最后一个同名键，故不需要先过滤）。
	if len(got) != len(base)+3 {
		t.Fatalf("env len=%d want %d: %v", len(got), len(base)+3, got)
	}
	if got[0] != "PATH=/bin" || got[1] != "HTTP_PROXY=http://user-set:1" {
		t.Errorf("original env must be preserved: %v", got[:2])
	}
	joined := strings.Join(got, "\n")
	for _, want := range []string{
		"HTTP_PROXY=socks5h://127.0.0.1:1080",
		"HTTPS_PROXY=socks5h://127.0.0.1:1080",
		"NO_PROXY=localhost,.corp.example",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("env missing %q: %v", want, got)
		}
	}

	// NO_PROXY 为空则不下发该键（只下发两个代理变量）。
	t.Setenv("WB2A_NO_PROXY", "")
	if got := proxyEnv(base); len(got) != len(base)+2 {
		t.Errorf("empty WB2A_NO_PROXY must not emit NO_PROXY: %v", got)
	}
}

// TestRunScriptSetsProxyEnv 接线验证：runScript 真的把 proxyEnv 结果交给子进程
// （proxyEnv 本身正确但没接线，等于没修）。
func TestRunScriptSetsProxyEnv(t *testing.T) {
	f := installFakeExec(t)
	t.Setenv("WB2A_PROXY", "127.0.0.1:7890")
	runScript("school", "/tmp", [][]string{{"python3", "scripts/x.py"}})
	if f.runN != 1 {
		t.Fatalf("runN=%d want 1", f.runN)
	}
	if !strings.Contains(strings.Join(f.lastEnv, "\n"), "HTTP_PROXY=127.0.0.1:7890") {
		t.Errorf("script subprocess env must carry the proxy: %v", f.lastEnv)
	}
}

// TestEffectiveProxyPriority 生效代理的优先级：WB2A_PROXY（显式 env）> config 注入。
func TestEffectiveProxyPriority(t *testing.T) {
	t.Cleanup(func() { SetScriptProxy("", "") })
	t.Setenv("WB2A_PROXY", "")
	t.Setenv("WB2A_NO_PROXY", "")

	// 1. 都未配置 → 空（不下发，零行为变更）。
	if p, n := effectiveProxy(); p != "" || n != "" {
		t.Errorf("nothing configured: (%q,%q) want empty", p, n)
	}

	// 2. 只有 config → 用 config 值。
	SetScriptProxy("socks5h://127.0.0.1:1080", "localhost")
	if p, n := effectiveProxy(); p != "socks5h://127.0.0.1:1080" || n != "localhost" {
		t.Errorf("config only: (%q,%q)", p, n)
	}

	// 3. env 覆盖 config（与网关其余部分「显式 env > config」一致）。
	t.Setenv("WB2A_PROXY", "http://127.0.0.1:7890")
	t.Setenv("WB2A_NO_PROXY", ".corp.example")
	if p, n := effectiveProxy(); p != "http://127.0.0.1:7890" || n != ".corp.example" {
		t.Errorf("env must win over config: (%q,%q)", p, n)
	}

	// 4. env 只设 proxy、config 设 no_proxy → 各自独立回落，不互相清空。
	t.Setenv("WB2A_NO_PROXY", "")
	if p, n := effectiveProxy(); p != "http://127.0.0.1:7890" || n != "localhost" {
		t.Errorf("per-field fallback: (%q,%q) want env proxy + config no_proxy", p, n)
	}
}

// TestProxyEnvFromConfigOnly config 配了代理、env 没配时，脚本子进程也必须拿到代理
// ——这是「config 作为唯一真相源」的回归锁（此前只有 env 路径能下发）。
func TestProxyEnvFromConfigOnly(t *testing.T) {
	t.Cleanup(func() { SetScriptProxy("", "") })
	t.Setenv("WB2A_PROXY", "")
	t.Setenv("WB2A_NO_PROXY", "")
	SetScriptProxy("socks5h://127.0.0.1:1080", "localhost")

	f := installFakeExec(t)
	runScript("school", "/tmp", [][]string{{"python3", "scripts/x.py"}})
	joined := strings.Join(f.lastEnv, "\n")
	for _, want := range []string{
		"HTTP_PROXY=socks5h://127.0.0.1:1080",
		"HTTPS_PROXY=socks5h://127.0.0.1:1080",
		"NO_PROXY=localhost",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("config-only proxy must reach the subprocess, missing %q: %v", want, f.lastEnv)
		}
	}
}
