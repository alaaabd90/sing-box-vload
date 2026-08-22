package tun

import tun "github.com/sagernet/sing-tun"

var HookBeforeCreatePlatformInterface func()

// HookWindowsTapAdapter constructs a tun.WinTun backed by a real
// TAP-Windows adapter instead of WinTun, when set (see the app's own
// go/cmd/vload_core/tap_windows.go) and the running inbound's
// option.TunInboundOptions.WindowsTapAdapter is true. nil (the default,
// and always on non-Windows platforms) leaves tun.New's normal WinTun
// path untouched.
var HookWindowsTapAdapter func(options tun.Options) (tun.WinTun, error)
