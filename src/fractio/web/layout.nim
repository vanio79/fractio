# Shared layout helpers — nav style + page shell constants.

proc navStyle*(active: bool): string =
  if active:
    "color:#fff;text-decoration:none;padding:.6rem 1rem;font-size:.82rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em;border-bottom:2px solid #e81c1c"
  else:
    "color:#bbb;text-decoration:none;padding:.6rem 1rem;font-size:.82rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em"

const navItems* = [
  ("/#/",        "Dashboard"),
  ("/#/nodes",   "Nodes"),
  ("/#/metrics", "Metrics"),
  ("/#/clock",   "Clock"),
  ("/#/spaces",  "Spaces"),
  ("/#/data",    "Data"),
]

const headerStyle*  = "display:flex;align-items:center;gap:1rem;padding:0 1.75rem;height:60px;background:#e81c1c;box-shadow:0 2px 8px rgba(0,0,0,.18);position:sticky;top:0;z-index:100"
const logoStyle*    = "font-size:1.1rem;font-weight:800;color:#fff;letter-spacing:.1em"
proc badgeStyle*(bg: string): string =
  "background:" & bg & ";color:#fff;padding:.25rem .75rem;border-radius:999px;font-size:.8rem;font-weight:700"
const navBarStyle*  = "background:#2d2d2d;display:flex;padding:0 1.25rem"
const mainStyle*    = "flex:1;padding:1.75rem;max-width:1260px;width:100%"
const shellStyle*   = "display:flex;flex-direction:column;min-height:100vh"
const footerStyle*  = "padding:.75rem 1.75rem;background:#2d2d2d;color:#999;font-size:.75rem;text-align:center"
