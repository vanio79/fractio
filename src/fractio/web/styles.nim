# Fractio Web Dashboard - Styles
#
# This module provides CSS styles for the SPA frontend.
# Supports both light and dark themes.

# =============================================================================
# CSS Color Variables
# =============================================================================

# Primary brand colors
const PrimaryColor* = "#e81c1c"
const PrimaryDark* = "#c41010"
const PrimaryLight* = "#ff4d4d"

# Semantic colors
const SuccessColor* = "#1a7f37"
const WarningColor* = "#b45309"
const DangerColor* = "#c41010"
const InfoColor* = "#2563eb"

# Light theme
const LightBg* = "#f8f8f8"
const LightCardBg* = "#ffffff"
const LightText* = "#111111"
const LightTextMuted* = "#666666"
const LightBorder* = "#e0e0e0"
const LightNavBg* = "#2d2d2d"
const LightHeaderBg* = PrimaryColor

# Dark theme
const DarkBg* = "#1a1a1a"
const DarkCardBg* = "#2d2d2d"
const DarkText* = "#f0f0f0"
const DarkTextMuted* = "#888888"
const DarkBorder* = "#404040"
const DarkNavBg* = "#1a1a1a"
const DarkHeaderBg* = "#2d2d2d"

# =============================================================================
# Inline Style Helpers (for component inline styles)
# =============================================================================

proc headerStyle*(dark: bool): string =
  if dark:
    "display:flex;align-items:center;gap:1rem;padding:0 1.75rem;height:60px;background:" &
        DarkHeaderBg & ";box-shadow:0 2px 8px rgba(0,0,0,.18);position:sticky;top:0;z-index:100"
  else:
    "display:flex;align-items:center;gap:1rem;padding:0 1.75rem;height:60px;background:" &
        PrimaryColor & ";box-shadow:0 2px 8px rgba(0,0,0,.18);position:sticky;top:0;z-index:100"

proc navBarStyle*(dark: bool): string =
  if dark:
    "background:" & DarkNavBg & ";display:flex;padding:0 1.25rem"
  else:
    "background:" & LightNavBg & ";display:flex;padding:0 1.25rem"

proc mainStyle*(dark: bool): string =
  if dark:
    "flex:1;padding:1.75rem;max-width:1260px;width:100%;background:" & DarkBg &
        ";color:" & DarkText
  else:
    "flex:1;padding:1.75rem;max-width:1260px;width:100%"

proc shellStyle*(dark: bool): string =
  if dark:
    "display:flex;flex-direction:column;min-height:100vh;background:" & DarkBg &
        ";color:" & DarkText
  else:
    "display:flex;flex-direction:column;min-height:100vh"

proc footerStyle*(dark: bool): string =
  if dark:
    "padding:.75rem 1.75rem;background:" & DarkNavBg & ";color:" &
        DarkTextMuted & ";font-size:.75rem;text-align:center"
  else:
    "padding:.75rem 1.75rem;background:" & LightNavBg & ";color:#999;font-size:.75rem;text-align:center"

proc cardStyle*(dark: bool): string =
  if dark:
    "background:" & DarkCardBg & ";border:1px solid " & DarkBorder & ";border-radius:6px;padding:1rem;box-shadow:0 1px 4px rgba(0,0,0,.07)"
  else:
    "background:" & LightCardBg & ";border:1px solid " & LightBorder & ";border-radius:6px;padding:1rem;box-shadow:0 1px 4px rgba(0,0,0,.07)"

proc statCardStyle*(dark: bool): string =
  if dark:
    "background:" & DarkCardBg & ";border-top:3px solid " & PrimaryColor & ";border-radius:6px;padding:1rem;box-shadow:0 1px 4px rgba(0,0,0,.07);text-align:center"
  else:
    "background:" & LightCardBg & ";border-top:3px solid " & PrimaryColor & ";border-radius:6px;padding:1rem;box-shadow:0 1px 4px rgba(0,0,0,.07);text-align:center"

proc labelStyle*(dark: bool): string =
  if dark:
    "font-size:.68rem;color:" & DarkTextMuted & ";text-transform:uppercase;letter-spacing:.07em;margin-bottom:.5rem;font-weight:600"
  else:
    "font-size:.68rem;color:#666;text-transform:uppercase;letter-spacing:.07em;margin-bottom:.5rem;font-weight:600"

proc valueStyle*(dark: bool): string =
  "font-size:1.5rem;font-weight:700;color:" & PrimaryColor

proc navStyle*(active: bool, dark: bool): string =
  let baseColor = if dark: DarkTextMuted else: "#bbb"
  let activeColor = if dark: DarkText else: "#fff"
  if active:
    "color:" & activeColor & ";text-decoration:none;padding:.6rem 1rem;font-size:.82rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em;border-bottom:2px solid " & PrimaryColor
  else:
    "color:" & baseColor & ";text-decoration:none;padding:.6rem 1rem;font-size:.82rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em"

proc logoStyle*: string =
  "font-size:1.1rem;font-weight:800;color:#fff;letter-spacing:.1em"

proc badgeStyle*(bg: string): string =
  "background:" & bg & ";color:#fff;padding:.25rem .75rem;border-radius:999px;font-size:.8rem;font-weight:700"

proc tableHeaderStyle*(dark: bool): string =
  if dark:
    "background:#3a3a3a;color:" & DarkText & ";padding:.55rem .85rem;text-align:left;font-size:.7rem;text-transform:uppercase;letter-spacing:.07em;font-weight:600"
  else:
    "background:#3a3a3a;color:#fff;padding:.55rem .85rem;text-align:left;font-size:.7rem;text-transform:uppercase;letter-spacing:.07em;font-weight:600"

proc tableCellStyle*(dark: bool): string =
  if dark:
    "padding:.55rem .85rem;border-bottom:1px solid " & DarkBorder & ";color:" &
        DarkText & ";font-family:monospace;font-size:.82rem"
  else:
    "padding:.55rem .85rem;border-bottom:1px solid #eee;color:#222;font-family:monospace;font-size:.82rem"

proc tableRowHoverStyle*(dark: bool): string =
  if dark:
    DarkCardBg
  else:
    "#fff5f5"

# =============================================================================
# Shoelace Theme CSS (injected at app startup)
# =============================================================================

const shoelaceThemeCss* = """
:root {
  --sl-color-primary-50: #fff0f0;
  --sl-color-primary-100: #ffd6d6;
  --sl-color-primary-200: #ffadad;
  --sl-color-primary-300: #ff8080;
  --sl-color-primary-400: #ff4d4d;
  --sl-color-primary-500: #e81c1c;
  --sl-color-primary-600: #c41010;
  --sl-color-primary-700: #a00000;
  --sl-color-primary-800: #7a0000;
  --sl-color-primary-900: #550000;
  --sl-color-primary-950: #330000;
  --sl-font-sans: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  
  /* Custom Fractio variables */
  --fractio-primary: #e81c1c;
  --fractio-success: #1a7f37;
  --fractio-warning: #b45309;
  --fractio-danger: #c41010;
  --fractio-info: #2563eb;
  
  /* Light theme (default) */
  --fractio-bg: #f8f8f8;
  --fractio-card-bg: #ffffff;
  --fractio-text: #111111;
  --fractio-text-muted: #666666;
  --fractio-border: #e0e0e0;
  --fractio-nav-bg: #2d2d2d;
}

[data-theme="dark"] {
  --sl-color-primary-50: #330000;
  --sl-color-primary-100: #550000;
  --sl-color-primary-200: #7a0000;
  --sl-color-primary-300: #a00000;
  --sl-color-primary-400: #c41010;
  --sl-color-primary-500: #e81c1c;
  --sl-color-primary-600: #ff4d4d;
  --sl-color-primary-700: #ff8080;
  --sl-color-primary-800: #ffadad;
  --sl-color-primary-900: #ffd6d6;
  --sl-color-primary-950: #fff0f0;
  
  /* Dark theme overrides */
  --fractio-bg: #1a1a1a;
  --fractio-card-bg: #2d2d2d;
  --fractio-text: #f0f0f0;
  --fractio-text-muted: #888888;
  --fractio-border: #404040;
  --fractio-nav-bg: #1a1a1a;
}

/* Global styles */
* { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100%; }
body {
  font-family: var(--sl-font-sans);
  background: var(--fractio-bg);
  color: var(--fractio-text);
  min-height: 100vh;
}

/* App shell */
.app { display: flex; flex-direction: column; min-height: 100vh; }

/* Header */
.fractio-header {
  display: flex;
  align-items: center;
  gap: 1rem;
  padding: 0 1.75rem;
  height: 60px;
  background: var(--fractio-primary);
  box-shadow: 0 2px 8px rgba(0,0,0,.18);
  position: sticky;
  top: 0;
  z-index: 100;
}

[data-theme="dark"] .fractio-header {
  background: var(--fractio-nav-bg);
  border-bottom: 2px solid var(--fractio-primary);
}

.fractio-logo {
  font-size: 1.1rem;
  font-weight: 800;
  color: #fff;
  letter-spacing: .1em;
  display: flex;
  align-items: center;
  gap: .45rem;
}

/* Navigation */
.fractio-nav {
  background: var(--fractio-nav-bg);
  display: flex;
  gap: 0;
  padding: 0 1.25rem;
}

.fractio-nav a {
  color: #bbb;
  text-decoration: none;
  padding: .6rem 1rem;
  font-size: .82rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: .06em;
  border-bottom: 2px solid transparent;
  transition: color .15s, border-color .15s;
}

.fractio-nav a:hover,
.fractio-nav a.active {
  color: #fff;
  border-bottom-color: var(--fractio-primary);
}

[data-theme="dark"] .fractio-nav a {
  color: var(--fractio-text-muted);
}

[data-theme="dark"] .fractio-nav a:hover,
[data-theme="dark"] .fractio-nav a.active {
  color: var(--fractio-text);
}

/* Main content area */
.fractio-main {
  flex: 1;
  padding: 1.75rem;
  max-width: 1260px;
  width: 100%;
}

/* Footer */
.fractio-footer {
  padding: .75rem 1.75rem;
  background: var(--fractio-nav-bg);
  color: #999;
  font-size: .75rem;
  text-align: center;
  letter-spacing: .03em;
}

[data-theme="dark"] .fractio-footer {
  color: var(--fractio-text-muted);
}

/* Stats grid */
.stats-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
  gap: 1rem;
}

/* Stat card */
.stat-card {
  background: var(--fractio-card-bg);
  border-top: 3px solid var(--fractio-primary);
  border-radius: 6px;
  padding: 1rem;
  box-shadow: 0 1px 4px rgba(0,0,0,.07);
  text-align: center;
}

.stat-label {
  font-size: .68rem;
  color: var(--fractio-text-muted);
  text-transform: uppercase;
  letter-spacing: .07em;
  margin-bottom: .5rem;
  font-weight: 600;
}

.stat-value {
  font-size: 1.5rem;
  font-weight: 700;
  color: var(--fractio-primary);
}

/* Card panel */
.fractio-card {
  background: var(--fractio-card-bg);
  border: 1px solid var(--fractio-border);
  border-radius: 6px;
  padding: 1rem;
  box-shadow: 0 1px 4px rgba(0,0,0,.07);
}

/* Panel header */
.panel-header {
  display: flex;
  align-items: center;
  gap: .75rem;
  margin-bottom: 1rem;
}

.panel-header h2 {
  font-size: 1.05rem;
  font-weight: 700;
  color: var(--fractio-text);
  margin: 0;
}

/* Data table */
.data-table {
  width: 100%;
  border-collapse: collapse;
  font-size: .875rem;
  background: var(--fractio-card-bg);
  border: 1px solid var(--fractio-border);
  border-radius: 6px;
  overflow: hidden;
}

.data-table th {
  background: #3a3a3a;
  color: #fff;
  padding: .55rem .85rem;
  text-align: left;
  font-size: .7rem;
  text-transform: uppercase;
  letter-spacing: .07em;
  font-weight: 600;
}

.data-table td {
  padding: .55rem .85rem;
  border-bottom: 1px solid var(--fractio-border);
  color: var(--fractio-text);
}

.data-table tbody tr:hover td {
  background: rgba(232, 28, 28, 0.05);
}

[data-theme="dark"] .data-table tbody tr:hover td {
  background: rgba(232, 28, 28, 0.1);
}

.data-table tbody tr:last-child td {
  border-bottom: none;
}

/* Table wrapper for scrolling */
.table-wrap {
  overflow-x: auto;
  margin-bottom: 1.25rem;
}

/* Form elements */
.form-row {
  display: flex;
  gap: .5rem;
  flex-wrap: wrap;
  align-items: flex-end;
  margin-bottom: .5rem;
}

.form-msg {
  font-size: .82rem;
  margin-top: .4rem;
  min-height: 1.3em;
}

.form-msg.ok { color: var(--fractio-success); }
.form-msg.err { color: var(--fractio-danger); }

/* Metrics grid */
.metrics-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
  gap: 1rem;
}

.metrics-table {
  width: 100%;
  font-size: .875rem;
  border-collapse: collapse;
}

.metrics-table td {
  padding: .35rem 0;
  color: var(--fractio-text-muted);
  border-bottom: 1px solid var(--fractio-border);
}

.metrics-table tr:last-child td {
  border-bottom: none;
}

.metrics-table td:last-child {
  text-align: right;
  font-family: 'SF Mono', 'Fira Mono', monospace;
  color: var(--fractio-primary);
  font-weight: 600;
}

/* Expandable rows */
.expandable-row {
  background: var(--fractio-card-bg);
  border: 1px solid var(--fractio-border);
  padding: .65rem 1rem;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: .75rem;
  transition: border-color .15s;
}

.expandable-row:hover {
  border-color: var(--fractio-primary);
}

.expandable-details {
  background: var(--fractio-bg);
  border: 1px solid var(--fractio-border);
  border-top: none;
  padding: 1rem;
}

/* Badge counts */
.count-badge {
  background: #eee;
  color: #444;
  padding: .2rem .6rem;
  border-radius: 999px;
  font-size: .8rem;
}

[data-theme="dark"] .count-badge {
  background: var(--fractio-border);
  color: var(--fractio-text-muted);
}

/* Status badges */
.status-badge {
  display: inline-flex;
  align-items: center;
  gap: .25rem;
  font-size: .82rem;
  font-weight: 600;
}

.status-badge.leader { color: var(--fractio-success); }
.status-badge.follower { color: var(--fractio-info); }
.status-badge.unknown { color: var(--fractio-text-muted); }
.status-badge.alive { color: var(--fractio-success); }
.status-badge.dead { color: var(--fractio-danger); }
.status-badge.rebalancing {
  background: var(--fractio-warning);
  color: #fff;
  padding: .15rem .5rem;
  border-radius: 999px;
  font-size: .75rem;
}

/* Breadcrumb */
.breadcrumb {
  display: flex;
  align-items: center;
  gap: .35rem;
  margin-bottom: 1.25rem;
  font-size: .85rem;
  color: var(--fractio-text-muted);
}

.breadcrumb a {
  color: var(--fractio-primary);
  font-weight: 600;
  text-decoration: none;
}

.breadcrumb a:hover {
  text-decoration: underline;
}

.breadcrumb .current {
  font-weight: 600;
  color: var(--fractio-text);
}

/* Grid card for data browser */
.grid-card {
  background: var(--fractio-card-bg);
  border: 1px solid var(--fractio-border);
  border-radius: 6px;
  padding: .85rem 1rem;
  transition: border-color .15s, box-shadow .15s;
  text-decoration: none;
  color: inherit;
  display: block;
}

.grid-card:hover {
  border-color: var(--fractio-primary);
  box-shadow: 0 2px 8px rgba(232, 28, 28, 0.15);
}

.grid-card.system {
  border-left: 3px solid var(--fractio-primary);
}

.grid-card-label {
  font-size: .65rem;
  color: var(--fractio-text-muted);
  text-transform: uppercase;
  letter-spacing: .07em;
  margin-bottom: .25rem;
  font-weight: 600;
}

.grid-card-title {
  font-size: .95rem;
  font-weight: 600;
  color: var(--fractio-text);
}

.grid-card-desc {
  font-size: .75rem;
  color: var(--fractio-text-muted);
}

/* SQL Editor */
.sql-editor-container {
  background: var(--fractio-card-bg);
  border: 1px solid var(--fractio-border);
  border-radius: 6px;
  padding: 1rem;
  margin-bottom: 1rem;
}

.sql-editor-header {
  display: flex;
  align-items: center;
  gap: .75rem;
  margin-bottom: .75rem;
}

.sql-editor-toolbar {
  display: flex;
  align-items: center;
  gap: .5rem;
  flex-wrap: wrap;
}

#monaco-editor {
  height: 300px;
  border: 1px solid var(--fractio-border);
  border-radius: 4px;
}

/* Pagination */
.pagination-controls {
  display: flex;
  align-items: center;
  gap: .5rem;
  margin-bottom: 1rem;
}

.pagination-info {
  font-size: .82rem;
  color: var(--fractio-text-muted);
}

/* Search box */
.search-box {
  display: flex;
  align-items: center;
  gap: .5rem;
  margin-bottom: 1rem;
}

/* Loading overlay */
.loading-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 1000;
}

/* Responsive */
@media (max-width: 768px) {
  .fractio-header {
    padding: 0 1rem;
  }
  .fractio-nav {
    padding: 0 .75rem;
  }
  .fractio-nav a {
    padding: .5rem .75rem;
    font-size: .75rem;
  }
  .fractio-main {
    padding: 1rem;
  }
  .stats-grid {
    grid-template-columns: repeat(auto-fill, minmax(120px, 1fr));
  }
  .metrics-grid {
    grid-template-columns: 1fr;
  }
}
"""

# =============================================================================
# Procedure to inject theme CSS
# =============================================================================

proc injectThemeCss*() =
  let css = shoelaceThemeCss
  {.emit: """
  var style = document.createElement('style');
  style.textContent = `css`;
  document.head.appendChild(style);
  """.}

# =============================================================================
# Nav Items
# =============================================================================

const navItems* = [
  ("/#/", "Dashboard"),
  ("/#/nodes", "Nodes"),
  ("/#/metrics", "Metrics"),
  ("/#/clock", "Clock"),
  ("/#/storage", "Storage"),
  ("/#/data", "Data"),
  ("/#/sql", "SQL"),
  ("/#/settings", "Settings"),
]
