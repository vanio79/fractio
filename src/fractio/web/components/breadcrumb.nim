# Fractio Web Dashboard - Breadcrumb Component
#
# Navigation breadcrumb for the data browser and other hierarchical views.

import happyx
import std/jsffi
import ../styles
import ../store
import ../js_interop

type BreadcrumbItem* = object
  href*: string
  text*: string
  isCurrent*: bool

component Breadcrumb:
  items: seq[BreadcrumbItem]

  html:
    let dark = gDarkMode.get()
    tDiv(style = if dark: "display:flex;align-items:center;gap:.35rem;margin-bottom:1.25rem;font-size:.85rem;color:#888" else: "display:flex;align-items:center;gap:.35rem;margin-bottom:1.25rem;font-size:.85rem;color:#666"):
      for i, item in self.items:
        if item.isCurrent:
          tSpan(style = "font-weight:600;color:" & (
              if dark: "#f0f0f0" else: "#111")):
            item.text
        else:
          tA(href = item.href, style = "color:#e81c1c;font-weight:600;text-decoration:none"):
            item.text
          if i < self.items.len - 1:
            tSpan: " / "
