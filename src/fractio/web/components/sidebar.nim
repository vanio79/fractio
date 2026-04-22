# Fractio Web Dashboard - Sidebar Component
#
# Alternative navigation sidebar for mobile or alternative layouts.

import happyx
import std/jsffi
import ../styles
import ../store
import ../js_interop

component SidebarNav:
  open: bool = false
  onClose: proc() = nil
  
  html:
    let dark = gDarkMode.get()
    
    tSlDrawer(
      label = "Navigation",
      placement = "start",
      style = "--size:250px"
    ):
      if self.open:
        @open:
          discard
      tNav(style = "display:flex;flex-direction:column;gap:0;padding:0"):
        for (href, label) in navItems:
          tA(
            href = href,
            style = "color:" & (if dark: DarkText else: "#111") & ";text-decoration:none;padding:.75rem 1rem;font-size:.9rem;font-weight:500;border-bottom:1px solid " & (if dark: DarkBorder else: "#eee"),
            @click = proc() =
              if self.onClose != nil:
                self.onClose()
          ):
            label

component SidebarMenu:
  open: bool = false
  
  html:
    let dark = gDarkMode.get()
    
    tSlDrawer(
      label = "Fractio Menu",
      placement = "start",
      contained = true,
      style = "--size:280px"
    ):
      if self.open:
        @open:
          discard
      
      # Logo area
      tDiv(style = "display:flex;align-items:center;gap:.5rem;padding:1rem;border-bottom:1px solid " & (if dark: DarkBorder else: "#eee")):
        tSpan(style = logoStyle & ";color:" & (if dark: DarkText else: "#111")):
          "⬡ FRACTIO"
      
      # Navigation
      tDiv(style = "padding:0"):
        for (href, label) in navItems:
          tA(
            href = href,
            style = "display:block;color:" & (if dark: DarkText else: "#111") & ";text-decoration:none;padding:.75rem 1rem;font-size:.9rem;font-weight:500;border-bottom:1px solid " & (if dark: DarkBorder else: "#eee") & ";transition:background .15s"
          ):
            label
      
      # Footer in sidebar
      tDiv(style = "padding:.75rem 1rem;border-top:1px solid " & (if dark: DarkBorder else: "#eee") & ";margin-top:auto"):
        ThemeToggle()