# Fractio Web Dashboard - Loading Spinner Component
#
# Shoelace-based loading spinner for async operations.

import happyx
import std/jsffi
import ../styles

component LoadingSpinner:
  size: string = "medium" # "small", "medium", "large"
  show: bool = true

  html:
    if self.show:
      tSlSpinner(style = "font-size:" & (case self.size:
        of "small": "1rem"
        of "medium": "2rem"
        of "large": "3rem"
        else: "2rem"))

component LoadingOverlay:
  message: string = "Loading..."

  html:
    tDiv(style = "position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.5);display:flex;justify-content:center;align-items:center;flex-direction:column;gap:.5rem;z-index:1000"):
      tSlSpinner(style = "font-size:3rem")
      tDiv(style = "color:#fff;font-size:.85rem"):
        self.message

component LoadingIndicator:
  loading: bool
  message: string = ""

  html:
    if self.loading:
      tDiv(style = "display:flex;align-items:center;gap:.5rem;padding:.5rem;color:#888"):
        tSlSpinner(style = "font-size:1rem")
        if self.message.len > 0:
          tSpan(style = "font-size:.82rem"):
            self.message
