# Fractio Web Dashboard - Toast Component
#
# Shoelace-based toast notification for success/error/warning/info messages.

import happyx
import ../store

component ToastAlert:
  message: string
  kind: string = "info" # "primary", "success", "warning", "danger", "info"
  duration: int = 5000
  closable: bool = true

  `html`:
    let msg = self.message.get()
    let variant = self.kind.get()
    let dur = self.duration.get()
    let close = self.closable.get()
    let durStr = $dur

    tSlAlert(
      variant = variant,
      duration = durStr,
      closable = close,
      style = "position:fixed;bottom:1rem;right:1rem;z-index:1000;max-width:400px"
    ):
      {msg}

proc showToast*(message: string, kind: string = "info") =
  gToastMessage.set(message)
  gToastType.set(kind)
  gShowToast.set(true)
  # Auto-hide after duration
  {.emit: """
  setTimeout(function() {
    `gShowToast`.value = false;
    `gShowToast`.`set`(`gShowToast`.value);
  }, 5000);
  """.}

proc hideToast*(): bool =
  gShowToast.set(false)
  true

component ToastContainer:
  `html`:
    if gShowToast.get():
      let msg = gToastMessage.get()
      let kind = gToastType.get()
      tSlAlert(
        variant = kind,
        duration = "5000",
        closable = true,
        style = "position:fixed;bottom:1rem;right:1rem;z-index:1000;max-width:400px;box-shadow:0 4px 12px rgba(0,0,0,.15)",
        @sl-hide = hideToast()
      ):
        {msg}
