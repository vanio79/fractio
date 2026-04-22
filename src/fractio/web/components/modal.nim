# Fractio Web Dashboard - Modal Dialog Component
#
# Shoelace-based modal for confirmation dialogs and other overlays.

import happyx
import ../store

# Callback wrappers for modal actions
proc modalCancelAction*(): bool =
  closeModal()
  true

proc modalConfirmAction*(): bool =
  let cfg = gModalConfig.get()
  if cfg.onConfirm != nil:
    cfg.onConfirm()
  closeModal()
  true

component GlobalModal:
  `html`:
    let open = gModalOpen.get()
    let cfg = gModalConfig.get()
    let titleStr = cfg.title
    let msgStr = cfg.message
    let cancelStr = cfg.cancelText
    let confirmStr = cfg.confirmText
    let isDangerous = cfg.dangerous
    let variantStr = if isDangerous: "danger" else: "primary"

    tSlDialog(
      label = titleStr,
      style = "--width:400px",
      open = open
    ):
      tDiv(style = "padding:1rem"):
        tP:
          {msgStr}
      tSlButton(
        variant = "default",
        slot = "footer"
      ):
        {cancelStr}
        @click:
          discard modalCancelAction()
      tSlButton(
        variant = variantStr,
        slot = "footer"
      ):
        {confirmStr}
        @click:
          discard modalConfirmAction()
