# Fractio Web Dashboard - Node Row Component
#
# Expandable row component for displaying node information with storage details.

import happyx
import std/jsffi
import ../styles
import ../store

# Helper procs to extract data from JsObject
proc getNodeField(node: JsObject, field: cstring): JsObject =
  {.emit: "return `node`[`field`];".}

proc getIntField(node: JsObject, field: cstring): int =
  {.emit: "return parseInt(`node`[`field`]) || 0;".}

proc getStrField(node: JsObject, field: cstring): cstring =
  {.emit: "return `node`[`field`] || '';".}

proc getBoolField(node: JsObject, field: cstring): bool =
  {.emit: "return `node`[`field`] || false;".}

proc getArrayLen(arr: JsObject): int =
  {.emit: "return (`arr` && `arr`.length) || 0;".}

proc getArrayElem(arr: JsObject, idx: int): JsObject =
  {.emit: "return `arr`[`idx`];".}

proc getIntArrayElem(arr: JsObject, idx: int): int =
  {.emit: "return parseInt(`arr`[`idx`]) || 0;".}

component StorageDetails:
  node: JsObject

  `html`:
    let node = self.node.get() # unwrap State to get JsObject
    let nf = getNodeField(node, "numFiles")
    let nfLen = getArrayLen(nf)

    if nfLen > 0:
      tDiv:
        for lvl in 0 ..< nfLen:
          let fc = getIntArrayElem(nf, lvl)
          let levelText = "L" & $lvl & ": " & $fc & " files"
          tSpan:
            {levelText}
    else:
      tDiv:
        "No storage data available"

component NodeRow:
  node: JsObject

  `html`:
    let dark = gDarkMode.get()
    let node = self.node.get() # unwrap State to get JsObject
    let expandedNodes = gExpandedNodes.get()         # read global state directly
    let nodeId = getIntField(node, "nodeId")
    let isExpanded = nodeId in expandedNodes # compute from global state
    let host = getStrField(node, "host")
    let role = getStrField(node, "role")
    let alive = getBoolField(node, "alive")
    let chevron = if isExpanded: "▾" else: "▸"
    let nidStr = $nodeId
    let rowStyle = cardStyle(dark) & ";cursor:pointer"
    let rowId = "node-row-" & nidStr

    tDiv(
      id = rowId,
      style = rowStyle
    ):
      @click:
        discard toggleNodeExpanded(nodeId)
      tSpan(style = "margin-right:0.5rem"):
        {chevron}
      tSpan(style = "margin-right:1rem;font-weight:600"):
        {nidStr}
      tSpan(style = "margin-right:1rem"):
        {host}
      tSpan(style = "margin-right:1rem"):
        {role}
      tSpan:
        if alive: "alive" else: "unreachable"

    if isExpanded:
      tDiv(style = cardStyle(dark)):
        StorageDetails(node = node)

component NodeList:
  `html`:
    let dark = gDarkMode.get()
    let arr = gNodes.get()
    let arrLen = getArrayLen(arr)
    let titleStyle = "font-size:1.05rem;font-weight:700;color:" & (
        if dark: DarkText else: "#111")

    tH2(style = titleStyle):
      "Cluster Nodes"

    if arrLen == 0:
      tDiv:
        "No nodes registered"
    else:
      tDiv:
        for i in 0 ..< arrLen:
          let node = getArrayElem(arr, i)
          NodeRow(node = node)
