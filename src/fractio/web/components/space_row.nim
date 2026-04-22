# Fractio Web Dashboard - Space Row Component
#
# Expandable row component for displaying space information with group details.

import happyx
import std/jsffi
import ../styles
import ../store

# Helper procs for JsObject access
proc getSpaceField(space: JsObject, field: cstring): JsObject =
  {.emit: "return `space`[`field`];".}

proc getSpaceInt(space: JsObject, field: cstring): int =
  {.emit: "return `space`[`field`] || 0;".}

proc getSpaceStr(space: JsObject, field: cstring): cstring =
  {.emit: "return `space`[`field`] || '';".}

proc getGroupsLen(groups: JsObject): int =
  {.emit: "return `groups`.length || 0;".}

proc getArrayLen(arr: JsObject): int =
  {.emit: "return `arr`.length || 0;".}

proc getArrayElem(arr: JsObject, idx: int): JsObject =
  {.emit: "return `arr`[`idx`];".}

proc getSpaceIdStr(space: JsObject): cstring =
  ## Get spaceId as a string (ULID format)
  {.emit: "return String(`space`.spaceId || '');".}

proc hashSpaceId(sid: cstring): int =
  ## Create a simple numeric hash from ULID string for use in expanded state
  {.emit: """
  var s = `sid` || '';
  var h = 0;
  for (var i = 0; i < s.length; i++) {
    h = ((h << 5) - h) + s.charCodeAt(i);
    h = h & h; // Convert to 32bit integer
  }
  `result` = Math.abs(h);
  """.}

component SpaceRow:
  space: JsObject
  expanded: bool

  `html`:
    let dark = gDarkMode.get()
    let space = self.space.get() # unwrap State
    let isExpanded = self.expanded.get() # unwrap State
    let sidStr = $getSpaceIdStr(space)
    let sidHash = hashSpaceId(getSpaceIdStr(space))
    let sname = $getSpaceStr(space, "name")
    let srep = getSpaceInt(space, "replicas")
    let srepStr = if srep == 0: "ALL" else: $srep & " replicas"
    let sgc = getSpaceInt(space, "groupCount")
    let sgcStr = $sgc & " groups"
    let schevron = if isExpanded: "▾" else: "▸"
    let rowStyle = cardStyle(dark) & ";cursor:pointer"
    let rowId = "space-row-" & sidStr

    tDiv(
      id = rowId,
      style = rowStyle
    ):
      @click:
        discard toggleSpaceExpanded(sidHash)
      tSpan(style = "margin-right:0.5rem"):
        {schevron}
      tSpan(style = "margin-right:1rem;font-weight:600"):
        {sname}
      tSpan(style = "margin-right:1rem"):
        {srepStr}
      tSpan:
        {sgcStr}

    if isExpanded:
      tDiv(style = cardStyle(dark)):
        let groups = getSpaceField(space, "groups")
        let groupsLen = getGroupsLen(groups)
        if groupsLen > 0:
          for gi in 0 ..< groupsLen:
            let groupText = "Group " & $gi
            tDiv:
              {groupText}
        else:
          tDiv:
            "No group data"

component SpaceList:
  `html`:
    let dark = gDarkMode.get()
    let spacesArr = gSpaces.get()
    let spacesLen = getArrayLen(spacesArr)
    let expandedSpaces = gExpandedSpaces.get()
    let titleStyle = "font-size:1.05rem;font-weight:700;color:" & (
        if dark: DarkText else: "#111")

    tH2(style = titleStyle):
      "Spaces"

    if spacesLen == 0:
      tDiv:
        if loadedSpaces:
          "No spaces found."
        else:
          "Loading spaces..."
    else:
      tDiv:
        for si in 0 ..< spacesLen:
          let sp = getArrayElem(spacesArr, si)
          let sidHash = hashSpaceId(getSpaceIdStr(sp))
          let isSpaceExpanded = sidHash in expandedSpaces
          SpaceRow(space = sp, expanded = isSpaceExpanded)
