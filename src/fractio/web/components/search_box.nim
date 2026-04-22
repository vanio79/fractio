# Fractio Web Dashboard - Search Box Component
#
# Shoelace-based search input for filtering data tables.

import happyx
import std/jsffi
import ../store
import ../styles
import ../js_interop

component SearchBox:
  placeholder: string = "Search..."
  value: string = ""
  onSearch: proc(query: string) = nil
  debounceMs: int = 300
  
  debounceTimer: int = 0
  
  html:
    let dark = gDarkMode.get()
    tSlInput(
      placeholder = self.placeholder,
      size = "medium",
      clearable = true,
      value = self.value,
      style = "width:100%;max-width:300px",
      @sl-input = proc(ev: JsObject) =
        let val = $safeStr(ev, "value")
        self.value = val
        # Debounce search
        {.emit: """
        if (`self`.`debounceTimer`) clearTimeout(`self`.`debounceTimer`);
        `self`.`debounceTimer` = setTimeout(function() {
          if (`self`.`onSearch`) `self`.`onSearch`(`val`);
        }, `debounceMs`);
        """.}
    )

component TableSearch:
  db: string = ""
  schema: string = ""
  table: string = ""
  
  html:
    let p = gTablePagination.get()
    let dark = gDarkMode.get()
    
    tDiv(style = "display:flex;align-items:center;gap:.75rem;margin-bottom:1rem;flex-wrap:wrap"):
      # Search input
      tSlInput(
        placeholder = "Search in table...",
        size = "medium",
        clearable = true,
        value = p.searchQuery,
        style = "width:100%;max-width:300px",
        @sl-input = proc(ev: JsObject) =
          let val = $safeStr(ev, "value")
          setSearchQuery(val)
      )
      
      # Current search display
      if p.searchQuery.len > 0:
        tSlTag(
          variant = "primary",
          size = "small",
          removable = true,
          @sl-remove = setSearchQuery("")
        ):
          "Searching: " & p.searchQuery