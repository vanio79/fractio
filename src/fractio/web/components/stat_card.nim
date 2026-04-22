# Fractio Web Dashboard - Stat Card Component
#
# A simple component for displaying a statistic value with a label.

import happyx
import ../styles
import ../store

component StatCard:
  label: string
  value: string

  html:
    let dark = gDarkMode.get()
    tDiv(style = statCardStyle(dark)):
      tDiv(style = labelStyle(dark)):
        {self.label}
      tDiv(style = valueStyle(dark)):
        {self.value}
