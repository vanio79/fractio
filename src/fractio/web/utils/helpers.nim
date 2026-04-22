# Fractio Web Dashboard - Utility Helpers
#
# Display helper functions for formatting data.

import std/strutils

proc roleStr*(r: int): string =
  case r
  of 1: "Leader"
  of 2: "Follower"
  of 3: "Candidate"
  else: "Unknown"

proc roleStrFromStr*(r: string): string =
  ## Convert role string from API ("leader", "follower", "unknown") to display format.
  case r.toLowerAscii()
  of "leader": "Leader"
  of "follower": "Follower"
  of "candidate": "Candidate"
  else: "Unknown"

proc uptimeStr*(secs: int): string =
  let h = secs div 3600
  let m = (secs mod 3600) div 60
  let s = secs mod 60
  if h > 0: $h & "h " & $m & "m"
  elif m > 0: $m & "m " & $s & "s"
  else: $s & "s"

proc healthStr*(s: int): string =
  case s
  of 0: "OK"
  of 1: "DEGRADED"
  of 2: "CRITICAL"
  else: "UNKNOWN"

proc healthColor*(s: int): string =
  case s
  of 0: "#1a7f37"
  of 1: "#b45309"
  of 2: "#c41010"
  else: "#888"

proc statusStr*(s: int): string =
  case s
  of 1: "active"
  of 2: "draining"
  of 3: "down"
  else: "unknown"

proc statusColor*(s: int): string =
  case s
  of 1: "#1a7f37"
  of 2: "#b45309"
  of 3: "#c41010"
  else: "#888"

proc formatBytes*(bytes: int): string =
  if bytes < 1024:
    $bytes & " B"
  elif bytes < 1024 * 1024:
    $(bytes / 1024).int & " KB"
  elif bytes < 1024 * 1024 * 1024:
    $(bytes / 1024 / 1024).int & " MB"
  else:
    $(bytes / 1024 / 1024 / 1024).int & " GB"

proc formatNumber*(n: int): string =
  if n < 1000:
    $n
  elif n < 1000000:
    $(n / 1000).int & "K"
  elif n < 1000000000:
    $(n / 1000000).int & "M"
  else:
    $(n / 1000000000).int & "B"
