# Display helper procs — pure functions, no external deps.

proc roleStr*(r: int): string =
  case r
  of 1: "Leader"
  of 2: "Follower"
  of 3: "Candidate"
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
