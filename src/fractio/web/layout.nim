# Shared layout helpers — nav style + page shell template.

proc navStyle*(active: bool): string =
  if active:
    "color:#fff;text-decoration:none;padding:.6rem 1rem;font-size:.82rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em;border-bottom:2px solid #e81c1c"
  else:
    "color:#bbb;text-decoration:none;padding:.6rem 1rem;font-size:.82rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em"
