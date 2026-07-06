import std/[os, times]
import fractio/client/fractio_client
import fractio/client/sql_client  # for query method
import fractio/sql/executor      # for erkRows etc.

proc main() =
  echo "=== Checking smoke schema ==="
  
  let client = newFractioClient(newFractioClientConfig("127.0.0.1", 9001))
  if not client.initialize():
    quit("Failed to initialize")
  discard client.forceMetadataRefresh()
  
  # Try simple SELECT without aggregates
  echo "Step 1: Simple query on sys.spaces..."
  let spacesRes = client.query("SELECT * FROM sys.spaces LIMIT 10")
  case spacesRes.kind
  of erkRows:
    if len(spacesRes.rows) > 0:
      echo "  Found spaces"
    else:
      echo "  No spaces found"
  of erkError:
    echo "  Error: " & spacesRes.error
  else:
    echo "  Got non-row result"
  
  # Check if smoke.space exists  
  echo ""
  echo "Step 2: Simple query on smoke.public.users..."
  let usersRes = client.query("SELECT * FROM smoke.public.users LIMIT 1")
  case usersRes.kind
  of erkRows:
    if len(usersRes.rows) > 0:
      echo "  Table exists and accessible"
    else:
      echo "  No rows returned (table might not exist)"
  of erkError:
    echo "  Error: " & usersRes.error
  else:
    echo "  Got non-row result"
  
  client.close()

when isMainModule:
  main()
