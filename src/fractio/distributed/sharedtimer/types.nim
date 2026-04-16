# Shared Timer Types - re-export from types_base.nim
#
# This file provides backward compatibility for imports like:
#   import fractio/distributed/sharedtimer/types
#
# The actual type definitions are in types_base.nim which is excluded
# from coverage reporting since type definitions cannot be unit tested.

import ./types_base
export types_base
