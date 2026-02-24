# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Any Tree
##
## Polymorphic tree type that can be either standard or blob tree.

import tree_ext
import blob_tree

type
  AnyTree* = ref object
    ## May be a standard Tree or a BlobTree
    case isStandard*: bool
    of true:
      tree*: pointer     # Would be Tree
    of false:
      blobTree*: pointer # Would be BlobTree

proc newStandardTree*(tree: pointer): AnyTree =
  AnyTree(isStandard: true, tree: tree)

proc newBlobTree*(blobTree: pointer): AnyTree =
  AnyTree(isStandard: false, blobTree: blobTree)
