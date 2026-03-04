# Unit tests for journal error module
# Tests for journal error types

import unittest
import fractio/storage/journal/error

suite "Journal Error Unit Tests":

  test "RecoveryMode enum":
    check rmTolerateCorruptTail.ord == 0
    check rmTolerateCorruptTail == defaultRecoveryMode()

  test "RecoveryError enum":
    check reInsufficientLength.ord == 0
    check reTooManyItems.ord == 1
    check reChecksumMismatch.ord == 2
    check reInvalidFileName.ord == 3

  test "RecoveryError string representation":
    check $reInsufficientLength == "RecoveryError(reInsufficientLength)"
    check $reTooManyItems == "RecoveryError(reTooManyItems)"
    check $reChecksumMismatch == "RecoveryError(reChecksumMismatch)"
    check $reInvalidFileName == "RecoveryError(reInvalidFileName)"

  test "RecoveryError distinct values":
    check reInsufficientLength != reTooManyItems
    check reTooManyItems != reChecksumMismatch
    check reChecksumMismatch != reInvalidFileName
    check reInvalidFileName != reInsufficientLength
