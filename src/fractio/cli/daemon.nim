## Unix daemonization module for Fractio server process.
##
## Implements standard double-fork daemonization pattern:
## 1. Fork and exit parent (child inherits process group but is not leader)
## 2. Child calls setsid() to become session leader, detaching from terminal
## 3. Fork again and exit intermediate child (final child cannot acquire terminal)
## 4. Redirect stdin/stdout/stderr to log files or /dev/null
## 5. Write PID file for process management
##
## Usage:
##   let daemon = DaemonConfig(
##     pidFile: "/var/run/fractio/node1.pid",
##     logFile: "/var/log/fractio/node1.log",
##     workingDir: "/var/lib/fractio/node1"
##   )
##   daemonize(daemon)
##   # Process is now daemonized, continue with server startup
##
## Note: Daemonization is the DEFAULT behavior on POSIX systems.
## Use --foreground CLI flag or daemon.foreground config option to run in foreground.

when not defined(posix):
  {.error: "Daemonization is only supported on POSIX systems".}

import std/[os, strutils, posix]

type
  DaemonConfig* = object
    ## Configuration for daemonization.
    pidFile*: string ## Path to write PID file (e.g., "/var/run/fractio/node1.pid")
    logFile*: string ## Path for stdout/stderr redirection (optional, defaults to /dev/null)
    workingDir*: string ## Working directory after daemonization (optional, defaults to "/")
    umask*: Mode ## File mode creation mask (default: 0o022)

  DaemonError* = object of CatchableError
    ## Error raised during daemonization failure.

const
  DEFAULT_UMASK = Mode(0o022)

proc writePidFile(path: string, pid: Pid): void =
  ## Write the daemon's PID to the specified file.
  ## Creates parent directories if needed.
  let parentDir = parentDir(path)
  if parentDir != "" and not dirExists(parentDir):
    try:
      createDir(parentDir)
    except OSError as e:
      raise newException(DaemonError, "cannot create PID directory '" &
          parentDir & "': " & e.msg)

  var f = open(path, fmWrite)
  try:
    f.writeLine($pid.int)
  finally:
    f.close()

proc redirectStdStreams(logFile: string): void =
  ## Redirect stdin, stdout, stderr appropriately for daemon.
  ## stdin -> /dev/null (daemon should not read from terminal)
  ## stdout/stderr -> logFile if specified, else /dev/null

  # Close existing stdin
  discard posix.close(0)

  # Open /dev/null for stdin
  let nullFd = posix.open("/dev/null", O_RDONLY)
  if nullFd < 0:
    raise newException(DaemonError, "cannot open /dev/null for stdin: errno=" & $errno)

  # stdin is now /dev/null (fd 0)

  # Close stdout and stderr
  discard posix.close(1)
  discard posix.close(2)

  if logFile != "":
    # Open log file for stdout (will be fd 1)
    let logFd = posix.open(logFile, O_WRONLY or O_CREAT or O_APPEND, Mode(0o644))
    if logFd < 0:
      raise newException(DaemonError, "cannot open log file '" & logFile &
          "': errno=" & $errno)

    # Dup logFd to stderr (fd 2) - stdout is already fd 1 from open
    discard posix.dup(logFd)
  else:
    # Open /dev/null for stdout
    let nullOutFd = posix.open("/dev/null", O_WRONLY)
    if nullOutFd < 0:
      raise newException(DaemonError, "cannot open /dev/null for stdout: errno=" & $errno)

    # Dup to stderr
    discard posix.dup(nullOutFd)

proc daemonize*(cfg: DaemonConfig): void =
  ## Perform Unix daemonization using double-fork pattern.
  ##
  ## This proc forks the process twice, detaches from the controlling terminal,
  ## redirects standard streams, and writes a PID file.
  ##
  ## After calling this proc, the process is running as a daemon. The caller
  ## should continue with server initialization.
  ##
  ## Raises DaemonError if any step fails.

  let umaskVal = if cfg.umask == Mode(0): DEFAULT_UMASK else: cfg.umask

  # Step 1: First fork
  let pid1 = posix.fork()
  if pid1 < 0:
    raise newException(DaemonError, "first fork failed: errno=" & $errno)

  if pid1 > 0:
    # Parent process exits successfully
    # The caller (CLI) should have already printed success message
    quit(0)

  # Child (intermediate) continues

  # Step 2: Create new session - child becomes session leader, detaches from terminal
  if posix.setsid() < 0:
    raise newException(DaemonError, "setsid failed: errno=" & $errno)

  # Step 3: Second fork - prevents daemon from acquiring a controlling terminal
  let pid2 = posix.fork()
  if pid2 < 0:
    raise newException(DaemonError, "second fork failed: errno=" & $errno)

  if pid2 > 0:
    # Intermediate process exits
    quit(0)

  # Final daemon process continues

  # Step 4: Set umask for file creation
  discard posix.umask(umaskVal)

  # Step 5: Change working directory
  let wd = if cfg.workingDir != "" and dirExists(
      cfg.workingDir): cfg.workingDir else: "/"
  try:
    setCurrentDir(wd)
  except OSError as e:
    raise newException(DaemonError, "cannot change working directory to '" &
        wd & "': " & e.msg)

  # Step 6: Redirect standard streams
  redirectStdStreams(cfg.logFile)

  # Step 7: Write PID file
  if cfg.pidFile != "":
    writePidFile(cfg.pidFile, posix.getpid())

  # Daemon is now fully initialized
  # Process ID can be retrieved via getpid()

proc stopDaemon*(pidFile: string): bool =
  ## Stop a running daemon by sending SIGTERM to the PID in the file.
  ## Returns true if the daemon was signaled, false if PID file not found or invalid.
  if not fileExists(pidFile):
    return false

  var pid: Pid = Pid(-1)
  try:
    let f = open(pidFile, fmRead)
    let pidStr = f.readLine().strip()
    f.close()
    pid = Pid(parseInt(pidStr))
  except CatchableError:
    return false

  if pid.int <= 1:
    return false

  # Check if process exists
  let rc = posix.kill(pid, 0)
  if rc < 0 and errno == ESRCH:
    # Process doesn't exist, clean up stale PID file
    try:
      removeFile(pidFile)
    except OSError:
      discard
    return false

  # Send SIGTERM
  let killRc = posix.kill(pid, SIGTERM)
  if killRc < 0:
    return false

  # Wait briefly for process to terminate
  var attempts = 0
  while attempts < 10:
    sleep(100)
    let checkRc = posix.kill(pid, 0)
    if checkRc < 0 and errno == ESRCH:
      # Process terminated, clean up PID file
      try:
        removeFile(pidFile)
      except OSError:
        discard
      return true
    inc attempts

  # Process didn't terminate gracefully, send SIGKILL
  discard posix.kill(pid, SIGKILL)
  sleep(100)
  try:
    removeFile(pidFile)
  except OSError:
    discard
  return true

proc isDaemonRunning*(pidFile: string): bool =
  ## Check if a daemon process is running by examining the PID file.
  ## Returns true if PID file exists and process is alive.
  if not fileExists(pidFile):
    return false

  var pid: Pid = Pid(-1)
  try:
    let f = open(pidFile, fmRead)
    let pidStr = f.readLine().strip()
    f.close()
    pid = Pid(parseInt(pidStr))
  except CatchableError:
    return false

  if pid.int <= 1:
    return false

  # Check if process exists
  let rc = posix.kill(pid, 0)
  if rc < 0 and errno == ESRCH:
    # Process doesn't exist, stale PID file
    try:
      removeFile(pidFile)
    except OSError:
      discard
    return false

  return true

proc getDaemonPid*(pidFile: string): Pid =
  ## Get the PID of a running daemon from the PID file.
  ## Returns -1 if daemon not running or PID file invalid.
  if not isDaemonRunning(pidFile):
    return Pid(-1)

  try:
    let f = open(pidFile, fmRead)
    let pidStr = f.readLine().strip()
    f.close()
    return Pid(parseInt(pidStr))
  except CatchableError:
    return Pid(-1)
