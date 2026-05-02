# fractio

A distributed SQL database with sharding and replication, built in Nim.

## Features

- Distributed SQL engine with lexer, parser, planner, and executor
- MVCC (Multi-Version Concurrency Control) transaction support
- WiscKey LSM-tree storage backend (LevelDB-based)
- Raft consensus via NuRaft for distributed replication
- Web dashboard (httpbeast + HTMX + Shoelace)
- Binary wire protocol with streaming result sets

## Prerequisites

- **Nim** >= 2.0.0
- **LevelDB** (install via package manager)
- **CMake** >= 3.10 (for building NuRaft C++ wrapper)
- **OpenSSL** development libraries
- **C++ compiler** with C++17 support

### macOS

```bash
brew install nim leveldb cmake openssl
```

### Linux (Ubuntu/Debian)

```bash
sudo apt-get install nim cmake libleveldb-dev libssl-dev
```

## Building

### 1. Clone with submodules

```bash
git clone --recurse-submodules https://github.com/vanio79/fractio.git
cd fractio
```

If you already cloned without submodules:

```bash
git submodule update --init --recursive
```

### 2. Build NuRaft wrapper

```bash
cd thirdparty/NuRaft
mkdir -p build && cd build
cmake ..
make -j$(nproc)
cd ../../..

cd src/fractio/distributed/raft/wrapper
make
cd ../../../../../..
```

### 3. Install Nim dependencies

```bash
nimble install
```

### 4. Build the project

```bash
nim c --mm:atomicArc --threads:on -p:src -o:bin/fractio src/fractio/cli/main.nim
```

Or use nimble:

```bash
nimble build
```

## Running Tests

```bash
# All unit tests
nimble test_unit

# All integration tests
nimble test_integration

# Everything
nimble test
```

## Project Structure

```
src/fractio/
  core/         # Types, errors, MVCC transactions, 2PC
  storage/      # WiscKey backend, MVCC engine, GC
  distributed/  # NuRaft coordinator, multi-group Raft, shared timer
  protocol/     # Wire format, client/server, message types
  sql/          # Lexer, parser, planner, executor
  network/      # TCP transport, connection pool
  web/          # HTTP dashboard
  client/       # Fractio client, routing, SQL client
  utils/        # Logging, binary serialization
  cli/          # Command-line interface
```

## License

MIT
