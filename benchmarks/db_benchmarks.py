#!/usr/bin/env python3
"""
Transactional Benchmarks for PostgreSQL, MySQL, and SQLite
Compares sequential and concurrent workloads
"""

import os
import time
import threading
import argparse
from dataclasses import dataclass
from typing import List, Optional
import statistics
import random

# Database drivers
try:
    import psycopg2

    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False

try:
    import mysql.connector

    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False

try:
    import sqlite3

    HAS_SQLITE = True
except ImportError:
    HAS_SQLITE = False

# =============================================================================
# Configuration
# =============================================================================


@dataclass
class BenchmarkConfig:
    num_keys: int = 5000
    num_ops: int = 1000
    num_threads: int = 4
    value_size: int = 100
    warmup_ops: int = 100


@dataclass
class BenchmarkResult:
    name: str
    ops_per_sec: float
    avg_latency_us: float
    min_latency_us: float
    max_latency_us: float
    total_ops: int
    errors: int


# =============================================================================
# Key/Value Generation
# =============================================================================


def make_key(id: int) -> str:
    return f"key_{id}"


def make_value(size: int) -> str:
    return "x" * size


# =============================================================================
# PostgreSQL Benchmark
# =============================================================================


class PostgresBenchmark:
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.conn_params = {
            "host": "localhost",
            "database": "fractio_bench",
            "user": "postgres",
            "password": "benchmark",
        }

    def setup(self):
        """Create table and seed data"""
        conn = psycopg2.connect(**self.conn_params)
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS kv_store")
        cur.execute("""
            CREATE TABLE kv_store (
                key VARCHAR(255) PRIMARY KEY,
                value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

        # Seed data
        value = make_value(self.config.value_size)
        for i in range(min(self.config.num_keys, 1000)):
            cur.execute(
                "INSERT INTO kv_store (key, value) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (make_key(i), value),
            )
        conn.commit()
        cur.close()
        conn.close()

    def cleanup(self):
        """Drop table"""
        conn = psycopg2.connect(**self.conn_params)
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS kv_store")
        conn.commit()
        cur.close()
        conn.close()

    def sequential_benchmark(self) -> BenchmarkResult:
        """Run sequential benchmark"""
        latencies = []
        errors = 0
        total_ops = 0

        conn = psycopg2.connect(**self.conn_params)
        cur = conn.cursor()
        value = make_value(self.config.value_size)

        # Warmup
        for i in range(self.config.warmup_ops):
            key = make_key(i % self.config.num_keys)
            cur.execute("SELECT value FROM kv_store WHERE key = %s", (key,))

        # Benchmark
        start_time = time.time()

        for i in range(self.config.num_ops):
            op_start = time.perf_counter()

            try:
                key = make_key(i % self.config.num_keys)
                # Mix of reads and writes
                if i % 3 == 0:
                    cur.execute(
                        "INSERT INTO kv_store (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s",
                        (key, value, value),
                    )
                    conn.commit()
                else:
                    cur.execute("SELECT value FROM kv_store WHERE key = %s", (key,))
                    cur.fetchone()

                total_ops += 1
            except Exception as e:
                errors += 1

            latencies.append((time.perf_counter() - op_start) * 1_000_000)

        duration = time.time() - start_time

        cur.close()
        conn.close()

        return BenchmarkResult(
            name="PostgreSQL Sequential",
            ops_per_sec=total_ops / duration if duration > 0 else 0,
            avg_latency_us=statistics.mean(latencies) if latencies else 0,
            min_latency_us=min(latencies) if latencies else 0,
            max_latency_us=max(latencies) if latencies else 0,
            total_ops=total_ops,
            errors=errors,
        )

    def _worker(self, thread_id: int, num_ops: int, results: dict, errors: dict):
        """Worker thread for concurrent benchmark"""
        conn = psycopg2.connect(**self.conn_params)
        cur = conn.cursor()
        value = make_value(self.config.value_size)

        local_latencies = []
        local_errors = 0
        local_ops = 0

        for i in range(num_ops):
            op_start = time.perf_counter()

            try:
                key = make_key((thread_id * num_ops + i) % self.config.num_keys)
                if i % 3 == 0:
                    cur.execute(
                        "INSERT INTO kv_store (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s",
                        (key, value, value),
                    )
                    conn.commit()
                else:
                    cur.execute("SELECT value FROM kv_store WHERE key = %s", (key,))
                    cur.fetchone()
                local_ops += 1
            except Exception as e:
                local_errors += 1

            local_latencies.append((time.perf_counter() - op_start) * 1_000_000)

        cur.close()
        conn.close()

        results[thread_id] = local_latencies
        errors[thread_id] = local_errors

    def concurrent_benchmark(self, num_threads: int) -> BenchmarkResult:
        """Run concurrent benchmark"""
        results = {}
        errors = {}
        threads = []

        ops_per_thread = self.config.num_ops // num_threads

        start_time = time.time()

        for t in range(num_threads):
            thread = threading.Thread(
                target=self._worker, args=(t, ops_per_thread, results, errors)
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        duration = time.time() - start_time

        all_latencies = []
        for l in results.values():
            all_latencies.extend(l)

        total_errors = sum(errors.values())
        total_ops = len(all_latencies)

        return BenchmarkResult(
            name=f"PostgreSQL Concurrent-{num_threads}t",
            ops_per_sec=total_ops / duration if duration > 0 else 0,
            avg_latency_us=statistics.mean(all_latencies) if all_latencies else 0,
            min_latency_us=min(all_latencies) if all_latencies else 0,
            max_latency_us=max(all_latencies) if all_latencies else 0,
            total_ops=total_ops,
            errors=total_errors,
        )


# =============================================================================
# MySQL Benchmark
# =============================================================================


class MySQLBenchmark:
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.conn_params = {
            "host": "localhost",
            "database": "fractio_bench",
            "user": "benchmark",
            "password": "benchmark",
        }

    def setup(self):
        """Create table and seed data"""
        conn = mysql.connector.connect(**self.conn_params)
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS kv_store")
        cur.execute("""
            CREATE TABLE kv_store (
                `key` VARCHAR(255) PRIMARY KEY,
                `value` TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        """)

        # Seed data
        value = make_value(self.config.value_size)
        for i in range(min(self.config.num_keys, 1000)):
            cur.execute(
                "INSERT IGNORE INTO kv_store (`key`, `value`) VALUES (%s, %s)",
                (make_key(i), value),
            )
        conn.commit()
        cur.close()
        conn.close()

    def cleanup(self):
        """Drop table"""
        conn = mysql.connector.connect(**self.conn_params)
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS kv_store")
        conn.commit()
        cur.close()
        conn.close()

    def sequential_benchmark(self) -> BenchmarkResult:
        """Run sequential benchmark"""
        latencies = []
        errors = 0
        total_ops = 0

        conn = mysql.connector.connect(**self.conn_params)
        cur = conn.cursor()
        value = make_value(self.config.value_size)

        # Warmup
        for i in range(self.config.warmup_ops):
            key = make_key(i % self.config.num_keys)
            cur.execute("SELECT `value` FROM kv_store WHERE `key` = %s", (key,))
            cur.fetchone()  # Must consume result for MySQL

        # Benchmark
        start_time = time.time()

        for i in range(self.config.num_ops):
            op_start = time.perf_counter()

            try:
                key = make_key(i % self.config.num_keys)
                if i % 3 == 0:
                    cur.execute(
                        "INSERT INTO kv_store (`key`, `value`) VALUES (%s, %s) ON DUPLICATE KEY UPDATE `value` = %s",
                        (key, value, value),
                    )
                    conn.commit()
                else:
                    cur.execute("SELECT `value` FROM kv_store WHERE `key` = %s", (key,))
                    cur.fetchone()
                total_ops += 1
            except Exception as e:
                errors += 1

            latencies.append((time.perf_counter() - op_start) * 1_000_000)

        duration = time.time() - start_time

        cur.close()
        conn.close()

        return BenchmarkResult(
            name="MySQL Sequential",
            ops_per_sec=total_ops / duration if duration > 0 else 0,
            avg_latency_us=statistics.mean(latencies) if latencies else 0,
            min_latency_us=min(latencies) if latencies else 0,
            max_latency_us=max(latencies) if latencies else 0,
            total_ops=total_ops,
            errors=errors,
        )

    def _worker(self, thread_id: int, num_ops: int, results: dict, errors: dict):
        """Worker thread for concurrent benchmark"""
        conn = mysql.connector.connect(**self.conn_params)
        cur = conn.cursor()
        value = make_value(self.config.value_size)

        local_latencies = []
        local_errors = 0
        local_ops = 0

        for i in range(num_ops):
            op_start = time.perf_counter()

            try:
                key = make_key((thread_id * num_ops + i) % self.config.num_keys)
                if i % 3 == 0:
                    cur.execute(
                        "INSERT INTO kv_store (`key`, `value`) VALUES (%s, %s) ON DUPLICATE KEY UPDATE `value` = %s",
                        (key, value, value),
                    )
                    conn.commit()
                else:
                    cur.execute("SELECT `value` FROM kv_store WHERE `key` = %s", (key,))
                    cur.fetchone()
                local_ops += 1
            except Exception as e:
                local_errors += 1

            local_latencies.append((time.perf_counter() - op_start) * 1_000_000)

        cur.close()
        conn.close()

        results[thread_id] = local_latencies
        errors[thread_id] = local_errors

    def concurrent_benchmark(self, num_threads: int) -> BenchmarkResult:
        """Run concurrent benchmark"""
        results = {}
        errors = {}
        threads = []

        ops_per_thread = self.config.num_ops // num_threads

        start_time = time.time()

        for t in range(num_threads):
            thread = threading.Thread(
                target=self._worker, args=(t, ops_per_thread, results, errors)
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        duration = time.time() - start_time

        all_latencies = []
        for l in results.values():
            all_latencies.extend(l)

        total_errors = sum(errors.values())
        total_ops = len(all_latencies)

        return BenchmarkResult(
            name=f"MySQL Concurrent-{num_threads}t",
            ops_per_sec=total_ops / duration if duration > 0 else 0,
            avg_latency_us=statistics.mean(all_latencies) if all_latencies else 0,
            min_latency_us=min(all_latencies) if all_latencies else 0,
            max_latency_us=max(all_latencies) if all_latencies else 0,
            total_ops=total_ops,
            errors=total_errors,
        )


# =============================================================================
# SQLite Benchmark
# =============================================================================


class SQLiteBenchmark:
    def __init__(self, config: BenchmarkConfig, db_path: str = "/tmp/fractio_bench.db"):
        self.config = config
        self.db_path = db_path
        self.lock = threading.Lock()

    def setup(self):
        """Create table and seed data"""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE kv_store (
                key TEXT PRIMARY KEY,
                value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Seed data
        value = make_value(self.config.value_size)
        for i in range(min(self.config.num_keys, 1000)):
            cur.execute(
                "INSERT OR IGNORE INTO kv_store (key, value) VALUES (?, ?)",
                (make_key(i), value),
            )
        conn.commit()
        cur.close()
        conn.close()

    def cleanup(self):
        """Remove database file"""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def sequential_benchmark(self) -> BenchmarkResult:
        """Run sequential benchmark"""
        latencies = []
        errors = 0
        total_ops = 0

        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        value = make_value(self.config.value_size)

        # Enable WAL mode for better concurrent performance
        cur.execute("PRAGMA journal_mode=WAL")

        # Warmup
        for i in range(self.config.warmup_ops):
            key = make_key(i % self.config.num_keys)
            cur.execute("SELECT value FROM kv_store WHERE key = ?", (key,))

        # Benchmark
        start_time = time.time()

        for i in range(self.config.num_ops):
            op_start = time.perf_counter()

            try:
                key = make_key(i % self.config.num_keys)
                if i % 3 == 0:
                    cur.execute(
                        "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)",
                        (key, value),
                    )
                    conn.commit()
                else:
                    cur.execute("SELECT value FROM kv_store WHERE key = ?", (key,))
                    cur.fetchone()
                total_ops += 1
            except Exception as e:
                errors += 1

            latencies.append((time.perf_counter() - op_start) * 1_000_000)

        duration = time.time() - start_time

        cur.close()
        conn.close()

        return BenchmarkResult(
            name="SQLite Sequential",
            ops_per_sec=total_ops / duration if duration > 0 else 0,
            avg_latency_us=statistics.mean(latencies) if latencies else 0,
            min_latency_us=min(latencies) if latencies else 0,
            max_latency_us=max(latencies) if latencies else 0,
            total_ops=total_ops,
            errors=errors,
        )

    def _worker(self, thread_id: int, num_ops: int, results: dict, errors: dict):
        """Worker thread for concurrent benchmark"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        value = make_value(self.config.value_size)

        local_latencies = []
        local_errors = 0
        local_ops = 0

        for i in range(num_ops):
            op_start = time.perf_counter()

            try:
                with self.lock:
                    key = make_key((thread_id * num_ops + i) % self.config.num_keys)
                    if i % 3 == 0:
                        cur.execute(
                            "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)",
                            (key, value),
                        )
                        conn.commit()
                    else:
                        cur.execute("SELECT value FROM kv_store WHERE key = ?", (key,))
                        cur.fetchone()
                local_ops += 1
            except Exception as e:
                local_errors += 1

            local_latencies.append((time.perf_counter() - op_start) * 1_000_000)

        cur.close()
        conn.close()

        results[thread_id] = local_latencies
        errors[thread_id] = local_errors

    def concurrent_benchmark(self, num_threads: int) -> BenchmarkResult:
        """Run concurrent benchmark"""
        results = {}
        errors = {}
        threads = []

        ops_per_thread = self.config.num_ops // num_threads

        start_time = time.time()

        for t in range(num_threads):
            thread = threading.Thread(
                target=self._worker, args=(t, ops_per_thread, results, errors)
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        duration = time.time() - start_time

        all_latencies = []
        for l in results.values():
            all_latencies.extend(l)

        total_errors = sum(errors.values())
        total_ops = len(all_latencies)

        return BenchmarkResult(
            name=f"SQLite Concurrent-{num_threads}t",
            ops_per_sec=total_ops / duration if duration > 0 else 0,
            avg_latency_us=statistics.mean(all_latencies) if all_latencies else 0,
            min_latency_us=min(all_latencies) if all_latencies else 0,
            max_latency_us=max(all_latencies) if all_latencies else 0,
            total_ops=total_ops,
            errors=total_errors,
        )


# =============================================================================
# Main
# =============================================================================


def print_results(results: List[BenchmarkResult]):
    """Print benchmark results in a formatted table"""
    print("\n" + "=" * 80)
    print("BENCHMARK RESULTS")
    print("=" * 80)

    print(
        f"\n{'Benchmark':<30} | {'Ops/sec':>12} | {'Avg Lat (μs)':>14} | {'Min (μs)':>10} | {'Max (μs)':>10}"
    )
    print("-" * 80)

    for r in results:
        print(
            f"{r.name:<30} | {r.ops_per_sec:>12.1f} | {r.avg_latency_us:>14.2f} | {r.min_latency_us:>10.2f} | {r.max_latency_us:>10.2f}"
        )


def main():
    parser = argparse.ArgumentParser(description="Database Transaction Benchmarks")
    parser.add_argument("--keys", type=int, default=5000, help="Number of keys")
    parser.add_argument(
        "--ops", type=int, default=1000, help="Operations per benchmark"
    )
    parser.add_argument(
        "--threads", type=int, default=4, help="Number of threads for concurrent"
    )
    parser.add_argument(
        "--value-size", type=int, default=100, help="Value size in bytes"
    )
    parser.add_argument(
        "--skip-postgres", action="store_true", help="Skip PostgreSQL benchmark"
    )
    parser.add_argument(
        "--skip-mysql", action="store_true", help="Skip MySQL benchmark"
    )
    parser.add_argument(
        "--skip-sqlite", action="store_true", help="Skip SQLite benchmark"
    )

    args = parser.parse_args()

    config = BenchmarkConfig(
        num_keys=args.keys,
        num_ops=args.ops,
        num_threads=args.threads,
        value_size=args.value_size,
    )

    print("=" * 80)
    print("DATABASE TRANSACTION BENCHMARKS")
    print("=" * 80)
    print(f"\nConfiguration:")
    print(f"  Keys: {config.num_keys}")
    print(f"  Operations: {config.num_ops}")
    print(f"  Threads: {config.num_threads}")
    print(f"  Value size: {config.value_size} bytes")

    all_results = []

    # PostgreSQL
    if not args.skip_postgres and HAS_POSTGRES:
        print("\n" + "=" * 80)
        print("PostgreSQL Benchmark")
        print("=" * 80)

        pg = PostgresBenchmark(config)
        print("Setting up...")
        pg.setup()

        print("Running sequential benchmark...")
        all_results.append(pg.sequential_benchmark())

        for threads in [2, 4, 8]:
            print(f"Running concurrent benchmark with {threads} threads...")
            all_results.append(pg.concurrent_benchmark(threads))

        print("Cleaning up...")
        pg.cleanup()
    elif not args.skip_postgres:
        print("\nPostgreSQL driver not available (psycopg2)")

    # MySQL
    if not args.skip_mysql and HAS_MYSQL:
        print("\n" + "=" * 80)
        print("MySQL Benchmark")
        print("=" * 80)

        mysql = MySQLBenchmark(config)
        print("Setting up...")
        mysql.setup()

        print("Running sequential benchmark...")
        all_results.append(mysql.sequential_benchmark())

        for threads in [2, 4, 8]:
            print(f"Running concurrent benchmark with {threads} threads...")
            all_results.append(mysql.concurrent_benchmark(threads))

        print("Cleaning up...")
        mysql.cleanup()
    elif not args.skip_mysql:
        print("\nMySQL driver not available (mysql-connector-python)")

    # SQLite
    if not args.skip_sqlite and HAS_SQLITE:
        print("\n" + "=" * 80)
        print("SQLite Benchmark")
        print("=" * 80)

        sqlite = SQLiteBenchmark(config)
        print("Setting up...")
        sqlite.setup()

        print("Running sequential benchmark...")
        all_results.append(sqlite.sequential_benchmark())

        for threads in [2, 4, 8]:
            print(f"Running concurrent benchmark with {threads} threads...")
            all_results.append(sqlite.concurrent_benchmark(threads))

        print("Cleaning up...")
        sqlite.cleanup()
    elif not args.skip_sqlite:
        print("\nSQLite driver not available")

    # Print results
    print_results(all_results)

    print("\nDone!")


if __name__ == "__main__":
    main()
