//! fjall (Rust) storage benchmark
//!
//! Measures performance of:
//! - Sequential writes
//! - Random writes
//! - Sequential reads
//! - Random reads
//! - Range scans
//! - Deletions
//! - Mixed workload

use std::env;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use fjall::config::CompressionPolicy;
use fjall::{CompressionType, Database, KeyspaceCreateOptions};
use rand::rngs::StdRng;
use rand::{seq::SliceRandom, SeedableRng};

/// Benchmark result for a single operation type
#[derive(Debug, Clone)]
struct BenchResult {
    name: String,
    total_ops: u64,
    duration_ms: u64,
    ops_per_sec: f64,
    latency_us: f64,
}

impl BenchResult {
    fn new(name: &str, total_ops: u64, duration: Duration) -> Self {
        let duration_ms = duration.as_millis() as u64;
        let duration_secs = duration.as_secs_f64();
        let ops_per_sec = if duration_secs > 0.0 {
            total_ops as f64 / duration_secs
        } else {
            0.0
        };
        let latency_us = if total_ops > 0 {
            (duration.as_micros() as f64) / (total_ops as f64)
        } else {
            0.0
        };

        BenchResult {
            name: name.to_string(),
            total_ops,
            duration_ms,
            ops_per_sec,
            latency_us,
        }
    }

    fn print(&self) {
        println!(
            "  {:<25} {:>10} ops in {:>6} ms | {:>12.2} ops/s | {:>8.2} us/op",
            self.name, self.total_ops, self.duration_ms, self.ops_per_sec, self.latency_us
        );
    }
}

/// Generate a key of the specified size
fn make_key(prefix: &str, i: u64, key_size: usize) -> Vec<u8> {
    let suffix = format!("_{}", i);
    let padding_len = key_size.saturating_sub(prefix.len() + suffix.len());
    format!("{}{}{}", prefix, "k".repeat(padding_len), suffix).into_bytes()
}

/// Generate a value of the specified size
fn make_value(value_size: usize) -> Vec<u8> {
    vec![b'v'; value_size]
}

/// Benchmark configuration
struct Config {
    num_ops: u64,
    key_size: usize,
    value_size: usize,
    batch_size: usize,
    db_path: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            num_ops: 100_000,
            key_size: 16,
            value_size: 100,
            batch_size: 1000,
            db_path: "/tmp/bench_fjall".to_string(),
        }
    }
}

fn main() -> fjall::Result<()> {
    let args: Vec<String> = env::args().collect();

    let mut config = Config::default();

    // Parse command-line arguments
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--ops" | "-n" => {
                config.num_ops = args[i + 1].parse().unwrap_or(config.num_ops);
                i += 2;
            }
            "--key-size" | "-k" => {
                config.key_size = args[i + 1].parse().unwrap_or(config.key_size);
                i += 2;
            }
            "--value-size" | "-v" => {
                config.value_size = args[i + 1].parse().unwrap_or(config.value_size);
                i += 2;
            }
            "--path" | "-p" => {
                config.db_path = args[i + 1].clone();
                i += 2;
            }
            "--help" | "-h" => {
                println!("fjall benchmark");
                println!();
                println!("Usage: fjall_bench [OPTIONS]");
                println!();
                println!("Options:");
                println!("  --ops, -n <NUM>       Number of operations (default: 100000)");
                println!("  --key-size, -k <NUM>  Key size in bytes (default: 16)");
                println!("  --value-size, -v <NUM> Value size in bytes (default: 100)");
                println!("  --path, -p <PATH>     Database path (default: /tmp/bench_fjall)");
                return Ok(());
            }
            _ => {
                i += 1;
            }
        }
    }

    println!("=== fjall (Rust) Storage Benchmark ===");
    println!();
    println!("Configuration:");
    println!("  Operations:  {}", config.num_ops);
    println!("  Key size:    {} bytes", config.key_size);
    println!("  Value size:  {} bytes", config.value_size);
    println!("  DB path:     {}", config.db_path);
    println!();

    // Clean up existing database
    if Path::new(&config.db_path).exists() {
        fs::remove_dir_all(&config.db_path).expect("Failed to remove existing database");
    }

    let mut results: Vec<BenchResult> = Vec::new();

    // Create database
    let db = Database::builder(&config.db_path).open()?;

    // Disable compression for fair comparison with Fractio (which has no compression)
    let ks_opts = KeyspaceCreateOptions::default()
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None));

    let ks = db.keyspace("default", || ks_opts)?;

    // =========================================================================
    // Benchmark 1: Sequential Writes
    // =========================================================================
    println!("Running sequential write benchmark...");

    let start = Instant::now();
    for i in 0..config.num_ops {
        let key = make_key("seq", i, config.key_size);
        let value = make_value(config.value_size);
        ks.insert(&key, &value)?;
    }
    let duration = start.elapsed();
    let result = BenchResult::new("sequential_writes", config.num_ops, duration);
    result.print();
    results.push(result);

    // =========================================================================
    // Benchmark 2: Random Writes
    // =========================================================================
    println!("Running random write benchmark...");

    let mut rng = StdRng::seed_from_u64(42);
    let mut indices: Vec<u64> = (0..config.num_ops).collect();
    indices.shuffle(&mut rng);

    let start = Instant::now();
    for &i in &indices {
        let key = make_key("rand", i, config.key_size);
        let value = make_value(config.value_size);
        ks.insert(&key, &value)?;
    }
    let duration = start.elapsed();
    let result = BenchResult::new("random_writes", config.num_ops, duration);
    result.print();
    results.push(result);

    // =========================================================================
    // Benchmark 3: Sequential Reads
    // =========================================================================
    println!("Running sequential read benchmark...");

    let start = Instant::now();
    for i in 0..config.num_ops {
        let key = make_key("seq", i, config.key_size);
        let _ = ks.get(&key)?;
    }
    let duration = start.elapsed();
    let result = BenchResult::new("sequential_reads", config.num_ops, duration);
    result.print();
    results.push(result);

    // =========================================================================
    // Benchmark 4: Random Reads
    // =========================================================================
    println!("Running random read benchmark...");

    let start = Instant::now();
    for &i in &indices {
        let key = make_key("rand", i, config.key_size);
        let _ = ks.get(&key)?;
    }
    let duration = start.elapsed();
    let result = BenchResult::new("random_reads", config.num_ops, duration);
    result.print();
    results.push(result);

    // =========================================================================
    // Benchmark 5: Range Scan
    // =========================================================================
    println!("Running range scan benchmark...");

    // Insert sequential keys for range scan
    let scan_count = 10_000.min(config.num_ops);
    for i in 0..scan_count {
        let key = format!("scan_{:08}", i);
        let value = make_value(config.value_size);
        ks.insert(key.as_bytes(), &value)?;
    }

    let start_key = "scan_00000000";
    let end_key = "scan_00009999";

    let start = Instant::now();
    let mut scanned = 0u64;
    for item in ks.range(start_key..=end_key) {
        let _ = item;
        scanned += 1;
    }
    let duration = start.elapsed();
    let result = BenchResult::new("range_scan", scanned, duration);
    result.print();
    results.push(result);

    // =========================================================================
    // Benchmark 6: Prefix Scan
    // =========================================================================
    println!("Running prefix scan benchmark...");

    let start = Instant::now();
    let mut scanned = 0u64;
    for item in ks.prefix("scan_") {
        let _ = item;
        scanned += 1;
    }
    let duration = start.elapsed();
    let result = BenchResult::new("prefix_scan", scanned, duration);
    result.print();
    results.push(result);

    // =========================================================================
    // Benchmark 7: Deletions
    // =========================================================================
    println!("Running deletion benchmark...");

    let delete_count = config.num_ops / 2;
    let start = Instant::now();
    for i in 0..delete_count {
        let key = make_key("seq", i, config.key_size);
        ks.remove(&key)?;
    }
    let duration = start.elapsed();
    let result = BenchResult::new("deletions", delete_count, duration);
    result.print();
    results.push(result);

    // =========================================================================
    // Benchmark 8: Batch Writes
    // =========================================================================
    println!("Running batch write benchmark...");

    let batch_ops = config.num_ops / 10; // Smaller for batch test
    let start = Instant::now();
    let mut batch_ops_count = 0u64;

    for batch_start in (0..batch_ops).step_by(config.batch_size) {
        let batch_end = (batch_start + config.batch_size as u64).min(batch_ops);
        let mut wb = db.batch();

        for i in batch_start..batch_end {
            let key = make_key("batch", i, config.key_size);
            let value = make_value(config.value_size);
            wb.insert(&ks, &key, &value);
            batch_ops_count += 1;
        }

        wb.commit()?;
    }

    let duration = start.elapsed();
    let result = BenchResult::new("batch_writes", batch_ops_count, duration);
    result.print();
    results.push(result);

    // =========================================================================
    // Benchmark 9: Contains Key
    // =========================================================================
    println!("Running contains_key benchmark...");

    let start = Instant::now();
    for i in 0..config.num_ops {
        let key = make_key("rand", i, config.key_size);
        let _ = ks.contains_key(&key)?;
    }
    let duration = start.elapsed();
    let result = BenchResult::new("contains_key", config.num_ops, duration);
    result.print();
    results.push(result);

    // =========================================================================
    // Summary
    // =========================================================================
    println!();
    println!("=== Summary ===");
    println!();
    println!(
        "{:<25} {:>12} {:>15} {:>12}",
        "Benchmark", "Total Ops", "Ops/sec", "Latency (us)"
    );
    println!("{}", "-".repeat(65));
    for r in &results {
        println!(
            "{:<25} {:>12} {:>15.2} {:>12.2}",
            r.name, r.total_ops, r.ops_per_sec, r.latency_us
        );
    }

    // Output results in JSON format for comparison
    println!();
    println!("=== JSON Output ===");
    println!("{{");
    println!("  \"engine\": \"fjall\",");
    println!("  \"config\": {{");
    println!("    \"num_ops\": {},", config.num_ops);
    println!("    \"key_size\": {},", config.key_size);
    println!("    \"value_size\": {}", config.value_size);
    println!("  }},");
    println!("  \"results\": {{");
    for (i, r) in results.iter().enumerate() {
        let comma = if i < results.len() - 1 { "," } else { "" };
        println!(
            "    \"{}\": {{ \"ops\": {}, \"ops_per_sec\": {:.2}, \"latency_us\": {:.2} }}{}",
            r.name, r.total_ops, r.ops_per_sec, r.latency_us, comma
        );
    }
    println!("  }}");
    println!("}}");

    // Clean up
    drop(ks);
    drop(db);
    fs::remove_dir_all(&config.db_path).ok();

    Ok(())
}
