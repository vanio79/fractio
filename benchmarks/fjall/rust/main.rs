//! fjall (Rust) storage benchmark with comprehensive metrics
//!
//! Measures:
//! - Latency (avg, p50, p95, p99)
//! - Throughput (ops/sec)
//! - CPU usage
//! - Memory usage (RSS, peak)
//! - Disk space usage
//! - IO saturation

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use fjall::config::CompressionPolicy;
use fjall::{CompressionType, Database, KeyspaceCreateOptions};
use rand::rngs::StdRng;
use rand::{seq::SliceRandom, SeedableRng};

/// System resource metrics
#[derive(Debug, Clone, Default)]
struct ResourceMetrics {
    cpu_user_ms: u64,
    cpu_system_ms: u64,
    cpu_total_ms: u64,
    memory_rss_kb: u64,
    memory_peak_kb: u64,
    disk_read_bytes: u64,
    disk_write_bytes: u64,
    disk_read_ops: u64,
    disk_write_ops: u64,
}

impl ResourceMetrics {
    fn read() -> Self {
        let mut metrics = ResourceMetrics::default();

        // Read /proc/self/stat for CPU time
        if let Ok(stat) = fs::read_to_string("/proc/self/stat") {
            let parts: Vec<&str> = stat.split_whitespace().collect();
            if parts.len() >= 17 {
                metrics.cpu_user_ms = parts[13].parse().unwrap_or(0) * 1000 / 100; // jiffies to ms
                metrics.cpu_system_ms = parts[14].parse().unwrap_or(0) * 1000 / 100;
                metrics.cpu_total_ms = metrics.cpu_user_ms + metrics.cpu_system_ms;
            }
        }

        // Read /proc/self/status for memory
        if let Ok(status) = fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    let kb: u64 = line
                        .split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    metrics.memory_rss_kb = kb;
                } else if line.starts_with("VmHWM:") {
                    let kb: u64 = line
                        .split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    metrics.memory_peak_kb = kb;
                }
            }
        }

        // Read /proc/self/io for IO stats
        if let Ok(io) = fs::read_to_string("/proc/self/io") {
            for line in io.lines() {
                if line.starts_with("read_bytes:") {
                    metrics.disk_read_bytes = line
                        .split(':')
                        .nth(1)
                        .and_then(|s| s.trim().parse().ok())
                        .unwrap_or(0);
                } else if line.starts_with("write_bytes:") {
                    metrics.disk_write_bytes = line
                        .split(':')
                        .nth(1)
                        .and_then(|s| s.trim().parse().ok())
                        .unwrap_or(0);
                } else if line.starts_with("syscr:") {
                    metrics.disk_read_ops = line
                        .split(':')
                        .nth(1)
                        .and_then(|s| s.trim().parse().ok())
                        .unwrap_or(0);
                } else if line.starts_with("syscw:") {
                    metrics.disk_write_ops = line
                        .split(':')
                        .nth(1)
                        .and_then(|s| s.trim().parse().ok())
                        .unwrap_or(0);
                }
            }
        }

        metrics
    }

    fn diff(&self, other: &ResourceMetrics) -> ResourceMetrics {
        ResourceMetrics {
            cpu_user_ms: self.cpu_user_ms.saturating_sub(other.cpu_user_ms),
            cpu_system_ms: self.cpu_system_ms.saturating_sub(other.cpu_system_ms),
            cpu_total_ms: self.cpu_total_ms.saturating_sub(other.cpu_total_ms),
            memory_rss_kb: self.memory_rss_kb,
            memory_peak_kb: self.memory_peak_kb,
            disk_read_bytes: self.disk_read_bytes.saturating_sub(other.disk_read_bytes),
            disk_write_bytes: self.disk_write_bytes.saturating_sub(other.disk_write_bytes),
            disk_read_ops: self.disk_read_ops.saturating_sub(other.disk_read_ops),
            disk_write_ops: self.disk_write_ops.saturating_sub(other.disk_write_ops),
        }
    }
}

/// Latency histogram for percentile calculation
#[derive(Debug, Clone)]
struct LatencyHistogram {
    samples: Vec<u64>, // in microseconds
}

impl LatencyHistogram {
    fn new() -> Self {
        LatencyHistogram {
            samples: Vec::new(),
        }
    }

    fn record(&mut self, latency_us: u64) {
        self.samples.push(latency_us);
    }

    fn percentile(&self, p: f64) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    fn avg(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        self.samples.iter().sum::<u64>() as f64 / self.samples.len() as f64
    }

    fn min(&self) -> u64 {
        self.samples.iter().copied().min().unwrap_or(0)
    }

    fn max(&self) -> u64 {
        self.samples.iter().copied().max().unwrap_or(0)
    }
}

/// Comprehensive benchmark result
#[derive(Debug, Clone)]
struct BenchResult {
    name: String,
    total_ops: u64,
    duration_ms: u64,
    ops_per_sec: f64,
    latency_avg_us: f64,
    latency_p50_us: u64,
    latency_p95_us: u64,
    latency_p99_us: u64,
    latency_min_us: u64,
    latency_max_us: u64,
    cpu_user_ms: u64,
    cpu_system_ms: u64,
    cpu_total_ms: u64,
    cpu_percent: f64,
    memory_rss_mb: f64,
    memory_peak_mb: f64,
    disk_read_mb: f64,
    disk_write_mb: f64,
    disk_read_ops: u64,
    disk_write_ops: u64,
    iops: f64,
    disk_total_mb: f64,
    throughput_mb_per_sec: f64,
    disk_read_bytes: u64,
    disk_write_bytes: u64,
}

impl BenchResult {
    fn print(&self) {
        println!(
            "  {:<25} {:>10} ops in {:>6} ms | {:>12.2} ops/s",
            self.name, self.total_ops, self.duration_ms, self.ops_per_sec
        );
        println!(
            "    Latency: avg={:.2}us p50={}us p95={}us p99={}us [min={} max={}]",
            self.latency_avg_us,
            self.latency_p50_us,
            self.latency_p95_us,
            self.latency_p99_us,
            self.latency_min_us,
            self.latency_max_us
        );
        println!(
            "    CPU: user={}ms sys={}ms total={}ms ({:.1}%)",
            self.cpu_user_ms, self.cpu_system_ms, self.cpu_total_ms, self.cpu_percent
        );
        println!(
            "    Memory: RSS={:.2}MB Peak={:.2}MB",
            self.memory_rss_mb, self.memory_peak_mb
        );
        println!(
            "    Disk: read={:.2}MB write={:.2}MB IOPS={:.0} throughput={:.2}MB/s",
            self.disk_read_mb, self.disk_write_mb, self.iops, self.throughput_mb_per_sec
        );
    }

    fn to_json(&self) -> String {
        format!(
            r#"{{ "ops": {}, "ops_per_sec": {:.2}, "latency_us": {:.2}, "cpu_percent": {:.2}, "memory_mb": {:.2}, "disk_mb": {:.2}, "disk_read_bytes": {}, "disk_write_bytes": {} }}"#,
            self.total_ops,
            self.ops_per_sec,
            self.latency_avg_us,
            self.cpu_percent,
            self.memory_rss_mb,
            self.disk_total_mb,
            self.disk_read_bytes,
            self.disk_write_bytes
        )
    }
}

/// Get actual disk usage in bytes (using block count, not file size)
/// This properly accounts for sparse files
#[cfg(unix)]
fn get_dir_disk_usage(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    let mut total = 0u64;
    if path.exists() {
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata() {
                    if metadata.is_file() {
                        // Use actual block count * 512 (standard block size)
                        total += metadata.blocks() * 512;
                    } else if metadata.is_dir() {
                        total += get_dir_disk_usage(&entry.path());
                    }
                }
            }
        }
    }
    total
}

#[cfg(not(unix))]
fn get_dir_disk_usage(path: &Path) -> u64 {
    // Fallback for non-Unix: use file size
    let mut total = 0u64;
    if path.exists() {
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata() {
                    if metadata.is_file() {
                        total += metadata.len();
                    } else if metadata.is_dir() {
                        total += get_dir_disk_usage(&entry.path());
                    }
                }
            }
        }
    }
    total
}

/// Legacy function for file size (kept for reference)
fn get_dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    if path.exists() {
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata() {
                    if metadata.is_file() {
                        total += metadata.len();
                    } else if metadata.is_dir() {
                        total += get_dir_size(&entry.path());
                    }
                }
            }
        }
    }
    total
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
                println!("fjall benchmark with comprehensive metrics");
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
    let ks_opts = KeyspaceCreateOptions::default()
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None));
    let ks = db.keyspace("default", || ks_opts)?;

    // Track initial resources
    let initial_resources = ResourceMetrics::read();
    let mut peak_memory_kb = 0u64;

    // Helper to create result with metrics
    let make_result = |name: String,
                       total_ops: u64,
                       duration: Duration,
                       latency: LatencyHistogram,
                       start_res: ResourceMetrics,
                       value_size: usize|
     -> BenchResult {
        let end_res = ResourceMetrics::read();
        let diff = end_res.diff(&start_res);
        let duration_secs = duration.as_secs_f64();
        let duration_ms = duration.as_millis() as u64;

        let cpu_percent = if duration_ms > 0 {
            (diff.cpu_total_ms as f64 / duration_ms as f64) * 100.0
        } else {
            0.0
        };

        let disk_total_mb =
            (diff.disk_read_bytes + diff.disk_write_bytes) as f64 / (1024.0 * 1024.0);
        let throughput_mb = if duration_secs > 0.0 {
            disk_total_mb / duration_secs
        } else {
            0.0
        };

        let iops = if duration_secs > 0.0 {
            (diff.disk_read_ops + diff.disk_write_ops) as f64 / duration_secs
        } else {
            0.0
        };

        BenchResult {
            name,
            total_ops,
            duration_ms,
            ops_per_sec: if duration_secs > 0.0 {
                total_ops as f64 / duration_secs
            } else {
                0.0
            },
            latency_avg_us: latency.avg(),
            latency_p50_us: latency.percentile(50.0),
            latency_p95_us: latency.percentile(95.0),
            latency_p99_us: latency.percentile(99.0),
            latency_min_us: latency.min(),
            latency_max_us: latency.max(),
            cpu_user_ms: diff.cpu_user_ms,
            cpu_system_ms: diff.cpu_system_ms,
            cpu_total_ms: diff.cpu_total_ms,
            cpu_percent,
            memory_rss_mb: end_res.memory_rss_kb as f64 / 1024.0,
            memory_peak_mb: end_res.memory_peak_kb as f64 / 1024.0,
            disk_read_mb: diff.disk_read_bytes as f64 / (1024.0 * 1024.0),
            disk_write_mb: diff.disk_write_bytes as f64 / (1024.0 * 1024.0),
            disk_read_ops: diff.disk_read_ops,
            disk_write_ops: diff.disk_write_ops,
            iops,
            disk_total_mb,
            throughput_mb_per_sec: throughput_mb,
            disk_read_bytes: diff.disk_read_bytes,
            disk_write_bytes: diff.disk_write_bytes,
        }
    };

    // =========================================================================
    // Benchmark 1: Sequential Writes
    // =========================================================================
    println!("Running sequential write benchmark...");
    let mut latency = LatencyHistogram::new();
    let start_res = ResourceMetrics::read();
    let start = Instant::now();

    for i in 0..config.num_ops {
        let op_start = Instant::now();
        let key = make_key("seq", i, config.key_size);
        let value = make_value(config.value_size);
        ks.insert(&key, &value)?;
        latency.record(op_start.elapsed().as_micros() as u64);
    }
    let duration = start.elapsed();
    let result = make_result(
        "sequential_writes".to_string(),
        config.num_ops,
        duration,
        latency,
        start_res,
        config.value_size,
    );
    result.print();
    results.push(result);

    // =========================================================================
    // Benchmark 2: Random Writes
    // =========================================================================
    println!("Running random write benchmark...");
    let mut rng = StdRng::seed_from_u64(42);
    let mut indices: Vec<u64> = (0..config.num_ops).collect();
    indices.shuffle(&mut rng);

    let mut latency = LatencyHistogram::new();
    let start_res = ResourceMetrics::read();
    let start = Instant::now();

    for &i in &indices {
        let op_start = Instant::now();
        let key = make_key("rand", i, config.key_size);
        let value = make_value(config.value_size);
        ks.insert(&key, &value)?;
        latency.record(op_start.elapsed().as_micros() as u64);
    }
    let duration = start.elapsed();
    let result = make_result(
        "random_writes".to_string(),
        config.num_ops,
        duration,
        latency,
        start_res,
        config.value_size,
    );
    result.print();
    results.push(result);

    // =========================================================================
    // Benchmark 3: Sequential Reads
    // =========================================================================
    println!("Running sequential read benchmark...");
    let mut latency = LatencyHistogram::new();
    let start_res = ResourceMetrics::read();
    let start = Instant::now();

    for i in 0..config.num_ops {
        let op_start = Instant::now();
        let key = make_key("seq", i, config.key_size);
        let _ = ks.get(&key)?;
        latency.record(op_start.elapsed().as_micros() as u64);
    }
    let duration = start.elapsed();
    let result = make_result(
        "sequential_reads".to_string(),
        config.num_ops,
        duration,
        latency,
        start_res,
        0,
    );
    result.print();
    results.push(result);

    // =========================================================================
    // Benchmark 4: Random Reads
    // =========================================================================
    println!("Running random read benchmark...");
    let mut latency = LatencyHistogram::new();
    let start_res = ResourceMetrics::read();
    let start = Instant::now();

    for &i in &indices {
        let op_start = Instant::now();
        let key = make_key("rand", i, config.key_size);
        let _ = ks.get(&key)?;
        latency.record(op_start.elapsed().as_micros() as u64);
    }
    let duration = start.elapsed();
    let result = make_result(
        "random_reads".to_string(),
        config.num_ops,
        duration,
        latency,
        start_res,
        0,
    );
    result.print();
    results.push(result);

    // =========================================================================
    // Benchmark 5: Range Scan
    // =========================================================================
    println!("Running range scan benchmark...");
    let scan_count = 10_000.min(config.num_ops);
    for i in 0..scan_count {
        let key = format!("scan_{:08}", i);
        let value = make_value(config.value_size);
        ks.insert(key.as_bytes(), &value)?;
    }

    let start_key = "scan_00000000";
    let end_key = "scan_00009999";
    let mut latency = LatencyHistogram::new();
    let start_res = ResourceMetrics::read();
    let start = Instant::now();

    let mut scanned = 0u64;
    let op_start = Instant::now();
    for item in ks.range(start_key..=end_key) {
        let _ = item;
        scanned += 1;
    }
    latency.record(op_start.elapsed().as_micros() as u64);

    let duration = start.elapsed();
    let result = make_result(
        "range_scan".to_string(),
        scanned,
        duration,
        latency,
        start_res,
        config.value_size,
    );
    result.print();
    results.push(result);

    // =========================================================================
    // Benchmark 6: Prefix Scan
    // =========================================================================
    println!("Running prefix scan benchmark...");
    let mut latency = LatencyHistogram::new();
    let start_res = ResourceMetrics::read();
    let start = Instant::now();

    let mut scanned = 0u64;
    let op_start = Instant::now();
    for item in ks.prefix("scan_") {
        let _ = item;
        scanned += 1;
    }
    latency.record(op_start.elapsed().as_micros() as u64);

    let duration = start.elapsed();
    let result = make_result(
        "prefix_scan".to_string(),
        scanned,
        duration,
        latency,
        start_res,
        config.value_size,
    );
    result.print();
    results.push(result);

    // =========================================================================
    // Benchmark 7: Deletions
    // =========================================================================
    println!("Running deletion benchmark...");
    let delete_count = config.num_ops / 2;
    let mut latency = LatencyHistogram::new();
    let start_res = ResourceMetrics::read();
    let start = Instant::now();

    for i in 0..delete_count {
        let op_start = Instant::now();
        let key = make_key("seq", i, config.key_size);
        ks.remove(&key)?;
        latency.record(op_start.elapsed().as_micros() as u64);
    }
    let duration = start.elapsed();
    let result = make_result(
        "deletions".to_string(),
        delete_count,
        duration,
        latency,
        start_res,
        0,
    );
    result.print();
    results.push(result);

    // =========================================================================
    // Benchmark 8: Batch Writes
    // =========================================================================
    println!("Running batch write benchmark...");
    let batch_ops = config.num_ops / 10;
    let mut latency = LatencyHistogram::new();
    let start_res = ResourceMetrics::read();
    let start = Instant::now();

    let mut batch_ops_count = 0u64;
    for batch_start in (0..batch_ops).step_by(config.batch_size) {
        let batch_end = (batch_start + config.batch_size as u64).min(batch_ops);
        let mut wb = db.batch();
        let op_start = Instant::now();

        for i in batch_start..batch_end {
            let key = make_key("batch", i, config.key_size);
            let value = make_value(config.value_size);
            wb.insert(&ks, &key, &value);
            batch_ops_count += 1;
        }

        wb.commit()?;
        latency.record(op_start.elapsed().as_micros() as u64);
    }

    let duration = start.elapsed();
    let result = make_result(
        "batch_writes".to_string(),
        batch_ops_count,
        duration,
        latency,
        start_res,
        config.value_size,
    );
    result.print();
    results.push(result);

    // =========================================================================
    // Benchmark 9: Contains Key
    // =========================================================================
    println!("Running contains_key benchmark...");
    let mut latency = LatencyHistogram::new();
    let start_res = ResourceMetrics::read();
    let start = Instant::now();

    for i in 0..config.num_ops {
        let op_start = Instant::now();
        let key = make_key("rand", i, config.key_size);
        let _ = ks.contains_key(&key)?;
        latency.record(op_start.elapsed().as_micros() as u64);
    }
    let duration = start.elapsed();
    let result = make_result(
        "contains_key".to_string(),
        config.num_ops,
        duration,
        latency,
        start_res,
        0,
    );
    result.print();
    results.push(result);

    // =========================================================================
    // Disk Space Summary
    // =========================================================================
    let disk_size = get_dir_disk_usage(Path::new(&config.db_path)); // Use actual disk blocks
    println!();
    println!(
        "Disk Space Usage (actual blocks): {:.2} MB",
        disk_size as f64 / (1024.0 * 1024.0)
    );

    // =========================================================================
    // JSON Output
    // =========================================================================
    println!();
    println!("=== JSON Output ===");
    println!("{{");
    println!("  \"engine\": \"fjall\",");
    println!("  \"config\": {{");
    println!("    \"num_ops\": {},", config.num_ops);
    println!("    \"key_size\": {},", config.key_size);
    println!("    \"value_size\": {}", config.value_size);
    println!("  }},");
    println!(
        "  \"disk_space_mb\": {:.2},",
        disk_size as f64 / (1024.0 * 1024.0)
    );
    println!("  \"results\": {{");
    for (i, r) in results.iter().enumerate() {
        let comma = if i < results.len() - 1 { "," } else { "" };
        println!("    \"{}\": {}{}", r.name, r.to_json(), comma);
    }
    println!("  }}");
    println!("}}");

    // Clean up
    drop(ks);
    drop(db);
    fs::remove_dir_all(&config.db_path).ok();

    Ok(())
}
