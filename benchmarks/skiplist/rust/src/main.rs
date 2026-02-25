//! crossbeam-skiplist (Rust) benchmark
//!
//! Measures:
//! - Insert throughput
//! - Lookup throughput
//! - Iteration throughput
//! - Combined insert/remove throughput

use std::time::Instant;

use crossbeam_epoch as epoch;
use crossbeam_skiplist::SkipList;
use rand::Rng;

const NUM_ELEMENTS: usize = 100_000;

fn make_key(i: usize) -> u64 {
    (i as u64).wrapping_mul(17).wrapping_add(255)
}

fn main() {
    println!("=== crossbeam-skiplist (Rust) Benchmark ===");
    println!();
    println!("Elements: {}", NUM_ELEMENTS);
    println!();

    // =========================================================================
    // Benchmark 1: Sequential Insert
    // =========================================================================
    println!("Running sequential insert benchmark...");
    let start = Instant::now();
    let guard = &epoch::pin();

    let mut list = SkipList::new(epoch::default_collector().clone());
    for i in 0..NUM_ELEMENTS {
        let key = make_key(i);
        list.insert(key, !key, guard);
    }
    let duration = start.elapsed();
    let ops_per_sec = NUM_ELEMENTS as f64 / duration.as_secs_f64();
    println!(
        "  sequential_insert: {} ops in {:?} | {:.0} ops/s",
        NUM_ELEMENTS, duration, ops_per_sec
    );

    // =========================================================================
    // Benchmark 2: Sequential Lookup (with value verification)
    // =========================================================================
    println!("Running sequential lookup benchmark...");
    let start = Instant::now();
    let mut found = 0;
    let mut verified = 0;
    for i in 0..NUM_ELEMENTS {
        let key = make_key(i);
        if let Some(val) = list.get(&key, guard) {
            found += 1;
            // Verify value is correct (should be !key)
            if *val.value() == !key {
                verified += 1;
            }
        }
    }
    let duration = start.elapsed();
    let ops_per_sec = NUM_ELEMENTS as f64 / duration.as_secs_f64();
    println!(
        " sequential_lookup: {} ops in {:?} | {:.0} ops/s ({} found, {} verified)",
        NUM_ELEMENTS, duration, ops_per_sec, found, verified
    );

    // =========================================================================
    // Benchmark 3: Random Lookup (with value verification)
    // =========================================================================
    println!("Running random lookup benchmark...");
    let mut rng = rand::thread_rng();
    let keys: Vec<u64> = (0..NUM_ELEMENTS)
        .map(|i| make_key(rng.gen_range(0..NUM_ELEMENTS)))
        .collect();
    let start = Instant::now();
    found = 0;
    let mut verified = 0;
    for key in &keys {
        if let Some(val) = list.get(key, guard) {
            found += 1;
            // Verify value is correct (should be !key)
            if *val.value() == !key {
                verified += 1;
            }
        }
    }
    let duration = start.elapsed();
    let ops_per_sec = NUM_ELEMENTS as f64 / duration.as_secs_f64();
    println!(
        " random_lookup: {} ops in {:?} | {:.0} ops/s ({} found, {} verified)",
        NUM_ELEMENTS, duration, ops_per_sec, found, verified
    );

    // =========================================================================
    // Benchmark 4: Iteration
    // =========================================================================
    println!("Running iteration benchmark...");
    let start = Instant::now();
    let mut visited = 0;
    for _ in list.iter(guard) {
        visited += 1;
    }
    let duration = start.elapsed();
    let ops_per_sec = visited as f64 / duration.as_secs_f64();
    println!(
        "  iteration: {} ops in {:?} | {:.0} ops/s",
        visited, duration, ops_per_sec
    );

    // =========================================================================
    // Benchmark 5: Reverse Iteration
    // =========================================================================
    println!("Running reverse iteration benchmark...");
    let start = Instant::now();
    visited = 0;
    for _ in list.iter(guard).rev() {
        visited += 1;
    }
    let duration = start.elapsed();
    let ops_per_sec = visited as f64 / duration.as_secs_f64();
    println!(
        "  reverse_iteration: {} ops in {:?} | {:.0} ops/s",
        visited, duration, ops_per_sec
    );

    // =========================================================================
    // Benchmark 6: Insert + Remove
    // =========================================================================
    println!("Running insert+remove benchmark...");
    let start = Instant::now();
    for i in 0..NUM_ELEMENTS {
        let key = make_key(i);
        list.insert(key, !key, guard);
    }
    for i in 0..NUM_ELEMENTS {
        let key = make_key(i);
        let _ = list.remove(&key, guard);
    }
    let duration = start.elapsed();
    let ops_per_sec = (NUM_ELEMENTS * 2) as f64 / duration.as_secs_f64();
    println!(
        "  insert_remove: {} ops in {:?} | {:.0} ops/s",
        NUM_ELEMENTS * 2,
        duration,
        ops_per_sec
    );

    println!();
    println!("=== JSON Output ===");
    println!("{{");
    println!("  \"engine\": \"crossbeam_skiplist_rust\",");
    println!("  \"elements\": {},", NUM_ELEMENTS);
    println!("  \"results\": {{");

    // Re-run to get JSON
    let start = Instant::now();
    let list = SkipList::new(epoch::default_collector().clone());
    for i in 0..NUM_ELEMENTS {
        let key = make_key(i);
        list.insert(key, !key, guard);
    }
    let seq_insert = start.elapsed();

    let start = Instant::now();
    for i in 0..NUM_ELEMENTS {
        let key = make_key(i);
        list.get(&key, guard);
    }
    let seq_lookup = start.elapsed();

    println!(
        "    \"sequential_insert\": {:.0},",
        NUM_ELEMENTS as f64 / seq_insert.as_secs_f64()
    );
    println!(
        "    \"sequential_lookup\": {:.0},",
        NUM_ELEMENTS as f64 / seq_lookup.as_secs_f64()
    );
    println!("    \"iteration\": {}", NUM_ELEMENTS);
    println!("  }}");
    println!("}}");
}
