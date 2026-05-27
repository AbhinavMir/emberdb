//! EmberDB standalone evaluation benchmark
//! Measures: write throughput, query latency, pattern detection, WAL, storage overhead

use emberdb::config::{Config, StorageConfig, ApiConfig};
use emberdb::storage::{StorageEngine, Record};
use emberdb::timeseries::detection::PatternDetector;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::fs;
use std::io::Write;
use rand::Rng;

fn test_config(path: &str) -> Config {
    Config {
        storage: StorageConfig {
            path: path.to_string(),
            max_chunk_size: 1_048_576,
        },
        api: ApiConfig {
            host: "127.0.0.1".to_string(),
            port: 5432,
        },
        chunk_duration: Duration::from_secs(3600),
    }
}

fn make_record(ts: i64, patient: &str, code: &str, value: f64) -> Record {
    let mut context = HashMap::new();
    context.insert("patient_id".to_string(), patient.to_string());
    Record {
        timestamp: ts,
        metric_name: format!("{}|{}|unit", patient, code),
        value,
        context,
        resource_type: "Observation".to_string(),
    }
}

fn bench_write_throughput(csv: &mut Vec<String>) {
    let batch_sizes: Vec<usize> = vec![1_000, 10_000, 50_000, 100_000, 500_000];
    let mut rng = rand::thread_rng();

    for &n in &batch_sizes {
        let dir = format!("/tmp/emberdb_eval_write_{}", n);
        let _ = fs::remove_dir_all(&dir);
        let config = test_config(&dir);
        let engine = StorageEngine::new(&config).expect("engine creation failed");
        engine.set_debug_settings(true, true, Some(1000)).unwrap();

        let base_ts: i64 = 1_700_000_000;
        let records: Vec<Record> = (0..n)
            .map(|i| {
                let patient = format!("P{:04}", i % 100);
                let code = "8867-4";
                let ts = base_ts + (i as i64);
                let val = 60.0 + rng.gen::<f64>() * 40.0;
                make_record(ts, &patient, code, val)
            })
            .collect();

        let start = Instant::now();
        for r in records {
            engine.insert(r).unwrap();
        }
        let elapsed = start.elapsed();

        let throughput = n as f64 / elapsed.as_secs_f64();
        let line = format!("write_throughput,{},{:.0},{:.6}", n, throughput, elapsed.as_secs_f64());
        println!("{}", line);
        csv.push(line);
        let _ = fs::remove_dir_all(&dir);
    }
}

fn bench_query_latency(csv: &mut Vec<String>) {
    let dir = "/tmp/emberdb_eval_query";
    let _ = fs::remove_dir_all(dir);
    let config = test_config(dir);
    let engine = StorageEngine::new(&config).expect("engine creation failed");
    engine.set_debug_settings(true, true, Some(1000)).unwrap();

    let mut rng = rand::thread_rng();
    let base_ts: i64 = 1_700_000_000;
    let n = 100_000;

    // Insert data: 100 patients, ~1000 records each
    for i in 0..n {
        let patient = format!("P{:04}", i % 100);
        let code = "8867-4";
        let ts = base_ts + (i as i64);
        let val = 60.0 + rng.gen::<f64>() * 40.0;
        engine.insert(make_record(ts, &patient, code, val)).unwrap();
    }

    // 1. Point query (single metric, narrow range)
    let iterations = 1000;
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = engine.query_range(base_ts + 500, base_ts + 501, "P0050|8867-4|unit");
    }
    let elapsed = start.elapsed();
    let avg_us = elapsed.as_micros() as f64 / iterations as f64;
    let line = format!("query_latency,point,{:.1},{}", avg_us, iterations);
    println!("{}", line);
    csv.push(line);

    // 2. Latest query
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = engine.get_latest("P0050|8867-4|unit");
    }
    let elapsed = start.elapsed();
    let avg_us = elapsed.as_micros() as f64 / iterations as f64;
    let line = format!("query_latency,latest,{:.1},{}", avg_us, iterations);
    println!("{}", line);
    csv.push(line);

    // 3. Patient+code query (all records for one patient)
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = engine.query_range(base_ts, base_ts + n as i64, "P0050|8867-4|unit");
    }
    let elapsed = start.elapsed();
    let avg_us = elapsed.as_micros() as f64 / iterations as f64;
    let line = format!("query_latency,patient_code,{:.1},{}", avg_us, iterations);
    println!("{}", line);
    csv.push(line);

    // 4. Full patient query (all metrics for patient via prefix match)
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = engine.get_matching_metrics("P0050|");
    }
    let elapsed = start.elapsed();
    let avg_us = elapsed.as_micros() as f64 / iterations as f64;
    let line = format!("query_latency,full_patient,{:.1},{}", avg_us, iterations);
    println!("{}", line);
    csv.push(line);

    // 5. Time range query (wide range, single metric)
    let start = Instant::now();
    for _ in 0..100 {
        let _ = engine.query_range(base_ts, base_ts + 50_000, "P0050|8867-4|unit");
    }
    let elapsed = start.elapsed();
    let avg_us = elapsed.as_micros() as f64 / 100.0;
    let line = format!("query_latency,time_range,{:.1},100", avg_us);
    println!("{}", line);
    csv.push(line);

    let _ = fs::remove_dir_all(dir);
}

fn bench_pattern_detection(csv: &mut Vec<String>) {
    let mut rng = rand::thread_rng();
    let base_ts: i64 = 1_700_000_000;
    let n = 2000;

    // Generate test data with a changepoint in the middle
    let records: Vec<Record> = (0..n)
        .map(|i| {
            let ts = base_ts + i * 60; // one record per minute
            let val = if i < n / 2 {
                80.0 + rng.gen::<f64>() * 10.0
            } else {
                120.0 + rng.gen::<f64>() * 10.0
            };
            make_record(ts, "P0001", "8867-4", val)
        })
        .collect();

    let detector = PatternDetector::new();

    // 1. CUSUM changepoint detection
    let start = Instant::now();
    for _ in 0..100 {
        let _ = detector.detect_changepoints(&records);
    }
    let elapsed = start.elapsed();
    let avg_us = elapsed.as_micros() as f64 / 100.0;
    let line = format!("pattern_detection,cusum,{:.1},100", avg_us);
    println!("{}", line);
    csv.push(line);

    // 2. Seasonal decomposition
    let start = Instant::now();
    for _ in 0..100 {
        let _ = detector.seasonal_decomposition(&records);
    }
    let elapsed = start.elapsed();
    let avg_us = elapsed.as_micros() as f64 / 100.0;
    let line = format!("pattern_detection,seasonal,{:.1},100", avg_us);
    println!("{}", line);
    csv.push(line);

    // 3. Moving window analysis
    let start = Instant::now();
    for _ in 0..100 {
        let _ = detector.moving_window_analysis(&records);
    }
    let elapsed = start.elapsed();
    let avg_us = elapsed.as_micros() as f64 / 100.0;
    let line = format!("pattern_detection,moving_window,{:.1},100", avg_us);
    println!("{}", line);
    csv.push(line);

    // 4. Multivariate (Mahalanobis) -- need multiple metrics
    let records_hr: Vec<Record> = (0..n)
        .map(|i| {
            let ts = base_ts + i * 60;
            make_record(ts, "P0001", "8867-4", 80.0 + rng.gen::<f64>() * 10.0)
        })
        .collect();
    let records_rr: Vec<Record> = (0..n)
        .map(|i| {
            let ts = base_ts + i * 60;
            make_record(ts, "P0001", "9279-1", 16.0 + rng.gen::<f64>() * 4.0)
        })
        .collect();
    let mut metric_map = HashMap::new();
    metric_map.insert("P0001|8867-4|unit".to_string(), records_hr);
    metric_map.insert("P0001|9279-1|unit".to_string(), records_rr);

    let start = Instant::now();
    for _ in 0..100 {
        let _ = detector.multivariate_outlier_detection(&metric_map);
    }
    let elapsed = start.elapsed();
    let avg_us = elapsed.as_micros() as f64 / 100.0;
    let line = format!("pattern_detection,mahalanobis,{:.1},100", avg_us);
    println!("{}", line);
    csv.push(line);
}

fn bench_wal(csv: &mut Vec<String>) {
    let mut rng = rand::thread_rng();

    // WAL write throughput
    {
        let dir = "/tmp/emberdb_eval_wal_write";
        let _ = fs::remove_dir_all(dir);
        let config = test_config(dir);
        let engine = StorageEngine::new(&config).expect("engine creation failed");
        // Keep persistence ON for WAL benchmarks

        let base_ts: i64 = 1_700_000_000;
        let n = 10_000;
        let records: Vec<Record> = (0..n)
            .map(|i| {
                let ts = base_ts + (i as i64);
                make_record(ts, "P0001", "8867-4", 80.0 + rng.gen::<f64>() * 10.0)
            })
            .collect();

        let start = Instant::now();
        for r in records {
            engine.insert(r).unwrap();
        }
        let elapsed = start.elapsed();

        let throughput = n as f64 / elapsed.as_secs_f64();
        let line = format!("wal,write_throughput,{:.0},{:.6}", throughput, elapsed.as_secs_f64());
        println!("{}", line);
        csv.push(line);

        // Flush then test replay
        engine.flush_all().unwrap();

        let start = Instant::now();
        let engine2 = StorageEngine::new(&config).expect("engine recovery failed");
        let elapsed = start.elapsed();
        let line = format!("wal,replay_time_secs,{:.6},{}", elapsed.as_secs_f64(), n);
        println!("{}", line);
        csv.push(line);
        drop(engine2);

        let _ = fs::remove_dir_all(dir);
    }
}

fn bench_storage_overhead(csv: &mut Vec<String>) {
    let dir = "/tmp/emberdb_eval_storage_size";
    let _ = fs::remove_dir_all(dir);
    let config = test_config(dir);
    let engine = StorageEngine::new(&config).expect("engine creation failed");
    // Use memory mode for fast insertion, then flush to measure disk size
    engine.set_debug_settings(true, true, Some(1000)).unwrap();

    let mut rng = rand::thread_rng();
    let base_ts: i64 = 1_700_000_000;
    let n: usize = 100_000;

    for i in 0..n {
        let patient = format!("P{:04}", i % 100);
        let ts = base_ts + (i as i64);
        let val = 80.0 + rng.gen::<f64>() * 40.0;
        engine.insert(make_record(ts, &patient, "8867-4", val)).unwrap();
    }

    // Re-enable persistence and flush all chunks to measure on-disk size
    engine.set_debug_settings(false, false, None).unwrap();
    engine.flush_all().unwrap();

    // Measure disk usage
    let total_bytes = dir_size(std::path::Path::new(dir)).unwrap_or(0);
    let bytes_per_record = total_bytes as f64 / n as f64;

    let line = format!("storage_overhead,bytes_per_record,{:.1},{}", bytes_per_record, n);
    println!("{}", line);
    csv.push(line);

    let line = format!("storage_overhead,total_bytes,{},{}", total_bytes, n);
    println!("{}", line);
    csv.push(line);

    let _ = fs::remove_dir_all(dir);
}

fn dir_size(path: &std::path::Path) -> std::io::Result<u64> {
    let mut total = 0;
    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let p = entry.path();
            if p.is_dir() {
                total += dir_size(&p)?;
            } else {
                total += entry.metadata()?.len();
            }
        }
    }
    Ok(total)
}

fn main() {
    println!("=== EmberDB Standalone Evaluation ===\n");
    let mut csv = Vec::new();
    csv.push("category,metric,value,detail".to_string());

    bench_write_throughput(&mut csv);
    println!();
    bench_query_latency(&mut csv);
    println!();
    bench_pattern_detection(&mut csv);
    println!();
    bench_wal(&mut csv);
    println!();
    bench_storage_overhead(&mut csv);

    // Write CSV
    let out_path = "eval_results.csv";
    let mut f = fs::File::create(out_path).expect("Failed to create CSV");
    for line in &csv {
        writeln!(f, "{}", line).unwrap();
    }
    println!("\nResults written to {}", out_path);
}
