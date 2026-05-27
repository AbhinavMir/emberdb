//! EmberDB vs SQLite comparative benchmark
//! Same workload on both: write throughput, query types, storage size

use emberdb::config::{Config, StorageConfig, ApiConfig};
use emberdb::storage::{StorageEngine, Record};
use rusqlite::{Connection, params};
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

fn setup_sqlite(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            patient_id TEXT NOT NULL,
            loinc_code TEXT NOT NULL,
            value REAL NOT NULL,
            unit TEXT,
            resource_type TEXT DEFAULT 'Observation'
        );
        CREATE INDEX IF NOT EXISTS idx_ts ON observations(timestamp);
        CREATE INDEX IF NOT EXISTS idx_patient ON observations(patient_id);
        CREATE INDEX IF NOT EXISTS idx_patient_code ON observations(patient_id, loinc_code);
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;",
    )
    .expect("SQLite setup failed");
}

fn bench_write_throughput(csv: &mut Vec<String>) {
    let scale_points: Vec<usize> = vec![1_000, 10_000, 50_000, 100_000, 500_000];
    let mut rng = rand::thread_rng();

    for &n in &scale_points {
        let base_ts: i64 = 1_700_000_000;

        // Pre-generate data
        let data: Vec<(i64, String, f64)> = (0..n)
            .map(|i| {
                let patient = format!("P{:04}", i % 100);
                let ts = base_ts + (i as i64);
                let val = 60.0 + rng.gen::<f64>() * 40.0;
                (ts, patient, val)
            })
            .collect();

        // -- EmberDB --
        let dir = format!("/tmp/emberdb_comp_write_{}", n);
        let _ = fs::remove_dir_all(&dir);
        let config = test_config(&dir);
        let engine = StorageEngine::new(&config).unwrap();
        engine.set_debug_settings(true, true, Some(1000)).unwrap();

        let start = Instant::now();
        for (ts, patient, val) in &data {
            engine
                .insert(make_record(*ts, patient, "8867-4", *val))
                .unwrap();
        }
        let ember_elapsed = start.elapsed();
        let ember_tput = n as f64 / ember_elapsed.as_secs_f64();
        let _ = fs::remove_dir_all(&dir);

        // -- SQLite --
        let sqlite_path = format!("/tmp/sqlite_comp_write_{}.db", n);
        let _ = fs::remove_file(&sqlite_path);
        let conn = Connection::open(&sqlite_path).unwrap();
        setup_sqlite(&conn);

        let start = Instant::now();
        let tx = conn.unchecked_transaction().unwrap();
        for (ts, patient, val) in &data {
            tx.execute(
                "INSERT INTO observations (timestamp, patient_id, loinc_code, value, unit) VALUES (?1,?2,?3,?4,?5)",
                params![ts, patient, "8867-4", val, "bpm"],
            )
            .unwrap();
        }
        tx.commit().unwrap();
        let sqlite_elapsed = start.elapsed();
        let sqlite_tput = n as f64 / sqlite_elapsed.as_secs_f64();
        let _ = fs::remove_file(&sqlite_path);

        let line = format!(
            "write_throughput,{},{:.0},{:.0},{:.2}",
            n,
            ember_tput,
            sqlite_tput,
            ember_tput / sqlite_tput
        );
        println!("{}", line);
        csv.push(line);
    }
}

fn bench_queries(csv: &mut Vec<String>) {
    let mut rng = rand::thread_rng();
    let base_ts: i64 = 1_700_000_000;
    let n: usize = 100_000;

    // -- Setup EmberDB --
    let dir = "/tmp/emberdb_comp_query";
    let _ = fs::remove_dir_all(dir);
    let config = test_config(dir);
    let engine = StorageEngine::new(&config).unwrap();
    engine.set_debug_settings(true, true, Some(1000)).unwrap();

    // -- Setup SQLite --
    let sqlite_path = "/tmp/sqlite_comp_query.db";
    let _ = fs::remove_file(sqlite_path);
    let conn = Connection::open(sqlite_path).unwrap();
    setup_sqlite(&conn);

    // Insert same data into both
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..n {
            let patient = format!("P{:04}", i % 100);
            let ts = base_ts + (i as i64);
            let val = 60.0 + rng.gen::<f64>() * 40.0;

            engine
                .insert(make_record(ts, &patient, "8867-4", val))
                .unwrap();
            tx.execute(
                "INSERT INTO observations (timestamp, patient_id, loinc_code, value, unit) VALUES (?1,?2,?3,?4,?5)",
                params![ts, &patient, "8867-4", val, "bpm"],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    let iterations = 500;

    // Query 1: Point lookup (narrow time range, specific patient)
    {
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = engine.query_range(base_ts + 500, base_ts + 501, "P0050|8867-4|unit");
        }
        let ember_us = start.elapsed().as_micros() as f64 / iterations as f64;

        let start = Instant::now();
        for _ in 0..iterations {
            let _: Vec<(i64, f64)> = conn
                .prepare_cached(
                    "SELECT timestamp, value FROM observations WHERE patient_id=?1 AND loinc_code=?2 AND timestamp BETWEEN ?3 AND ?4",
                )
                .unwrap()
                .query_map(params!["P0050", "8867-4", base_ts + 500, base_ts + 501], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?))
                })
                .unwrap()
                .filter_map(|r| r.ok())
                .collect();
        }
        let sqlite_us = start.elapsed().as_micros() as f64 / iterations as f64;

        let line = format!("query_latency,point,{:.1},{:.1},{:.2}", ember_us, sqlite_us, sqlite_us / ember_us);
        println!("{}", line);
        csv.push(line);
    }

    // Query 2: Latest value
    {
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = engine.get_latest("P0050|8867-4|unit");
        }
        let ember_us = start.elapsed().as_micros() as f64 / iterations as f64;

        let start = Instant::now();
        for _ in 0..iterations {
            let _: Option<(i64, f64)> = conn
                .prepare_cached(
                    "SELECT timestamp, value FROM observations WHERE patient_id=?1 AND loinc_code=?2 ORDER BY timestamp DESC LIMIT 1",
                )
                .unwrap()
                .query_row(params!["P0050", "8867-4"], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?))
                })
                .ok();
        }
        let sqlite_us = start.elapsed().as_micros() as f64 / iterations as f64;

        let line = format!("query_latency,latest,{:.1},{:.1},{:.2}", ember_us, sqlite_us, sqlite_us / ember_us);
        println!("{}", line);
        csv.push(line);
    }

    // Query 3: Time range (50K records span)
    {
        let start = Instant::now();
        for _ in 0..100 {
            let _ = engine.query_range(base_ts, base_ts + 50_000, "P0050|8867-4|unit");
        }
        let ember_us = start.elapsed().as_micros() as f64 / 100.0;

        let start = Instant::now();
        for _ in 0..100 {
            let _: Vec<(i64, f64)> = conn
                .prepare_cached(
                    "SELECT timestamp, value FROM observations WHERE patient_id=?1 AND loinc_code=?2 AND timestamp BETWEEN ?3 AND ?4",
                )
                .unwrap()
                .query_map(params!["P0050", "8867-4", base_ts, base_ts + 50_000], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?))
                })
                .unwrap()
                .filter_map(|r| r.ok())
                .collect();
        }
        let sqlite_us = start.elapsed().as_micros() as f64 / 100.0;

        let line = format!("query_latency,time_range,{:.1},{:.1},{:.2}", ember_us, sqlite_us, sqlite_us / ember_us);
        println!("{}", line);
        csv.push(line);
    }

    // Query 4: Full patient (all records for one patient)
    {
        let start = Instant::now();
        for _ in 0..100 {
            let _ = engine.query_range(base_ts, base_ts + n as i64, "P0050|8867-4|unit");
        }
        let ember_us = start.elapsed().as_micros() as f64 / 100.0;

        let start = Instant::now();
        for _ in 0..100 {
            let _: Vec<(i64, f64)> = conn
                .prepare_cached(
                    "SELECT timestamp, value FROM observations WHERE patient_id=?1 ORDER BY timestamp",
                )
                .unwrap()
                .query_map(params!["P0050"], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?))
                })
                .unwrap()
                .filter_map(|r| r.ok())
                .collect();
        }
        let sqlite_us = start.elapsed().as_micros() as f64 / 100.0;

        let line = format!("query_latency,full_patient,{:.1},{:.1},{:.2}", ember_us, sqlite_us, sqlite_us / ember_us);
        println!("{}", line);
        csv.push(line);
    }

    // Storage size comparison
    {
        engine.flush_all().unwrap();
        let ember_bytes = dir_size(std::path::Path::new(dir)).unwrap_or(0);
        let sqlite_bytes = fs::metadata(sqlite_path).map(|m| m.len()).unwrap_or(0);
        let line = format!(
            "storage_size,{},{},{},{:.2}",
            n, ember_bytes, sqlite_bytes,
            ember_bytes as f64 / sqlite_bytes as f64
        );
        println!("{}", line);
        csv.push(line);
    }

    let _ = fs::remove_dir_all(dir);
    let _ = fs::remove_file(sqlite_path);
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
    println!("=== EmberDB vs SQLite Comparative Benchmark ===\n");
    let mut csv = Vec::new();
    csv.push("category,scale_or_type,emberdb,sqlite,ratio".to_string());

    bench_write_throughput(&mut csv);
    println!();
    bench_queries(&mut csv);

    let out_path = "comparative_results.csv";
    let mut f = fs::File::create(out_path).expect("Failed to create CSV");
    for line in &csv {
        writeln!(f, "{}", line).unwrap();
    }
    println!("\nResults written to {}", out_path);
}
