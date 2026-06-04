//! MIMIC-IV format evaluation benchmark
//! Generates synthetic MIMIC-IV chartevents, converts to FHIR, benchmarks EmberDB vs SQLite

use emberdb::config::{Config, StorageConfig, ApiConfig};
use emberdb::storage::{StorageEngine, Record};
use emberdb::mimic::{ChartEvent, chartevent_to_record, itemid_to_loinc};
use rusqlite::{Connection, params};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::fs;
use std::io::Write;
use rand::Rng;
use rand::rngs::StdRng;
use rand::SeedableRng;

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

/// Vital sign configuration: (itemid, mean, std, min_clamp, max_clamp)
fn vital_configs() -> Vec<(i64, f64, f64, f64, f64)> {
    vec![
        (220045, 84.0, 17.0, 30.0, 200.0),   // Heart rate
        (220050, 121.0, 23.0, 60.0, 250.0),   // Systolic BP
        (220051, 70.0, 14.0, 30.0, 150.0),    // Diastolic BP
        (220277, 96.8, 2.8, 70.0, 100.0),     // SpO2
        (220210, 18.0, 4.0, 6.0, 45.0),       // Respiratory rate
        (223761, 98.6, 1.2, 95.0, 105.0),     // Temperature (F)
    ]
}

/// Generate synthetic MIMIC-IV chartevents for n_patients, each with a 48h ICU stay
fn generate_synthetic_chartevents(n_patients: usize, seed: u64) -> Vec<ChartEvent> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut events = Vec::new();
    let vitals = vital_configs();
    let base_ts: i64 = 1_700_000_000;

    for p in 0..n_patients {
        let subject_id = 10000 + p as i64;
        let hadm_id = 20000 + p as i64;
        let stay_id = 30000 + p as i64;
        let admit_ts = base_ts + (p as i64 * 300); // stagger admissions

        // 48 hours = 2880 minutes; vital signs every 5 minutes = 576 measurements per vital
        let duration_min = 48 * 60;
        let interval_min = 5;
        let n_measurements = duration_min / interval_min;

        for (itemid, mean, std, min_c, max_c) in &vitals {
            for m in 0..n_measurements {
                let charttime = admit_ts + (m as i64 * interval_min as i64 * 60);
                // Box-Muller for normal distribution
                let u1: f64 = rng.gen::<f64>().max(1e-10);
                let u2: f64 = rng.gen::<f64>();
                let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
                let raw_val = mean + std * z;
                let val = raw_val.max(*min_c).min(*max_c);

                events.push(ChartEvent {
                    subject_id,
                    hadm_id: Some(hadm_id),
                    stay_id: Some(stay_id),
                    caregiver_id: Some(40000 + (p as i64 % 50)), // synthetic caregiver pool
                    charttime,
                    storetime: Some(charttime + 60), // charted ~1 min after observation
                    itemid: *itemid,
                    value: Some(format!("{:.1}", val)),
                    valuenum: Some(val),
                    valueuom: itemid_to_loinc(*itemid).map(|(_, _, u)| u.to_string()),
                    warning: Some(0),
                });
            }
        }
    }

    events
}

fn setup_sqlite(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS chartevents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject_id INTEGER NOT NULL,
            hadm_id INTEGER,
            stay_id INTEGER,
            charttime INTEGER NOT NULL,
            itemid INTEGER NOT NULL,
            value TEXT,
            valuenum REAL,
            valueuom TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_ce_subject ON chartevents(subject_id);
        CREATE INDEX IF NOT EXISTS idx_ce_ts ON chartevents(charttime);
        CREATE INDEX IF NOT EXISTS idx_ce_item ON chartevents(itemid);
        CREATE INDEX IF NOT EXISTS idx_ce_subject_item ON chartevents(subject_id, itemid);
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;",
    )
    .expect("SQLite setup failed");
}

fn main() {
    println!("=== MIMIC-IV Synthetic Benchmark ===\n");
    let mut csv = Vec::new();
    csv.push("category,metric,emberdb,sqlite,ratio,detail".to_string());

    let n_patients = 500;
    println!("Generating synthetic chartevents for {} patients (48h ICU stays, 6 vitals)...", n_patients);
    let events = generate_synthetic_chartevents(n_patients, 42);
    let total_events = events.len();
    println!("Generated {} chartevents\n", total_events);

    // Emit the synthetic data in the real MIMIC-IV 11-column shape with
    // human-readable `YYYY-MM-DD HH:MM:SS` timestamps, so it is byte-compatible
    // with the real loader and can be inspected / diffed against the demo file.
    let synth_csv = "/tmp/emberdb_synth_chartevents.csv";
    emberdb::mimic::write_chartevents_csv(synth_csv, &events)
        .expect("failed to write synthetic chartevents CSV");
    println!("Wrote synthetic chartevents (real MIMIC 11-col shape) to {}\n", synth_csv);

    // Convert to EmberDB records
    let records: Vec<Record> = events.iter().filter_map(chartevent_to_record).collect();
    println!("Converted {} events to {} FHIR records\n", total_events, records.len());

    // -- Ingest into EmberDB --
    let ember_dir = "/tmp/emberdb_mimic_bench";
    let _ = fs::remove_dir_all(ember_dir);
    let config = test_config(ember_dir);
    let engine = StorageEngine::new(&config).unwrap();
    engine.set_debug_settings(true, true, Some(1000)).unwrap();

    println!("Ingesting into EmberDB...");
    let start = Instant::now();
    for r in &records {
        engine.insert(r.clone()).unwrap();
    }
    let ember_ingest = start.elapsed();
    let ember_tput = records.len() as f64 / ember_ingest.as_secs_f64();
    println!("  EmberDB: {:.0} records/sec ({:.3}s)\n", ember_tput, ember_ingest.as_secs_f64());

    // -- Ingest into SQLite --
    let sqlite_path = "/tmp/sqlite_mimic_bench.db";
    let _ = fs::remove_file(sqlite_path);
    let conn = Connection::open(sqlite_path).unwrap();
    setup_sqlite(&conn);

    println!("Ingesting into SQLite...");
    let start = Instant::now();
    {
        let tx = conn.unchecked_transaction().unwrap();
        for e in &events {
            tx.execute(
                "INSERT INTO chartevents (subject_id, hadm_id, stay_id, charttime, itemid, value, valuenum, valueuom) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    e.subject_id,
                    e.hadm_id,
                    e.stay_id,
                    e.charttime,
                    e.itemid,
                    e.value,
                    e.valuenum,
                    e.valueuom
                ],
            ).unwrap();
        }
        tx.commit().unwrap();
    }
    let sqlite_ingest = start.elapsed();
    let sqlite_tput = events.len() as f64 / sqlite_ingest.as_secs_f64();
    println!("  SQLite:  {:.0} records/sec ({:.3}s)\n", sqlite_tput, sqlite_ingest.as_secs_f64());

    let line = format!(
        "ingest,throughput,{:.0},{:.0},{:.2},{}",
        ember_tput, sqlite_tput, ember_tput / sqlite_tput, records.len()
    );
    println!("{}", line);
    csv.push(line);

    // -- Query benchmarks --
    let base_ts: i64 = 1_700_000_000;
    let iterations = 200;

    // Query 1: Single patient, single vital, last hour
    {
        let patient = "10050";
        let metric = format!("{}|8867-4|/min", patient); // HR
        let t_start = base_ts + 40_000;
        let t_end = t_start + 3600;

        let start = Instant::now();
        for _ in 0..iterations {
            let _ = engine.query_range(t_start, t_end, &metric);
        }
        let ember_us = start.elapsed().as_micros() as f64 / iterations as f64;

        let start = Instant::now();
        for _ in 0..iterations {
            let _: Vec<(i64, f64)> = conn
                .prepare_cached(
                    "SELECT charttime, valuenum FROM chartevents WHERE subject_id=?1 AND itemid=?2 AND charttime BETWEEN ?3 AND ?4",
                )
                .unwrap()
                .query_map(params![10050i64, 220045i64, t_start, t_end], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?))
                })
                .unwrap()
                .filter_map(|r| r.ok())
                .collect();
        }
        let sqlite_us = start.elapsed().as_micros() as f64 / iterations as f64;

        let line = format!("query,single_vital_1h,{:.1},{:.1},{:.2},{}", ember_us, sqlite_us, sqlite_us / ember_us.max(1.0), iterations);
        println!("{}", line);
        csv.push(line);
    }

    // Query 2: Single patient, all vitals (full stay)
    {
        let patient_id = 10050i64;
        let patient_str = "10050";
        let t_start = base_ts;
        let t_end = base_ts + 48 * 3600;

        // EmberDB: query each vital metric
        let vital_items: Vec<(i64, &str, &str)> = vec![
            (220045, "8867-4", "/min"),
            (220050, "8480-6", "mmHg"),
            (220051, "8462-4", "mmHg"),
            (220277, "2708-6", "%"),
            (220210, "9279-1", "/min"),
            (223761, "8310-5", "degF"),
        ];

        let start = Instant::now();
        for _ in 0..iterations {
            for (_, loinc, unit) in &vital_items {
                let metric = format!("{}|{}|{}", patient_str, loinc, unit);
                let _ = engine.query_range(t_start, t_end, &metric);
            }
        }
        let ember_us = start.elapsed().as_micros() as f64 / iterations as f64;

        let start = Instant::now();
        for _ in 0..iterations {
            let _: Vec<(i64, i64, f64)> = conn
                .prepare_cached(
                    "SELECT charttime, itemid, valuenum FROM chartevents WHERE subject_id=?1 AND charttime BETWEEN ?2 AND ?3 ORDER BY charttime",
                )
                .unwrap()
                .query_map(params![patient_id, t_start, t_end], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, f64>(2)?))
                })
                .unwrap()
                .filter_map(|r| r.ok())
                .collect();
        }
        let sqlite_us = start.elapsed().as_micros() as f64 / iterations as f64;

        let line = format!("query,full_patient_stay,{:.1},{:.1},{:.2},{}", ember_us, sqlite_us, sqlite_us / ember_us.max(1.0), iterations);
        println!("{}", line);
        csv.push(line);
    }

    // Query 3: Cohort query (all patients, single vital, 1h window)
    {
        let t_start = base_ts + 20_000;
        let t_end = t_start + 3600;

        let start = Instant::now();
        for _ in 0..10 {
            for p in 0..n_patients {
                let patient = format!("{}", 10000 + p);
                let metric = format!("{}|8867-4|/min", patient);
                let _ = engine.query_range(t_start, t_end, &metric);
            }
        }
        let ember_us = start.elapsed().as_micros() as f64 / 10.0;

        let start = Instant::now();
        for _ in 0..10 {
            let _: Vec<(i64, i64, f64)> = conn
                .prepare_cached(
                    "SELECT subject_id, charttime, valuenum FROM chartevents WHERE itemid=?1 AND charttime BETWEEN ?2 AND ?3",
                )
                .unwrap()
                .query_map(params![220045i64, t_start, t_end], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, f64>(2)?))
                })
                .unwrap()
                .filter_map(|r| r.ok())
                .collect();
        }
        let sqlite_us = start.elapsed().as_micros() as f64 / 10.0;

        let line = format!("query,cohort_vital_1h,{:.1},{:.1},{:.2},10", ember_us, sqlite_us, sqlite_us / ember_us.max(1.0));
        println!("{}", line);
        csv.push(line);
    }

    // Query 4: Latest vital for a patient
    {
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = engine.get_latest("10050|8867-4|/min");
        }
        let ember_us = start.elapsed().as_micros() as f64 / iterations as f64;

        let start = Instant::now();
        for _ in 0..iterations {
            let _: Option<(i64, f64)> = conn
                .prepare_cached(
                    "SELECT charttime, valuenum FROM chartevents WHERE subject_id=?1 AND itemid=?2 ORDER BY charttime DESC LIMIT 1",
                )
                .unwrap()
                .query_row(params![10050i64, 220045i64], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?))
                })
                .ok();
        }
        let sqlite_us = start.elapsed().as_micros() as f64 / iterations as f64;

        let line = format!("query,latest_vital,{:.1},{:.1},{:.2},{}", ember_us, sqlite_us, sqlite_us / ember_us.max(1.0), iterations);
        println!("{}", line);
        csv.push(line);
    }

    // Storage comparison
    {
        engine.flush_all().unwrap();
        let ember_bytes = dir_size(std::path::Path::new(ember_dir)).unwrap_or(0);
        let sqlite_bytes = fs::metadata(sqlite_path).map(|m| m.len()).unwrap_or(0);
        let line = format!(
            "storage,bytes,{},{},{:.2},{}",
            ember_bytes, sqlite_bytes,
            ember_bytes as f64 / sqlite_bytes.max(1) as f64,
            records.len()
        );
        println!("{}", line);
        csv.push(line);
    }

    // Summary statistics
    println!("\n--- Summary ---");
    println!("Patients: {}", n_patients);
    println!("Total chartevents: {}", total_events);
    println!("FHIR records: {}", records.len());
    println!("Vitals per patient: 6 (HR, SBP, DBP, SpO2, RR, Temp)");
    println!("Stay duration: 48h, measurement interval: 5 min");

    // Cleanup
    let _ = fs::remove_dir_all(ember_dir);
    let _ = fs::remove_file(sqlite_path);

    // Write CSV
    let out_path = "mimic_bench_results.csv";
    let mut f = fs::File::create(out_path).expect("Failed to create CSV");
    for line in &csv {
        writeln!(f, "{}", line).unwrap();
    }
    println!("\nResults written to {}", out_path);
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
