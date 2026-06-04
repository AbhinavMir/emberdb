//! One-shot probe: EmberDB on-disk storage for the 1.728M MIMIC workload.
//! Ingests with persistence enabled, flushes, and reports total bytes / per-record.
//! Used to fill the storage row of the head-to-head baseline table honestly.

use emberdb::config::{Config, StorageConfig, ApiConfig};
use emberdb::storage::{StorageEngine, Record};
use emberdb::mimic::{ChartEvent, chartevent_to_record, itemid_to_loinc};
use std::time::Duration;
use std::fs;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

fn vital_configs() -> Vec<(i64, f64, f64, f64, f64)> {
    vec![
        (220045, 84.0, 17.0, 30.0, 200.0),
        (220050, 121.0, 23.0, 60.0, 250.0),
        (220051, 70.0, 14.0, 30.0, 150.0),
        (220277, 96.8, 2.8, 70.0, 100.0),
        (220210, 18.0, 4.0, 6.0, 45.0),
        (223761, 98.6, 1.2, 95.0, 105.0),
    ]
}

fn generate(n_patients: usize, seed: u64) -> Vec<ChartEvent> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut events = Vec::new();
    let vitals = vital_configs();
    let base_ts: i64 = 1_700_000_000;
    for p in 0..n_patients {
        let subject_id = 10000 + p as i64;
        let admit_ts = base_ts + (p as i64 * 300);
        let n_measurements = (48 * 60) / 5;
        for (itemid, mean, std, min_c, max_c) in &vitals {
            for m in 0..n_measurements {
                let charttime = admit_ts + (m as i64 * 5 * 60);
                let u1: f64 = rng.gen::<f64>().max(1e-10);
                let u2: f64 = rng.gen::<f64>();
                let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
                let val = (mean + std * z).max(*min_c).min(*max_c);
                events.push(ChartEvent {
                    subject_id, hadm_id: Some(20000 + p as i64), stay_id: Some(30000 + p as i64),
                    charttime, itemid: *itemid, value: Some(format!("{:.1}", val)),
                    valuenum: Some(val), valueuom: itemid_to_loinc(*itemid).map(|(_, _, u)| u.to_string()),
                    ..Default::default()
                });
            }
        }
    }
    events
}

fn dir_size(path: &std::path::Path) -> std::io::Result<u64> {
    let mut total = 0;
    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let p = entry.path();
            if p.is_dir() { total += dir_size(&p)?; } else { total += entry.metadata()?.len(); }
        }
    }
    Ok(total)
}

fn main() {
    let events = generate(500, 42);
    let records: Vec<Record> = events.iter().filter_map(chartevent_to_record).collect();
    let dir = "/tmp/emberdb_storage_probe";
    let _ = fs::remove_dir_all(dir);
    let config = Config {
        storage: StorageConfig { path: dir.to_string(), max_chunk_size: 1_048_576 },
        api: ApiConfig { host: "127.0.0.1".to_string(), port: 5432 },
        chunk_duration: Duration::from_secs(3600),
    };
    let mut engine = StorageEngine::new(&config).unwrap();
    // Ingest in memory mode (no per-record WAL fsync), then enable persistence
    // and flush once. This measures the steady-state on-disk chunk format,
    // matching the standalone storage-overhead methodology in the paper.
    engine.set_debug_settings(true, true, Some(10_000)).unwrap();
    for r in &records { engine.insert(r.clone()).unwrap(); }
    engine.set_persistence(true);
    engine.flush_all().unwrap();
    // Count only the persisted chunk files; the WAL is transient and was not
    // written in memory mode.
    let chunk_dir = format!("{}/chunks", dir);
    let bytes = dir_size(std::path::Path::new(&chunk_dir)).unwrap_or(0);
    let per_rec = bytes as f64 / records.len() as f64;
    println!("EMBER_STORAGE bytes={} records={} bytes_per_rec={:.1}", bytes, records.len(), per_rec);
    let _ = fs::remove_dir_all(dir);
}
