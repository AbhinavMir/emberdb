//! MIMIC-IV REAL demo benchmark.
//!
//! Ingests the REAL open-access MIMIC-IV Clinical Database Demo v2.2 ICU
//! chartevents table (ODC Open Database License, ~100 patients) into all four
//! systems on the SAME real rows and reports ingest throughput, clinical query
//! latency, and storage:
//!
//!   EmberDB      (in-process)
//!   SQLite       (bundled, WAL)            -- in-process
//!   TimescaleDB  pg16  localhost:5433      -- docker tsdb-bench
//!   InfluxDB 2.7       localhost:8087      -- docker influx-bench
//!
//! The real demo CSV schema differs from the synthetic generator:
//!   subject_id,hadm_id,stay_id,caregiver_id,charttime,storetime,itemid,
//!   value,valuenum,valueuom,warning
//! charttime/storetime are ISO datetime strings (de-identified to future
//! dates), so this loader parses that real schema directly and converts
//! charttime to a unix epoch. It reuses emberdb::mimic::{itemid_to_loinc,
//! chartevent_to_record, ChartEvent} for the FHIR/LOINC mapping so the mapping
//! logic is identical to production.
//!
//! Pass the CSV path as argv[1] (default: the extracted demo path under /tmp).
//!
//! Results written to mimic_demo_results.csv.

use emberdb::config::{Config, StorageConfig, ApiConfig};
use emberdb::storage::{StorageEngine, Record};
use emberdb::mimic::{ChartEvent, chartevent_to_record, itemid_to_loinc};
use rusqlite::{Connection, params};
use std::time::{Duration, Instant};
use std::fs;
use std::io::{BufRead, Write};
use postgres::{Client, NoTls};

const INFLUX_URL: &str = "http://localhost:8087";
const INFLUX_ORG: &str = "emberbench";
const INFLUX_BUCKET: &str = "vitals";
const INFLUX_TOKEN: &str = "emberbenchtoken123456789";
const TS_CONN: &str = "host=localhost port=5433 user=postgres password=pw dbname=postgres";

fn test_config(path: &str) -> Config {
    Config {
        storage: StorageConfig { path: path.to_string(), max_chunk_size: 1_048_576 },
        api: ApiConfig { host: "127.0.0.1".to_string(), port: 5432 },
        chunk_duration: Duration::from_secs(3600),
    }
}

// ---------------------------------------------------------------------------
// Real CSV parsing
// ---------------------------------------------------------------------------

/// Parse "YYYY-MM-DD HH:MM:SS" into a unix epoch (seconds, UTC, proleptic
/// Gregorian). MIMIC dates are de-identified into the future but remain valid
/// proleptic-Gregorian calendar dates, so this is exact for relative ordering
/// and windowing.
fn iso_to_epoch(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.len() < 19 { return None; }
    let b = s.as_bytes();
    let year: i64 = s.get(0..4)?.parse().ok()?;
    let month: i64 = s.get(5..7)?.parse().ok()?;
    let day: i64 = s.get(8..10)?.parse().ok()?;
    let hh: i64 = s.get(11..13)?.parse().ok()?;
    let mm: i64 = s.get(14..16)?.parse().ok()?;
    let ss: i64 = s.get(17..19)?.parse().ok()?;
    let _ = b; // suppress unused if slicing changes
    let days = days_from_civil(year, month as u32, day as u32);
    Some(days * 86400 + hh * 3600 + mm * 60 + ss)
}

/// Howard Hinnant's days_from_civil: (y,m,d) -> days since 1970-01-01.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as i64;
    let m = m as i64;
    let d = d as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Split a CSV line honoring simple double-quoted fields. The demo file has a
/// few quoted free-text values; vitals rows are unquoted, but we handle quotes
/// defensively so column positions never shift.
fn split_csv(line: &str) -> Vec<String> {
    let mut out = Vec::with_capacity(11);
    let mut cur = String::new();
    let mut in_q = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                if in_q && chars.peek() == Some(&'"') { cur.push('"'); chars.next(); }
                else { in_q = !in_q; }
            }
            ',' if !in_q => { out.push(std::mem::take(&mut cur)); }
            _ => cur.push(c),
        }
    }
    out.push(cur);
    out
}

/// Parse the REAL MIMIC-IV demo chartevents schema (11 columns) into
/// ChartEvent rows. Rows with unparseable charttime are dropped.
fn parse_real_chartevents(path: &str) -> Vec<ChartEvent> {
    let f = fs::File::open(path).unwrap_or_else(|e| panic!("open {path}: {e}"));
    let mut events = Vec::new();
    let mut lines = std::io::BufReader::new(f).lines();
    let _hdr = lines.next();
    for l in lines {
        let l = match l { Ok(l) => l, Err(_) => continue };
        if l.is_empty() { continue; }
        let f = split_csv(&l);
        // subject_id,hadm_id,stay_id,caregiver_id,charttime,storetime,itemid,value,valuenum,valueuom,warning
        if f.len() < 10 { continue; }
        let subject_id = match f[0].trim().parse::<i64>() { Ok(v) => v, Err(_) => continue };
        let hadm_id = f[1].trim().parse::<i64>().ok();
        let stay_id = f[2].trim().parse::<i64>().ok();
        let caregiver_id = f[3].trim().parse::<i64>().ok();
        let charttime = match iso_to_epoch(&f[4]) { Some(v) => v, None => continue };
        let storetime = iso_to_epoch(&f[5]);
        let itemid = match f[6].trim().parse::<i64>() { Ok(v) => v, Err(_) => continue };
        let value = { let v = f[7].trim(); if v.is_empty() { None } else { Some(v.to_string()) } };
        let valuenum = f[8].trim().parse::<f64>().ok();
        let valueuom = { let v = f[9].trim(); if v.is_empty() { None } else { Some(v.to_string()) } };
        let warning = f.get(10).and_then(|w| w.trim().parse::<i64>().ok());
        events.push(ChartEvent { subject_id, hadm_id, stay_id, caregiver_id, charttime, storetime, itemid, value, valuenum, valueuom, warning });
    }
    events
}

fn setup_sqlite(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS chartevents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject_id INTEGER NOT NULL, hadm_id INTEGER, stay_id INTEGER,
            charttime INTEGER NOT NULL, itemid INTEGER NOT NULL,
            value TEXT, valuenum REAL, valueuom TEXT);
        CREATE INDEX IF NOT EXISTS idx_ce_subject ON chartevents(subject_id);
        CREATE INDEX IF NOT EXISTS idx_ce_ts ON chartevents(charttime);
        CREATE INDEX IF NOT EXISTS idx_ce_item ON chartevents(itemid);
        CREATE INDEX IF NOT EXISTS idx_ce_subject_item ON chartevents(subject_id, itemid);
        PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;",
    ).expect("SQLite setup failed");
}

fn loinc_of(itemid: i64) -> &'static str {
    itemid_to_loinc(itemid).map(|(c, _, _)| c).unwrap_or("0")
}

// Query target chosen from the real data: patient with the most HR samples.
const Q_PATIENT: i64 = 10003400;
const Q_ITEMID: i64 = 220045; // Heart rate
const Q_LOINC: &str = "8867-4";
const Q_UNIT: &str = "/min";

fn main() {
    let csv_path = std::env::args().nth(1).unwrap_or_else(|| {
        "/tmp/mimic-demo/mimic-iv-clinical-database-demo-2.2/icu/chartevents.csv".to_string()
    });
    println!("=== MIMIC-IV REAL Demo Benchmark ===");
    println!("CSV: {csv_path}\n");

    let events = parse_real_chartevents(&csv_path);
    let total = events.len();
    println!("Parsed {total} chartevents rows (all itemids)");

    // Records mapped to known LOINC vitals (this is what every system ingests).
    let records: Vec<Record> = events.iter().filter_map(chartevent_to_record).collect();
    // The subset of raw events that map (for SQLite/TS/Influx we ingest the
    // same mapped subset so all four systems hold identical data).
    let mapped: Vec<&ChartEvent> = events.iter()
        .filter(|e| e.valuenum.is_some() && itemid_to_loinc(e.itemid).is_some())
        .collect();
    println!("Mapped {} rows to LOINC vitals (= rows ingested into every system)\n", records.len());

    let n_patients: usize = { let mut s: Vec<i64> = events.iter().map(|e| e.subject_id).collect(); s.sort(); s.dedup(); s.len() };

    // Derive query time windows from the real data for the target patient/HR.
    let mut hr_ts: Vec<i64> = mapped.iter()
        .filter(|e| e.subject_id == Q_PATIENT && e.itemid == Q_ITEMID)
        .map(|e| e.charttime).collect();
    hr_ts.sort();
    let hr_min = *hr_ts.first().expect("target patient has HR data");
    let hr_max = *hr_ts.last().unwrap();
    // 1h point window in the middle of the stay (guaranteed to hold rows)
    let q1_start = hr_ts[hr_ts.len() / 2];
    let q1_end = q1_start + 3600;
    // Full patient span
    let full_start = hr_min;
    let full_end = hr_max + 1;
    // Cohort 1h window: pick a busy global hour (use overall median HR ts)
    let mut all_hr: Vec<i64> = mapped.iter().filter(|e| e.itemid == Q_ITEMID).map(|e| e.charttime).collect();
    all_hr.sort();
    let cohort_start = all_hr[all_hr.len() / 2];
    let cohort_end = cohort_start + 3600;

    let mut out = Vec::new();
    out.push("system,metric,value,unit,detail".to_string());

    // ----- EmberDB + SQLite -----
    let (ember_tput, sqlite_tput, ember_bytes, sqlite_bytes,
         e_q1, s_q1, e_q2, s_q2, e_q3, s_q3, e_q4, s_q4) =
        bench_ember_sqlite(&records, &mapped, q1_start, q1_end, full_start, full_end,
                           cohort_start, cohort_end);

    out.push(format!("emberdb,ingest_throughput,{:.0},rec/s,{}", ember_tput, records.len()));
    out.push(format!("sqlite,ingest_throughput,{:.0},rec/s,{}", sqlite_tput, mapped.len()));
    out.push(format!("emberdb,query_single_vital_1h,{:.1},us,200", e_q1));
    out.push(format!("sqlite,query_single_vital_1h,{:.1},us,200", s_q1));
    out.push(format!("emberdb,query_full_patient_stay,{:.1},us,200", e_q2));
    out.push(format!("sqlite,query_full_patient_stay,{:.1},us,200", s_q2));
    out.push(format!("emberdb,query_cohort_vital_1h,{:.1},us,10", e_q3));
    out.push(format!("sqlite,query_cohort_vital_1h,{:.1},us,10", s_q3));
    out.push(format!("emberdb,query_latest_vital,{:.1},us,200", e_q4));
    out.push(format!("sqlite,query_latest_vital,{:.1},us,200", s_q4));
    out.push(format!("emberdb,storage_bytes,{},b,{}", ember_bytes, records.len()));
    out.push(format!("sqlite,storage_bytes,{},b,{}", sqlite_bytes, mapped.len()));

    // ----- TimescaleDB -----
    bench_timescale(&mapped, q1_start, q1_end, full_start, full_end, cohort_start, cohort_end, &mut out);

    // ----- InfluxDB -----
    bench_influx(&mapped, q1_start, q1_end, full_start, full_end, cohort_start, cohort_end, &mut out);

    // dataset provenance row
    out.push(format!("dataset,rows_total,{},rows,{}", total, "all_itemids"));
    out.push(format!("dataset,rows_mapped,{},rows,vitals", records.len()));
    out.push(format!("dataset,patients,{},count,subjects", n_patients));

    let out_path = "mimic_demo_results.csv";
    let mut f = fs::File::create(out_path).unwrap();
    for l in &out { writeln!(f, "{l}").unwrap(); }
    println!("\nResults written to {out_path}");
    println!("\n--- Summary ---");
    println!("Total rows: {total}  Mapped vitals rows: {}  Patients: {n_patients}", records.len());
}

#[allow(clippy::too_many_arguments)]
fn bench_ember_sqlite(
    records: &[Record], mapped: &[&ChartEvent],
    q1s: i64, q1e: i64, fs_: i64, fe: i64, cs: i64, ce: i64,
) -> (f64, f64, u64, u64, f64, f64, f64, f64, f64, f64, f64, f64) {
    let ember_dir = "/tmp/emberdb_mimic_real";
    let _ = fs::remove_dir_all(ember_dir);
    let config = test_config(ember_dir);
    let engine = StorageEngine::new(&config).unwrap();
    engine.set_debug_settings(true, true, Some(1000)).unwrap();

    println!("Ingesting {} records into EmberDB...", records.len());
    let start = Instant::now();
    for r in records { engine.insert(r.clone()).unwrap(); }
    let ember_ingest = start.elapsed();
    let ember_tput = records.len() as f64 / ember_ingest.as_secs_f64();
    println!("  EmberDB: {:.0} rec/s ({:.3}s)", ember_tput, ember_ingest.as_secs_f64());

    let sqlite_path = "/tmp/sqlite_mimic_real.db";
    let _ = fs::remove_file(sqlite_path);
    let conn = Connection::open(sqlite_path).unwrap();
    setup_sqlite(&conn);
    println!("Ingesting {} rows into SQLite...", mapped.len());
    let start = Instant::now();
    {
        let tx = conn.unchecked_transaction().unwrap();
        for e in mapped {
            tx.execute(
                "INSERT INTO chartevents (subject_id, hadm_id, stay_id, charttime, itemid, value, valuenum, valueuom) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                params![e.subject_id, e.hadm_id, e.stay_id, e.charttime, e.itemid, e.value, e.valuenum, e.valueuom],
            ).unwrap();
        }
        tx.commit().unwrap();
    }
    let sqlite_ingest = start.elapsed();
    let sqlite_tput = mapped.len() as f64 / sqlite_ingest.as_secs_f64();
    println!("  SQLite:  {:.0} rec/s ({:.3}s)", sqlite_tput, sqlite_ingest.as_secs_f64());

    let iters = 200;
    let metric = format!("{}|{}|{}", Q_PATIENT, Q_LOINC, Q_UNIT);

    // Q1
    let start = Instant::now();
    for _ in 0..iters { let _ = engine.query_range(q1s, q1e, &metric); }
    let e_q1 = start.elapsed().as_micros() as f64 / iters as f64;
    let start = Instant::now();
    for _ in 0..iters {
        let _: Vec<(i64, f64)> = conn.prepare_cached(
            "SELECT charttime, valuenum FROM chartevents WHERE subject_id=?1 AND itemid=?2 AND charttime BETWEEN ?3 AND ?4")
            .unwrap().query_map(params![Q_PATIENT, Q_ITEMID, q1s, q1e], |r| Ok((r.get::<_,i64>(0)?, r.get::<_,f64>(1)?)))
            .unwrap().filter_map(|r| r.ok()).collect();
    }
    let s_q1 = start.elapsed().as_micros() as f64 / iters as f64;

    // Q2 full patient stay (all mapped vitals)
    let vital_items: Vec<(&str, &str)> = vec![
        ("8867-4","/min"),("8480-6","mmHg"),("8462-4","mmHg"),("8478-0","mmHg"),
        ("2708-6","%"),("9279-1","/min"),("8310-5","degF"),("8310-5","degC"),
    ];
    let start = Instant::now();
    for _ in 0..iters {
        for (loinc, unit) in &vital_items {
            let m = format!("{}|{}|{}", Q_PATIENT, loinc, unit);
            let _ = engine.query_range(fs_, fe, &m);
        }
    }
    let e_q2 = start.elapsed().as_micros() as f64 / iters as f64;
    let start = Instant::now();
    for _ in 0..iters {
        let _: Vec<(i64,i64,f64)> = conn.prepare_cached(
            "SELECT charttime, itemid, valuenum FROM chartevents WHERE subject_id=?1 AND charttime BETWEEN ?2 AND ?3 ORDER BY charttime")
            .unwrap().query_map(params![Q_PATIENT, fs_, fe], |r| Ok((r.get::<_,i64>(0)?, r.get::<_,i64>(1)?, r.get::<_,f64>(2)?)))
            .unwrap().filter_map(|r| r.ok()).collect();
    }
    let s_q2 = start.elapsed().as_micros() as f64 / iters as f64;

    // Q3 cohort, single vital, 1h
    let citers = 10;
    let mut patients: Vec<i64> = mapped.iter().map(|e| e.subject_id).collect();
    patients.sort(); patients.dedup();
    let start = Instant::now();
    for _ in 0..citers {
        for p in &patients {
            let m = format!("{}|{}|{}", p, Q_LOINC, Q_UNIT);
            let _ = engine.query_range(cs, ce, &m);
        }
    }
    let e_q3 = start.elapsed().as_micros() as f64 / citers as f64;
    let start = Instant::now();
    for _ in 0..citers {
        let _: Vec<(i64,i64,f64)> = conn.prepare_cached(
            "SELECT subject_id, charttime, valuenum FROM chartevents WHERE itemid=?1 AND charttime BETWEEN ?2 AND ?3")
            .unwrap().query_map(params![Q_ITEMID, cs, ce], |r| Ok((r.get::<_,i64>(0)?, r.get::<_,i64>(1)?, r.get::<_,f64>(2)?)))
            .unwrap().filter_map(|r| r.ok()).collect();
    }
    let s_q3 = start.elapsed().as_micros() as f64 / citers as f64;

    // Q4 latest vital
    let start = Instant::now();
    for _ in 0..iters { let _ = engine.get_latest(&metric); }
    let e_q4 = start.elapsed().as_micros() as f64 / iters as f64;
    let start = Instant::now();
    for _ in 0..iters {
        let _: Option<(i64,f64)> = conn.prepare_cached(
            "SELECT charttime, valuenum FROM chartevents WHERE subject_id=?1 AND itemid=?2 ORDER BY charttime DESC LIMIT 1")
            .unwrap().query_row(params![Q_PATIENT, Q_ITEMID], |r| Ok((r.get::<_,i64>(0)?, r.get::<_,f64>(1)?))).ok();
    }
    let s_q4 = start.elapsed().as_micros() as f64 / iters as f64;

    engine.flush_all().unwrap();
    let ember_bytes = dir_size(std::path::Path::new(ember_dir)).unwrap_or(0);
    let sqlite_bytes = fs::metadata(sqlite_path).map(|m| m.len()).unwrap_or(0);

    println!("  EmberDB Q1={:.1}us Q2={:.1}us Q3={:.1}us Q4={:.1}us", e_q1, e_q2, e_q3, e_q4);
    println!("  SQLite  Q1={:.1}us Q2={:.1}us Q3={:.1}us Q4={:.1}us", s_q1, s_q2, s_q3, s_q4);
    println!("  Storage EmberDB={}B SQLite={}B", ember_bytes, sqlite_bytes);

    let _ = fs::remove_dir_all(ember_dir);
    let _ = fs::remove_file(sqlite_path);
    (ember_tput, sqlite_tput, ember_bytes, sqlite_bytes, e_q1, s_q1, e_q2, s_q2, e_q3, s_q3, e_q4, s_q4)
}

#[allow(clippy::too_many_arguments)]
fn bench_timescale(
    mapped: &[&ChartEvent], q1s: i64, q1e: i64, fs_: i64, fe: i64, cs: i64, ce: i64,
    out: &mut Vec<String>,
) {
    println!("\n=== TimescaleDB (pg16) ===");
    let mut client = match Client::connect(TS_CONN, NoTls) {
        Ok(c) => c,
        Err(e) => { println!("  unavailable: {e} -- skipping"); out.push("timescaledb,ingest_throughput,unavailable,,".into()); return; }
    };
    client.batch_execute(
        "CREATE EXTENSION IF NOT EXISTS timescaledb;
         DROP TABLE IF EXISTS chartevents;
         CREATE TABLE chartevents (
            charttime TIMESTAMPTZ NOT NULL, subject_id BIGINT NOT NULL,
            itemid BIGINT NOT NULL, loinc TEXT NOT NULL, valuenum DOUBLE PRECISION);
         SELECT create_hypertable('chartevents','charttime', chunk_time_interval => INTERVAL '30 days');",
    ).expect("ts schema");
    println!("  Ingesting {} rows via COPY...", mapped.len());
    let start = Instant::now();
    {
        let mut w = client.copy_in("COPY chartevents (charttime, subject_id, itemid, loinc, valuenum) FROM STDIN").unwrap();
        let mut line = String::with_capacity(64);
        for e in mapped {
            let v = match e.valuenum { Some(v) => v, None => continue };
            line.clear();
            line.push_str(&epoch_to_iso(e.charttime)); line.push('\t');
            line.push_str(&e.subject_id.to_string()); line.push('\t');
            line.push_str(&e.itemid.to_string()); line.push('\t');
            line.push_str(loinc_of(e.itemid)); line.push('\t');
            line.push_str(&format!("{v}")); line.push('\n');
            std::io::Write::write_all(&mut w, line.as_bytes()).unwrap();
        }
        w.finish().unwrap();
    }
    let ingest = start.elapsed();
    let tput = mapped.len() as f64 / ingest.as_secs_f64();
    println!("  ingest: {:.0} rec/s ({:.2}s)", tput, ingest.as_secs_f64());
    out.push(format!("timescaledb,ingest_throughput,{:.0},rec/s,{}", tput, mapped.len()));

    client.batch_execute(
        "CREATE INDEX idx_ce_subj_item ON chartevents(subject_id, itemid, charttime DESC);
         CREATE INDEX idx_ce_item ON chartevents(itemid, charttime); ANALYZE chartevents;").unwrap();

    let iters = 200;
    // Q1
    let sql = "SELECT charttime, valuenum FROM chartevents WHERE subject_id=$1 AND itemid=$2 AND charttime BETWEEN to_timestamp($3) AND to_timestamp($4)";
    let stmt = client.prepare(sql).unwrap();
    let (a, b) = (q1s as f64, q1e as f64);
    let start = Instant::now();
    for _ in 0..iters { let _ = client.query(&stmt, &[&Q_PATIENT, &Q_ITEMID, &a, &b]).unwrap(); }
    let us = start.elapsed().as_micros() as f64 / iters as f64;
    println!("  Q single_vital_1h: {us:.1}us"); out.push(format!("timescaledb,query_single_vital_1h,{us:.1},us,{iters}"));
    // Q2
    let sql = "SELECT charttime, itemid, valuenum FROM chartevents WHERE subject_id=$1 AND charttime BETWEEN to_timestamp($2) AND to_timestamp($3) ORDER BY charttime";
    let stmt = client.prepare(sql).unwrap();
    let (a, b) = (fs_ as f64, fe as f64);
    let start = Instant::now();
    for _ in 0..iters { let _ = client.query(&stmt, &[&Q_PATIENT, &a, &b]).unwrap(); }
    let us = start.elapsed().as_micros() as f64 / iters as f64;
    println!("  Q full_patient_stay: {us:.1}us"); out.push(format!("timescaledb,query_full_patient_stay,{us:.1},us,{iters}"));
    // Q3
    let sql = "SELECT subject_id, charttime, valuenum FROM chartevents WHERE itemid=$1 AND charttime BETWEEN to_timestamp($2) AND to_timestamp($3)";
    let stmt = client.prepare(sql).unwrap();
    let (a, b) = (cs as f64, ce as f64);
    let citers = 50;
    let start = Instant::now();
    for _ in 0..citers { let _ = client.query(&stmt, &[&Q_ITEMID, &a, &b]).unwrap(); }
    let us = start.elapsed().as_micros() as f64 / citers as f64;
    println!("  Q cohort_vital_1h: {us:.1}us"); out.push(format!("timescaledb,query_cohort_vital_1h,{us:.1},us,{citers}"));
    // Q4
    let sql = "SELECT charttime, valuenum FROM chartevents WHERE subject_id=$1 AND itemid=$2 ORDER BY charttime DESC LIMIT 1";
    let stmt = client.prepare(sql).unwrap();
    let start = Instant::now();
    for _ in 0..iters { let _ = client.query(&stmt, &[&Q_PATIENT, &Q_ITEMID]).unwrap(); }
    let us = start.elapsed().as_micros() as f64 / iters as f64;
    println!("  Q latest_vital: {us:.1}us"); out.push(format!("timescaledb,query_latest_vital,{us:.1},us,{iters}"));
    // Storage
    if let Ok(r) = client.query_one("SELECT hypertable_size('chartevents'::regclass)::BIGINT", &[]) {
        let bytes: i64 = r.get(0);
        println!("  Storage: {bytes} B"); out.push(format!("timescaledb,storage_bytes,{bytes},b,{}", mapped.len()));
    }
}

#[allow(clippy::too_many_arguments)]
fn bench_influx(
    mapped: &[&ChartEvent], q1s: i64, q1e: i64, fs_: i64, fe: i64, cs: i64, ce: i64,
    out: &mut Vec<String>,
) {
    println!("\n=== InfluxDB 2.7 ===");
    let avail = ureq::get(&format!("{INFLUX_URL}/health")).timeout(Duration::from_secs(5)).call().map(|r| r.status() == 200).unwrap_or(false);
    if !avail { println!("  unavailable -- skipping"); out.push("influxdb,ingest_throughput,unavailable,,".into()); return; }
    // clear prior data
    let _ = ureq::post(&format!("{INFLUX_URL}/api/v2/delete?org={INFLUX_ORG}&bucket={INFLUX_BUCKET}"))
        .set("Authorization", &format!("Token {INFLUX_TOKEN}")).set("Content-Type", "application/json")
        .timeout(Duration::from_secs(30))
        .send_string(r#"{"start":"1900-01-01T00:00:00Z","stop":"2300-01-01T00:00:00Z","predicate":"_measurement=\"vital\""}"#);

    println!("  Ingesting {} points...", mapped.len());
    let batch = 10_000usize;
    let mut buf = String::with_capacity(batch * 48);
    let mut n = 0;
    let start = Instant::now();
    for e in mapped {
        let v = match e.valuenum { Some(v) => v, None => continue };
        buf.push_str("vital,patient="); buf.push_str(&e.subject_id.to_string());
        buf.push_str(",code="); buf.push_str(loinc_of(e.itemid));
        buf.push_str(" value="); buf.push_str(&format!("{v}")); buf.push(' ');
        buf.push_str(&e.charttime.to_string()); buf.push('\n');
        n += 1;
        if n >= batch { if let Err(err) = influx_write(std::mem::take(&mut buf)) { println!("  batch failed: {err}"); out.push("influxdb,ingest_throughput,error,,".into()); return; } n = 0; }
    }
    if n > 0 { let _ = influx_write(std::mem::take(&mut buf)); }
    let ingest = start.elapsed();
    let tput = mapped.len() as f64 / ingest.as_secs_f64();
    println!("  ingest: {:.0} rec/s ({:.2}s)", tput, ingest.as_secs_f64());
    out.push(format!("influxdb,ingest_throughput,{:.0},rec/s,{}", tput, mapped.len()));
    std::thread::sleep(Duration::from_secs(3));

    let iters = 50;
    // Q1
    let flux = format!(r#"from(bucket:"vitals") |> range(start:{q1s},stop:{q1e}) |> filter(fn:(r)=>r._measurement=="vital" and r.patient=="{Q_PATIENT}" and r.code=="{Q_LOINC}")"#);
    let start = Instant::now(); for _ in 0..iters { let _ = influx_query(&flux); }
    let us = start.elapsed().as_micros() as f64 / iters as f64;
    println!("  Q single_vital_1h: {us:.1}us"); out.push(format!("influxdb,query_single_vital_1h,{us:.1},us,{iters}"));
    // Q2
    let flux = format!(r#"from(bucket:"vitals") |> range(start:{fs_},stop:{fe}) |> filter(fn:(r)=>r._measurement=="vital" and r.patient=="{Q_PATIENT}")"#);
    let start = Instant::now(); for _ in 0..iters { let _ = influx_query(&flux); }
    let us = start.elapsed().as_micros() as f64 / iters as f64;
    println!("  Q full_patient_stay: {us:.1}us"); out.push(format!("influxdb,query_full_patient_stay,{us:.1},us,{iters}"));
    // Q3
    let flux = format!(r#"from(bucket:"vitals") |> range(start:{cs},stop:{ce}) |> filter(fn:(r)=>r._measurement=="vital" and r.code=="{Q_LOINC}")"#);
    let citers = 10;
    let start = Instant::now(); for _ in 0..citers { let _ = influx_query(&flux); }
    let us = start.elapsed().as_micros() as f64 / citers as f64;
    println!("  Q cohort_vital_1h: {us:.1}us"); out.push(format!("influxdb,query_cohort_vital_1h,{us:.1},us,{citers}"));
    // Q4
    let flux = format!(r#"from(bucket:"vitals") |> range(start:-200y) |> filter(fn:(r)=>r._measurement=="vital" and r.patient=="{Q_PATIENT}" and r.code=="{Q_LOINC}") |> last()"#);
    let start = Instant::now(); for _ in 0..iters { let _ = influx_query(&flux); }
    let us = start.elapsed().as_micros() as f64 / iters as f64;
    println!("  Q latest_vital: {us:.1}us"); out.push(format!("influxdb,query_latest_vital,{us:.1},us,{iters}"));
    // Storage
    std::thread::sleep(Duration::from_secs(5));
    if let Ok(o) = std::process::Command::new("docker").args(["exec","influx-bench","du","-sb","/var/lib/influxdb2/engine"]).output() {
        if o.status.success() {
            let s = String::from_utf8_lossy(&o.stdout);
            if let Some(num) = s.split_whitespace().next() { if let Ok(bytes) = num.parse::<u64>() {
                println!("  Storage: {bytes} B"); out.push(format!("influxdb,storage_bytes,{bytes},b,{}", mapped.len()));
            }}
        }
    }
}

fn influx_write(body: String) -> Result<(), String> {
    let url = format!("{INFLUX_URL}/api/v2/write?org={INFLUX_ORG}&bucket={INFLUX_BUCKET}&precision=s");
    match ureq::post(&url).set("Authorization", &format!("Token {INFLUX_TOKEN}"))
        .set("Content-Type", "text/plain; charset=utf-8").timeout(Duration::from_secs(120)).send_string(&body) {
        Ok(r) if r.status()/100 == 2 => Ok(()),
        Ok(r) => Err(format!("status {}", r.status())),
        Err(e) => Err(format!("{e}")),
    }
}
fn influx_query(flux: &str) -> Result<usize, String> {
    let url = format!("{INFLUX_URL}/api/v2/query?org={INFLUX_ORG}");
    match ureq::post(&url).set("Authorization", &format!("Token {INFLUX_TOKEN}"))
        .set("Accept", "application/csv").set("Content-Type", "application/vnd.flux")
        .timeout(Duration::from_secs(120)).send_string(flux) {
        Ok(r) => { let t = r.into_string().map_err(|e| e.to_string())?; Ok(t.lines().filter(|l| l.contains(",_result")).count()) }
        Err(e) => Err(format!("{e}")),
    }
}

fn epoch_to_iso(epoch: i64) -> String {
    let days = epoch.div_euclid(86400);
    let sod = epoch.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}+00", y, m, d, sod/3600, (sod%3600)/60, sod%60)
}
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365*yoe + yoe/4 - yoe/100);
    let mp = (5*doy + 2)/153;
    let d = (doy - (153*mp+2)/5 + 1) as u32;
    let m = (if mp < 10 { mp+3 } else { mp-9 }) as u32;
    (if m <= 2 { y+1 } else { y }, m, d)
}

fn dir_size(path: &std::path::Path) -> std::io::Result<u64> {
    let mut total = 0;
    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?; let p = entry.path();
            if p.is_dir() { total += dir_size(&p)?; } else { total += entry.metadata()?.len(); }
        }
    }
    Ok(total)
}
