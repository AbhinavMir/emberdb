//! Head-to-head baseline benchmark: InfluxDB and TimescaleDB
//!
//! Drives the SAME synthetic MIMIC-IV chartevents workload used by
//! `mimic_bench.rs` (500 patients, 48h ICU stays, 6 vitals @ 5-min cadence,
//! 1,728,000 events, seed 42) into purpose-built time-series stores running
//! in Docker, and measures write throughput, query latency, and storage.
//!
//! Targets (started via the commands in the paper/README):
//!   InfluxDB 2.7    http://localhost:8087   org=emberbench bucket=vitals
//!   TimescaleDB pg16 postgres://postgres:pw@localhost:5433/postgres
//!
//! Each store is exercised with the same four clinical query shapes as the
//! EmberDB/SQLite MIMIC comparison so the resulting table is apples-to-apples.
//!
//! Results are appended to `baseline_results.csv`. Stores that are not
//! reachable are skipped and recorded as `unavailable` rather than aborting
//! the whole run.

use emberdb::mimic::{ChartEvent, itemid_to_loinc};
use std::time::Instant;
use std::fs;
use std::io::Write as _;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use postgres::{Client, NoTls};

const INFLUX_URL: &str = "http://localhost:8087";
const INFLUX_ORG: &str = "emberbench";
const INFLUX_BUCKET: &str = "vitals";
const INFLUX_TOKEN: &str = "emberbenchtoken123456789";
const TS_CONN: &str = "host=localhost port=5433 user=postgres password=pw dbname=postgres";

const N_PATIENTS: usize = 500;
const SEED: u64 = 42;
const BASE_TS: i64 = 1_700_000_000;

/// Vital sign configuration: (itemid, mean, std, min_clamp, max_clamp)
/// Identical to mimic_bench.rs.
fn vital_configs() -> Vec<(i64, f64, f64, f64, f64)> {
    vec![
        (220045, 84.0, 17.0, 30.0, 200.0),   // Heart rate
        (220050, 121.0, 23.0, 60.0, 250.0),  // Systolic BP
        (220051, 70.0, 14.0, 30.0, 150.0),   // Diastolic BP
        (220277, 96.8, 2.8, 70.0, 100.0),    // SpO2
        (220210, 18.0, 4.0, 6.0, 45.0),      // Respiratory rate
        (223761, 98.6, 1.2, 95.0, 105.0),    // Temperature (F)
    ]
}

/// Generate synthetic MIMIC-IV chartevents.
/// Byte-for-byte the same generator as mimic_bench.rs (same RNG stream).
fn generate_synthetic_chartevents(n_patients: usize, seed: u64) -> Vec<ChartEvent> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut events = Vec::new();
    let vitals = vital_configs();
    let base_ts: i64 = BASE_TS;

    for p in 0..n_patients {
        let subject_id = 10000 + p as i64;
        let hadm_id = 20000 + p as i64;
        let stay_id = 30000 + p as i64;
        let admit_ts = base_ts + (p as i64 * 300);

        let duration_min = 48 * 60;
        let interval_min = 5;
        let n_measurements = duration_min / interval_min;

        for (itemid, mean, std, min_c, max_c) in &vitals {
            for m in 0..n_measurements {
                let charttime = admit_ts + (m as i64 * interval_min as i64 * 60);
                let u1: f64 = rng.gen::<f64>().max(1e-10);
                let u2: f64 = rng.gen::<f64>();
                let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
                let raw_val = mean + std * z;
                let val = raw_val.max(*min_c).min(*max_c);

                events.push(ChartEvent {
                    subject_id,
                    hadm_id: Some(hadm_id),
                    stay_id: Some(stay_id),
                    charttime,
                    itemid: *itemid,
                    value: Some(format!("{:.1}", val)),
                    valuenum: Some(val),
                    valueuom: itemid_to_loinc(*itemid).map(|(_, _, u)| u.to_string()),
                    ..Default::default()
                });
            }
        }
    }
    events
}

// ----------------------------------------------------------------------------
// InfluxDB
// ----------------------------------------------------------------------------

fn influx_available() -> bool {
    ureq::get(&format!("{}/health", INFLUX_URL))
        .timeout(std::time::Duration::from_secs(5))
        .call()
        .map(|r| r.status() == 200)
        .unwrap_or(false)
}

fn influx_write_line(body: String) -> Result<(), String> {
    let url = format!(
        "{}/api/v2/write?org={}&bucket={}&precision=s",
        INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET
    );
    let resp = ureq::post(&url)
        .set("Authorization", &format!("Token {}", INFLUX_TOKEN))
        .set("Content-Type", "text/plain; charset=utf-8")
        .timeout(std::time::Duration::from_secs(120))
        .send_string(&body);
    match resp {
        Ok(r) if r.status() / 100 == 2 => Ok(()),
        Ok(r) => Err(format!("write status {}", r.status())),
        Err(e) => Err(format!("write error {}", e)),
    }
}

fn influx_query(flux: &str) -> Result<usize, String> {
    let url = format!("{}/api/v2/query?org={}", INFLUX_URL, INFLUX_ORG);
    let resp = ureq::post(&url)
        .set("Authorization", &format!("Token {}", INFLUX_TOKEN))
        .set("Accept", "application/csv")
        .set("Content-Type", "application/vnd.flux")
        .timeout(std::time::Duration::from_secs(120))
        .send_string(flux);
    match resp {
        Ok(r) => {
            let text = r.into_string().map_err(|e| e.to_string())?;
            // count non-empty, non-annotation data rows
            let rows = text
                .lines()
                .filter(|l| l.starts_with(",_result") || (l.starts_with(",") && l.contains(",_result")))
                .count();
            Ok(rows)
        }
        Err(e) => Err(format!("query error {}", e)),
    }
}

/// Map itemid -> influx tag `code`
fn loinc_of(itemid: i64) -> &'static str {
    itemid_to_loinc(itemid).map(|(c, _, _)| c).unwrap_or("0")
}

fn bench_influx(events: &[ChartEvent], csv: &mut Vec<String>) {
    println!("\n=== InfluxDB 2.7 ===");
    if !influx_available() {
        println!("  InfluxDB unavailable at {} -- skipping", INFLUX_URL);
        csv.push("influxdb,ingest_throughput,unavailable,,".to_string());
        return;
    }

    // Clear any prior run's data so storage numbers are from this run alone.
    let _ = ureq::post(&format!(
        "{}/api/v2/delete?org={}&bucket={}",
        INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET
    ))
    .set("Authorization", &format!("Token {}", INFLUX_TOKEN))
    .set("Content-Type", "application/json")
    .timeout(std::time::Duration::from_secs(30))
    .send_string(
        r#"{"start":"1900-01-01T00:00:00Z","stop":"2100-01-01T00:00:00Z","predicate":"_measurement=\"vital\""}"#,
    );

    // Ingest: line protocol, batched. measurement=vital, tags patient+code, field value.
    println!("  Ingesting {} points via line protocol...", events.len());
    let batch_lines = 10_000usize;
    let mut buf = String::with_capacity(batch_lines * 48);
    let mut n_in_batch = 0usize;
    let start = Instant::now();
    for e in events {
        let v = match e.valuenum { Some(v) => v, None => continue };
        // line protocol: vital,patient=10000,code=8867-4 value=84.0 1700000000
        buf.push_str("vital,patient=");
        buf.push_str(&e.subject_id.to_string());
        buf.push_str(",code=");
        buf.push_str(loinc_of(e.itemid));
        buf.push_str(" value=");
        buf.push_str(&format!("{}", v));
        buf.push(' ');
        buf.push_str(&e.charttime.to_string());
        buf.push('\n');
        n_in_batch += 1;
        if n_in_batch >= batch_lines {
            if let Err(err) = influx_write_line(std::mem::take(&mut buf)) {
                println!("  ingest batch failed: {}", err);
                csv.push("influxdb,ingest_throughput,error,,".to_string());
                return;
            }
            n_in_batch = 0;
        }
    }
    if n_in_batch > 0 {
        if let Err(err) = influx_write_line(std::mem::take(&mut buf)) {
            println!("  final ingest batch failed: {}", err);
        }
    }
    let ingest = start.elapsed();
    let tput = events.len() as f64 / ingest.as_secs_f64();
    println!("  InfluxDB ingest: {:.0} rec/s ({:.2}s)", tput, ingest.as_secs_f64());
    csv.push(format!("influxdb,ingest_throughput,{:.0},{:.2}s,{}", tput, ingest.as_secs_f64(), events.len()));

    // Influx persists asynchronously; give the WAL a moment to flush to TSM.
    std::thread::sleep(std::time::Duration::from_secs(3));

    let iters = 50;

    // Q1: single patient, single vital (HR), 1h window
    {
        let t0 = BASE_TS + 40_000;
        let t1 = t0 + 3600;
        let flux = format!(
            r#"from(bucket:"vitals") |> range(start:{},stop:{}) |> filter(fn:(r)=>r._measurement=="vital" and r.patient=="10050" and r.code=="8867-4")"#,
            t0, t1
        );
        let start = Instant::now();
        for _ in 0..iters { let _ = influx_query(&flux); }
        let us = start.elapsed().as_micros() as f64 / iters as f64;
        println!("  Q single_vital_1h: {:.1} us", us);
        csv.push(format!("influxdb,query_single_vital_1h,{:.1},us,{}", us, iters));
    }

    // Q2: single patient, all vitals, full 48h stay
    {
        let t0 = BASE_TS;
        let t1 = BASE_TS + 48 * 3600 + 500 * 300;
        let flux = format!(
            r#"from(bucket:"vitals") |> range(start:{},stop:{}) |> filter(fn:(r)=>r._measurement=="vital" and r.patient=="10050")"#,
            t0, t1
        );
        let start = Instant::now();
        for _ in 0..iters { let _ = influx_query(&flux); }
        let us = start.elapsed().as_micros() as f64 / iters as f64;
        println!("  Q full_patient_stay: {:.1} us", us);
        csv.push(format!("influxdb,query_full_patient_stay,{:.1},us,{}", us, iters));
    }

    // Q3: cohort, single vital (HR), 1h window, all patients
    {
        let t0 = BASE_TS + 20_000;
        let t1 = t0 + 3600;
        let flux = format!(
            r#"from(bucket:"vitals") |> range(start:{},stop:{}) |> filter(fn:(r)=>r._measurement=="vital" and r.code=="8867-4")"#,
            t0, t1
        );
        let citers = 10;
        let start = Instant::now();
        for _ in 0..citers { let _ = influx_query(&flux); }
        let us = start.elapsed().as_micros() as f64 / citers as f64;
        println!("  Q cohort_vital_1h: {:.1} us", us);
        csv.push(format!("influxdb,query_cohort_vital_1h,{:.1},us,{}", us, citers));
    }

    // Q4: latest vital for a patient
    {
        let flux = r#"from(bucket:"vitals") |> range(start:-100y) |> filter(fn:(r)=>r._measurement=="vital" and r.patient=="10050" and r.code=="8867-4") |> last()"#;
        let start = Instant::now();
        for _ in 0..iters { let _ = influx_query(flux); }
        let us = start.elapsed().as_micros() as f64 / iters as f64;
        println!("  Q latest_vital: {:.1} us", us);
        csv.push(format!("influxdb,query_latest_vital,{:.1},us,{}", us, iters));
    }

    // Storage: on-disk size of the TSM engine dir inside the container.
    // Influx flushes the WAL to TSM lazily; allow a flush window then measure.
    std::thread::sleep(std::time::Duration::from_secs(5));
    match std::process::Command::new("docker")
        .args(["exec", "influx-bench", "du", "-sb", "/var/lib/influxdb2/engine"])
        .output()
    {
        Ok(out) if out.status.success() => {
            let s = String::from_utf8_lossy(&out.stdout);
            if let Some(num) = s.split_whitespace().next() {
                if let Ok(bytes) = num.parse::<u64>() {
                    let per_rec = bytes as f64 / events.len() as f64;
                    println!("  Storage: {} bytes total ({:.1} bytes/rec)", bytes, per_rec);
                    csv.push(format!("influxdb,storage_bytes,{},{:.1}b/rec,{}", bytes, per_rec, events.len()));
                }
            }
        }
        _ => {
            println!("  Storage: could not read container engine dir");
            csv.push(format!("influxdb,storage_bytes,unavailable,,{}", events.len()));
        }
    }
}

// ----------------------------------------------------------------------------
// TimescaleDB
// ----------------------------------------------------------------------------

fn bench_timescale(events: &[ChartEvent], csv: &mut Vec<String>) {
    println!("\n=== TimescaleDB (pg16) ===");
    let mut client = match Client::connect(TS_CONN, NoTls) {
        Ok(c) => c,
        Err(e) => {
            println!("  TimescaleDB unavailable: {} -- skipping", e);
            csv.push("timescaledb,ingest_throughput,unavailable,,".to_string());
            return;
        }
    };

    // Fresh schema + hypertable.
    client.batch_execute(
        "CREATE EXTENSION IF NOT EXISTS timescaledb;
         DROP TABLE IF EXISTS chartevents;
         CREATE TABLE chartevents (
            charttime   TIMESTAMPTZ NOT NULL,
            subject_id  BIGINT      NOT NULL,
            itemid      BIGINT      NOT NULL,
            loinc       TEXT        NOT NULL,
            valuenum    DOUBLE PRECISION
         );
         SELECT create_hypertable('chartevents','charttime', chunk_time_interval => INTERVAL '1 hour');
         "
    ).expect("timescale schema setup failed");

    // Ingest via COPY (binary-ish text COPY is the fast Timescale path).
    println!("  Ingesting {} rows via COPY...", events.len());
    let start = Instant::now();
    {
        let writer = client
            .copy_in("COPY chartevents (charttime, subject_id, itemid, loinc, valuenum) FROM STDIN")
            .expect("copy_in failed");
        let mut w = writer;
        let mut line = String::with_capacity(64);
        for e in events {
            let v = match e.valuenum { Some(v) => v, None => continue };
            line.clear();
            // charttime as 'to_timestamp' compatible? COPY text wants a timestamptz literal.
            // Use epoch seconds rendered via Postgres-accepted 'epoch' format is not valid in COPY;
            // emit ISO-ish using the integer epoch through a numeric -> we instead store as
            // 'YYYY-...' is overkill; Postgres COPY accepts 'epoch'+interval only via expression.
            // Simplest robust path: emit the value as a bigint epoch and cast column later is not
            // possible mid-COPY, so format an ISO timestamp from the epoch.
            let secs = e.charttime;
            line.push_str(&epoch_to_iso(secs));
            line.push('\t');
            line.push_str(&e.subject_id.to_string());
            line.push('\t');
            line.push_str(&e.itemid.to_string());
            line.push('\t');
            line.push_str(loinc_of(e.itemid));
            line.push('\t');
            line.push_str(&format!("{}", v));
            line.push('\n');
            std::io::Write::write_all(&mut w, line.as_bytes()).expect("copy write failed");
        }
        w.finish().expect("copy finish failed");
    }
    let ingest = start.elapsed();
    let tput = events.len() as f64 / ingest.as_secs_f64();
    println!("  TimescaleDB ingest: {:.0} rec/s ({:.2}s)", tput, ingest.as_secs_f64());
    csv.push(format!("timescaledb,ingest_throughput,{:.0},{:.2}s,{}", tput, ingest.as_secs_f64(), events.len()));

    // Add indexes used for point/range/cohort queries (created post-load, the
    // standard bulk-load pattern). Timescale auto-indexes time; add composite.
    client.batch_execute(
        "CREATE INDEX idx_ce_subj_item ON chartevents(subject_id, itemid, charttime DESC);
         CREATE INDEX idx_ce_item ON chartevents(itemid, charttime);
         ANALYZE chartevents;"
    ).expect("index creation failed");

    let iters = 200;

    // Q1: single patient, single vital, 1h
    {
        let t0 = (BASE_TS + 40_000) as f64;
        let t1 = (BASE_TS + 40_000 + 3600) as f64;
        let sql = "SELECT charttime, valuenum FROM chartevents WHERE subject_id=$1 AND itemid=$2 AND charttime BETWEEN to_timestamp($3) AND to_timestamp($4)";
        let stmt = client.prepare(sql).unwrap();
        let start = Instant::now();
        for _ in 0..iters {
            let _ = client.query(&stmt, &[&10050i64, &220045i64, &t0, &t1]).unwrap();
        }
        let us = start.elapsed().as_micros() as f64 / iters as f64;
        println!("  Q single_vital_1h: {:.1} us", us);
        csv.push(format!("timescaledb,query_single_vital_1h,{:.1},us,{}", us, iters));
    }

    // Q2: full patient stay, all vitals
    {
        let t0 = BASE_TS as f64;
        let t1 = (BASE_TS + 48 * 3600 + 500 * 300) as f64;
        let sql = "SELECT charttime, itemid, valuenum FROM chartevents WHERE subject_id=$1 AND charttime BETWEEN to_timestamp($2) AND to_timestamp($3) ORDER BY charttime";
        let stmt = client.prepare(sql).unwrap();
        let start = Instant::now();
        for _ in 0..iters {
            let _ = client.query(&stmt, &[&10050i64, &t0, &t1]).unwrap();
        }
        let us = start.elapsed().as_micros() as f64 / iters as f64;
        println!("  Q full_patient_stay: {:.1} us", us);
        csv.push(format!("timescaledb,query_full_patient_stay,{:.1},us,{}", us, iters));
    }

    // Q3: cohort, single vital, 1h
    {
        let t0 = (BASE_TS + 20_000) as f64;
        let t1 = (BASE_TS + 20_000 + 3600) as f64;
        let sql = "SELECT subject_id, charttime, valuenum FROM chartevents WHERE itemid=$1 AND charttime BETWEEN to_timestamp($2) AND to_timestamp($3)";
        let stmt = client.prepare(sql).unwrap();
        let citers = 50;
        let start = Instant::now();
        for _ in 0..citers {
            let _ = client.query(&stmt, &[&220045i64, &t0, &t1]).unwrap();
        }
        let us = start.elapsed().as_micros() as f64 / citers as f64;
        println!("  Q cohort_vital_1h: {:.1} us", us);
        csv.push(format!("timescaledb,query_cohort_vital_1h,{:.1},us,{}", us, citers));
    }

    // Q4: latest vital
    {
        let sql = "SELECT charttime, valuenum FROM chartevents WHERE subject_id=$1 AND itemid=$2 ORDER BY charttime DESC LIMIT 1";
        let stmt = client.prepare(sql).unwrap();
        let start = Instant::now();
        for _ in 0..iters {
            let _ = client.query(&stmt, &[&10050i64, &220045i64]).unwrap();
        }
        let us = start.elapsed().as_micros() as f64 / iters as f64;
        println!("  Q latest_vital: {:.1} us", us);
        csv.push(format!("timescaledb,query_latest_vital,{:.1},us,{}", us, iters));
    }

    // Storage: total on-disk size of the hypertable (table + indexes + chunks).
    {
        let row = client
            .query_one(
                "SELECT hypertable_size('chartevents'::regclass)::BIGINT",
                &[],
            )
            .ok();
        if let Some(r) = row {
            let bytes: i64 = r.get(0);
            let per_rec = bytes as f64 / events.len() as f64;
            println!("  Storage: {} bytes total ({:.1} bytes/rec)", bytes, per_rec);
            csv.push(format!("timescaledb,storage_bytes,{},{:.1}b/rec,{}", bytes, per_rec, events.len()));
        }
    }
}

/// Render a unix-epoch second count as an ISO-8601 UTC timestamp that
/// Postgres/COPY accepts. Avoids pulling in chrono formatting paths.
fn epoch_to_iso(epoch: i64) -> String {
    // days/time decomposition (proleptic Gregorian, civil-from-days algorithm)
    let days = epoch.div_euclid(86400);
    let secs_of_day = epoch.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    let hh = secs_of_day / 3600;
    let mm = (secs_of_day % 3600) / 60;
    let ss = secs_of_day % 60;
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}+00", y, m, d, hh, mm, ss)
}

/// Howard Hinnant's civil_from_days: days since 1970-01-01 -> (y, m, d).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn main() {
    println!("=== EmberDB Baseline Benchmark: InfluxDB + TimescaleDB ===");
    println!("Generating synthetic MIMIC-IV workload ({} patients, seed {})...", N_PATIENTS, SEED);
    let events = generate_synthetic_chartevents(N_PATIENTS, SEED);
    println!("Generated {} chartevents (6 vitals, 48h, 5-min cadence)\n", events.len());

    let mut csv = Vec::new();
    csv.push("system,metric,value,unit,detail".to_string());

    bench_timescale(&events, &mut csv);
    bench_influx(&events, &mut csv);

    let out_path = "baseline_results.csv";
    let mut f = fs::File::create(out_path).expect("Failed to create CSV");
    for line in &csv {
        writeln!(f, "{}", line).unwrap();
    }
    println!("\nResults written to {}", out_path);
}
