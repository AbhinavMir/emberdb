# EmberDB on the Real MIMIC-IV Clinical Database Demo

This report benchmarks EmberDB against three baselines on the **real**, open-access
MIMIC-IV Clinical Database Demo. No synthetic data is used here. All four systems
ingest and query the *same* real rows.

## 1. Dataset provenance

| Field | Value |
|---|---|
| Dataset | MIMIC-IV Clinical Database Demo |
| Version | 2.2 |
| Source | https://physionet.org/content/mimic-iv-demo/ |
| Download (artifact used) | https://physionet.org/static/published-projects/mimic-iv-demo/mimic-iv-clinical-database-demo-2.2.zip |
| License | **ODC Open Database License (ODbL)** — open access, no credentialing required (verified from bundled `LICENSE.txt`) |
| File benchmarked | `icu/chartevents.csv` (gunzipped from `icu/chartevents.csv.gz`) |
| SHA-256 of `chartevents.csv.gz` | `90f096ad3db847ed1aaec073f8d86053c51fea7b616d3b046410e030f083d560` (matches bundled `SHA256SUMS.txt`) |
| **Total chartevents rows** | **668,862** (all itemids) |
| **Distinct patients (subject_id)** | **100** |
| Distinct ICU stays (stay_id) | 140 |
| charttime span (de-identified) | 2110-04-11 to 2201-12-13 |
| **Rows mapped to known LOINC vitals** | **78,441** (all have a numeric `valuenum`) |

This is explicitly the **open ~100-patient DEMO subset**, *not* full MIMIC-IV
(which is ~300k+ patients and requires PhysioNet credentialing + a data use
agreement). Absolute throughput is therefore far lower than the 500-patient
synthetic run (1.73M synthetic rows vs 78k real mapped rows here); that is
expected and fine.

### Real schema vs. the loader

The real demo CSV has **11 columns** and ISO datetime strings:

```
subject_id,hadm_id,stay_id,caregiver_id,charttime,storetime,itemid,value,valuenum,valueuom,warning
```

The existing `src/mimic.rs::parse_chartevents_csv` assumes an **8-column**
positional layout with `charttime` as a Unix integer. That loader would have
mis-parsed the real file (the extra `caregiver_id`/`storetime` columns shift the
positions, and the ISO datetime would parse to 0). Rather than alter the
production loader (which the synthetic benches and unit tests depend on), the
real benchmark `benches/mimic_real_bench.rs` parses the real 11-column schema and
converts ISO `charttime` to epoch, then **reuses the production FHIR/LOINC mapping**
(`emberdb::mimic::{itemid_to_loinc, chartevent_to_record, ChartEvent}`) so the
record shape is identical to production. SHA-256 of the source file was verified
against the bundled checksum before use.

### Mapped vitals (the 78,441 rows every system ingests)

| itemid | LOINC | vital | rows |
|---|---|---|---|
| 220210 | 9279-1 | Respiratory rate | 13,913 |
| 220045 | 8867-4 | Heart rate | 13,913 |
| 220277 | 2708-6 | SpO2 | 13,540 |
| 220180 | 8462-4 | Non-invasive diastolic BP | 8,349 |
| 220179 | 8480-6 | Non-invasive systolic BP | 8,347 |
| 220052 | 8478-0 | Mean arterial pressure | 5,560 |
| 220050 | 8480-6 | Systolic BP | 5,525 |
| 220051 | 8462-4 | Diastolic BP | 5,524 |
| 223761 | 8310-5 | Temperature (degF) | 3,379 |
| 223762 | 8310-5 | Temperature (degC) | 391 |
| **Total** | | | **78,441** |

## 2. Four-system results on real data

Systems: EmberDB (in-process), SQLite (bundled, WAL), TimescaleDB pg16
(`tsdb-bench`, localhost:5433), InfluxDB 2.7 (`influx-bench`, localhost:8087).
All four ingest the **same 78,441 mapped vitals rows** and run the **same four
clinical query shapes** over windows derived from the real data (point patient
10003400 / heart rate, the busiest HR series).

**Ingest throughput (records/sec — higher is better)**

| System | rec/s | vs EmberDB |
|---|---|---|
| **EmberDB** | **1,944,141** | 1.0x |
| InfluxDB 2.7 | 328,673 | 5.9x slower |
| SQLite | 236,602 | 8.2x slower |
| TimescaleDB | 158,006 | 12.3x slower |

**Query latency (microseconds — lower is better)**

| Query | EmberDB | SQLite | TimescaleDB | InfluxDB |
|---|---|---|---|---|
| single_vital_1h (point) | **0.5** | 142.6 | 554.7 | 4,758.4 |
| cohort_vital_1h | **155.9** | 2,760.1 | 427.9 | 4,006.8 |
| full_patient_stay | 4,334.3 | **1,687.2** | 2,151.0 | 14,545.8 |
| latest_vital | 7,537.8 | **165.0** | 1,767.5 | 4,758.2 |

**Storage (bytes for the 78,441 rows — lower is better)**

| System | bytes | bytes/row |
|---|---|---|
| SQLite | 8,704,000 | 111.0 |
| TimescaleDB | 24,788,992 | 316.0 |
| InfluxDB | 57,123,354 | 728.2 |
| EmberDB | 0 (not measured) | — |

> EmberDB ran in in-memory/no-persistence debug mode (`set_debug_settings(true,
> true, ...)`), identical to the synthetic `mimic_bench`, so on-disk storage is
> reported as 0 in both runs. EmberDB on-disk footprint is **not measured** by
> this harness; the storage column is not a valid EmberDB comparison in either
> the synthetic or the real run. This is a known limitation carried over from the
> existing synthetic benchmark.

Raw machine-readable results: `mimic_demo_results.csv`.

## 3. Does the EmberDB win hold on real data?

**Partly — and the differences are explained by the real data's shape.**

| Metric | Synthetic (500 pts, 1.73M rows) | Real demo (100 pts, 78k rows) | Win holds? |
|---|---|---|---|
| Ingest | EmberDB 1.42M rec/s, 9.4x > SQLite | EmberDB 1.94M rec/s, 8.2x > SQLite; fastest of all four | **Yes** |
| single_vital_1h (point) | EmberDB 3.3us, 9.5x faster than SQLite | EmberDB 0.5us, ~285x faster than SQLite; fastest of all four | **Yes** |
| cohort_vital_1h | EmberDB 817us, 22.7x faster than SQLite | EmberDB 155.9us, fastest of all four | **Yes** |
| full_patient_stay | SQLite won (0.65x) | SQLite wins (EmberDB 4,334us vs SQLite 1,687us) | No (consistent with synthetic) |
| latest_vital | EmberDB 28us, 2.9x faster than SQLite | **SQLite wins** (165us vs EmberDB 7,538us) — regression vs synthetic | **No** |

**Headline: EmberDB still wins ingest and point/cohort queries on real data**,
and by a wider relative margin for the point query (the real demo is sparse, so a
1-hour window holds ~1 sample; EmberDB's hash+vec lookup answers in sub-microsecond
time while SQLite/Timescale/Influx pay fixed per-query overhead).

**Two honest regressions the real data exposes**, both rooted in the same cause —
the de-identified MIMIC timestamps are spread across a ~90-year span (2110–2201),
so EmberDB's hourly chunks are scattered across a very large key space:

- `latest_vital` flips from an EmberDB win (synthetic) to a loss. EmberDB's
  `get_latest` **scans every chunk** looking for the metric; with data sprayed
  across decades there are many chunks, each costing a hashmap miss, so it climbs
  to ~7.5ms. SQLite answers from its `(subject_id,itemid,charttime DESC)` index in
  165us. This is a genuine EmberDB design weakness surfaced only by realistically
  spread timestamps.
- `full_patient_stay` (8 metric range-scans per patient) loses to SQLite for the
  same reason — each `query_range` walks the chunk range across the wide span.
  This already lost in the synthetic run; the gap widens here.

### Caveats / notes

- **Sparse windows.** The real demo charts vitals roughly hourly, not every 5
  minutes like the synthetic generator. A 1-hour point window holds ~1 row (vs
  ~12 synthetic). All four systems query identical sparse windows on identical
  data, so the comparison is fair, but absolute query times are not comparable
  across the synthetic/real runs.
- **InfluxDB ingest variance.** Influx ingest measured 328k rec/s warm in the
  recorded run; a cold run (with the pre-delete) measured ~38k rec/s. EmberDB
  leads ingest in both cases.
- **Scale.** 78k mapped rows is ~22x smaller than the 1.73M synthetic rows, so
  all absolute throughputs are lower than the synthetic paper run; this is the
  expected consequence of using the open demo subset.

## 4. Reproduction

```bash
# 1. Acquire (open, no credentials)
curl -sSL -o /tmp/mimic-demo.zip \
  https://physionet.org/static/published-projects/mimic-iv-demo/mimic-iv-clinical-database-demo-2.2.zip
unzip -o /tmp/mimic-demo.zip \
  "mimic-iv-clinical-database-demo-2.2/icu/chartevents.csv.gz" -d /tmp/mimic-demo
gunzip -kf /tmp/mimic-demo/mimic-iv-clinical-database-demo-2.2/icu/chartevents.csv.gz

# 2. Baseline containers (already up as tsdb-bench:5433 / influx-bench:8087)

# 3. Run (CSV path is argv[1]; defaults to the path above)
cargo run --release --bin mimic_real_bench
# -> writes mimic_demo_results.csv
```
