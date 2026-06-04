//! MIMIC-IV data loader module
//!
//! Reads MIMIC-IV chartevents CSV format and converts to EmberDB Records
//! via FHIR Observation intermediaries.

use std::collections::HashMap;
use std::io::BufRead;
use std::path::Path;
use crate::storage::Record;

/// Mapping from MIMIC-IV itemid to (LOINC code, display name, unit)
pub fn itemid_to_loinc(itemid: i64) -> Option<(&'static str, &'static str, &'static str)> {
    match itemid {
        220045 => Some(("8867-4", "Heart rate", "/min")),
        220050 => Some(("8480-6", "Systolic blood pressure", "mmHg")),
        220051 => Some(("8462-4", "Diastolic blood pressure", "mmHg")),
        220052 => Some(("8478-0", "Mean arterial pressure", "mmHg")),
        220179 => Some(("8480-6", "Non-invasive systolic BP", "mmHg")),
        220180 => Some(("8462-4", "Non-invasive diastolic BP", "mmHg")),
        220210 => Some(("9279-1", "Respiratory rate", "/min")),
        220277 => Some(("2708-6", "SpO2", "%")),
        223761 => Some(("8310-5", "Temperature", "degF")),
        223762 => Some(("8310-5", "Temperature", "degC")),
        _ => None,
    }
}

/// A single row from MIMIC-IV chartevents.
///
/// Mirrors the real MIMIC-IV `icu/chartevents` schema (11 columns):
/// `subject_id, hadm_id, stay_id, caregiver_id, charttime, storetime,
///  itemid, value, valuenum, valueuom, warning`.
///
/// `charttime`/`storetime` are held as Unix epoch seconds internally; on the
/// wire (CSV) they are the human-readable `YYYY-MM-DD HH:MM:SS` form that real
/// MIMIC uses (see `epoch_to_iso` / `iso_to_epoch`). `caregiver_id`,
/// `storetime`, and `warning` are optional, so the legacy 8-column synthetic
/// layout still round-trips (they parse to `None`).
#[derive(Debug, Clone, Default)]
pub struct ChartEvent {
    pub subject_id: i64,
    pub hadm_id: Option<i64>,
    pub stay_id: Option<i64>,
    pub caregiver_id: Option<i64>,
    pub charttime: i64,         // Unix epoch seconds
    pub storetime: Option<i64>, // Unix epoch seconds (when charted into the system)
    pub itemid: i64,
    pub value: Option<String>,
    pub valuenum: Option<f64>,
    pub valueuom: Option<String>,
    pub warning: Option<i64>,
}

/// Parse a charttime/storetime cell **exactly**, accepting either form MIMIC
/// data appears in:
///
/// * an integer Unix epoch in seconds (the synthetic / legacy layout), or
/// * a `YYYY-MM-DD HH:MM:SS` datetime string (the real MIMIC-IV layout).
///
/// Returns `None` if the cell is neither (e.g. empty `storetime`).
pub fn parse_charttime(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        None
    } else if let Ok(epoch) = s.parse::<i64>() {
        // Integer Unix epoch (synthetic / legacy).
        Some(epoch)
    } else if let Some(epoch) = iso_to_epoch(s) {
        // Human-readable `YYYY-MM-DD HH:MM:SS` (real MIMIC).
        Some(epoch)
    } else {
        None
    }
}

/// Parse a `YYYY-MM-DD HH:MM:SS` datetime into Unix epoch seconds (UTC,
/// proleptic Gregorian). MIMIC timestamps carry no zone and are de-identified
/// into the future; treating them as UTC is exact for ordering and windowing.
pub fn iso_to_epoch(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.len() < 19 {
        return None;
    }
    let year: i64 = s.get(0..4)?.parse().ok()?;
    let month: u32 = s.get(5..7)?.parse().ok()?;
    let day: u32 = s.get(8..10)?.parse().ok()?;
    let hh: i64 = s.get(11..13)?.parse().ok()?;
    let mm: i64 = s.get(14..16)?.parse().ok()?;
    let ss: i64 = s.get(17..19)?.parse().ok()?;
    Some(days_from_civil(year, month, day) * 86_400 + hh * 3_600 + mm * 60 + ss)
}

/// Format Unix epoch seconds as MIMIC's human-readable `YYYY-MM-DD HH:MM:SS`.
/// Inverse of `iso_to_epoch`.
pub fn epoch_to_iso(epoch: i64) -> String {
    let days = epoch.div_euclid(86_400);
    let secs = epoch.rem_euclid(86_400);
    let (y, m, d) = days_to_civil(days);
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        y,
        m,
        d,
        secs / 3_600,
        (secs % 3_600) / 60,
        secs % 60
    )
}

/// Howard Hinnant's `days_from_civil`: (y, m, d) -> days since 1970-01-01.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let m = m as i64;
    let d = d as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Inverse of `days_from_civil`: days since 1970-01-01 -> (y, m, d).
fn days_to_civil(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Split a CSV line honoring simple double-quoted fields, so a comma inside a
/// quoted free-text value never shifts column positions.
fn split_csv(line: &str) -> Vec<String> {
    let mut out = Vec::with_capacity(11);
    let mut cur = String::new();
    let mut in_q = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                if in_q && chars.peek() == Some(&'"') {
                    cur.push('"');
                    chars.next();
                } else {
                    in_q = !in_q;
                }
            }
            ',' if !in_q => out.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    out.push(cur);
    out
}

fn non_empty(s: &str) -> Option<String> {
    let v = s.trim();
    if v.is_empty() {
        None
    } else {
        Some(v.to_string())
    }
}

fn csv_quote(s: &str) -> String {
    if s.contains(',') || s.contains('"') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Parse a chartevents CSV into `ChartEvent` rows. Handles **both** layouts,
/// dispatching on the column count:
///
/// * Real MIMIC-IV (11 columns):
///   `subject_id, hadm_id, stay_id, caregiver_id, charttime, storetime,
///    itemid, value, valuenum, valueuom, warning`
/// * Legacy synthetic (8 columns):
///   `subject_id, hadm_id, stay_id, charttime, itemid, value, valuenum, valueuom`
///
/// `charttime`/`storetime` may each be an integer Unix epoch *or* a
/// `YYYY-MM-DD HH:MM:SS` datetime string; `parse_charttime` handles both
/// exactly. Rows whose charttime parses to neither form are skipped.
pub fn parse_chartevents_csv<P: AsRef<Path>>(path: P) -> Result<Vec<ChartEvent>, String> {
    let file = std::fs::File::open(path.as_ref())
        .map_err(|e| format!("Failed to open CSV: {}", e))?;
    let reader = std::io::BufReader::new(file);
    let mut events = Vec::new();
    let mut lines = reader.lines();

    // Skip header
    let _header = lines.next();

    for line_result in lines {
        let line = line_result.map_err(|e| format!("Read error: {}", e))?;
        if line.is_empty() {
            continue;
        }
        let fields = split_csv(&line);

        let event = if fields.len() >= 11 {
            // Real MIMIC-IV 11-column layout.
            let charttime = match parse_charttime(&fields[4]) {
                Some(v) => v,
                None => continue,
            };
            ChartEvent {
                subject_id: fields[0].trim().parse::<i64>().unwrap_or(0),
                hadm_id: fields[1].trim().parse::<i64>().ok(),
                stay_id: fields[2].trim().parse::<i64>().ok(),
                caregiver_id: fields[3].trim().parse::<i64>().ok(),
                charttime,
                storetime: parse_charttime(&fields[5]),
                itemid: fields[6].trim().parse::<i64>().unwrap_or(0),
                value: non_empty(&fields[7]),
                valuenum: fields[8].trim().parse::<f64>().ok(),
                valueuom: non_empty(&fields[9]),
                warning: fields[10].trim().parse::<i64>().ok(),
            }
        } else if fields.len() >= 8 {
            // Legacy synthetic 8-column layout.
            let charttime = match parse_charttime(&fields[3]) {
                Some(v) => v,
                None => continue,
            };
            ChartEvent {
                subject_id: fields[0].trim().parse::<i64>().unwrap_or(0),
                hadm_id: fields[1].trim().parse::<i64>().ok(),
                stay_id: fields[2].trim().parse::<i64>().ok(),
                caregiver_id: None,
                charttime,
                storetime: None,
                itemid: fields[4].trim().parse::<i64>().unwrap_or(0),
                value: non_empty(&fields[5]),
                valuenum: fields[6].trim().parse::<f64>().ok(),
                valueuom: non_empty(&fields[7]),
                warning: None,
            }
        } else {
            continue;
        };

        events.push(event);
    }

    Ok(events)
}

/// Serialize `ChartEvent`s to the real MIMIC-IV 11-column CSV shape, with
/// human-readable `YYYY-MM-DD HH:MM:SS` timestamps. Output round-trips through
/// `parse_chartevents_csv`.
pub fn write_chartevents_csv<P: AsRef<Path>>(path: P, events: &[ChartEvent]) -> Result<(), String> {
    use std::io::Write;
    let mut f = std::fs::File::create(path.as_ref())
        .map_err(|e| format!("Failed to create CSV: {}", e))?;
    writeln!(
        f,
        "subject_id,hadm_id,stay_id,caregiver_id,charttime,storetime,itemid,value,valuenum,valueuom,warning"
    )
    .map_err(|e| e.to_string())?;
    let cell_i = |o: Option<i64>| o.map(|v| v.to_string()).unwrap_or_default();
    for e in events {
        writeln!(
            f,
            "{},{},{},{},{},{},{},{},{},{},{}",
            e.subject_id,
            cell_i(e.hadm_id),
            cell_i(e.stay_id),
            cell_i(e.caregiver_id),
            epoch_to_iso(e.charttime),
            e.storetime.map(epoch_to_iso).unwrap_or_default(),
            e.itemid,
            csv_quote(e.value.as_deref().unwrap_or("")),
            e.valuenum.map(|v| format!("{}", v)).unwrap_or_default(),
            csv_quote(e.valueuom.as_deref().unwrap_or("")),
            cell_i(e.warning),
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Convert a ChartEvent to an EmberDB Record using FHIR LOINC mapping.
/// Returns None if the itemid is not mapped or valuenum is missing.
pub fn chartevent_to_record(event: &ChartEvent) -> Option<Record> {
    let valuenum = event.valuenum?;
    let (loinc_code, _display, unit) = itemid_to_loinc(event.itemid)?;

    let mut context = HashMap::new();
    if let Some(hadm_id) = event.hadm_id {
        context.insert("hadm_id".to_string(), hadm_id.to_string());
    }
    if let Some(stay_id) = event.stay_id {
        context.insert("stay_id".to_string(), stay_id.to_string());
    }
    if let Some(ref uom) = event.valueuom {
        context.insert("valueuom".to_string(), uom.clone());
    }
    context.insert("itemid".to_string(), event.itemid.to_string());

    let metric_name = format!("{}|{}|{}", event.subject_id, loinc_code, unit);

    Some(Record {
        timestamp: event.charttime,
        metric_name,
        value: valuenum,
        context,
        resource_type: "Observation".to_string(),
    })
}

/// Convert a batch of ChartEvents to Records, skipping unmapped items.
pub fn chartevents_to_records(events: &[ChartEvent]) -> Vec<Record> {
    events.iter().filter_map(chartevent_to_record).collect()
}

/// Load a MIMIC-IV chartevents CSV directly into Records.
pub fn load_chartevents<P: AsRef<Path>>(path: P) -> Result<Vec<Record>, String> {
    let events = parse_chartevents_csv(path)?;
    Ok(chartevents_to_records(&events))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_itemid_to_loinc() {
        assert!(itemid_to_loinc(220045).is_some());
        assert_eq!(itemid_to_loinc(220045).unwrap().0, "8867-4");
        assert!(itemid_to_loinc(999999).is_none());
    }

    #[test]
    fn test_chartevent_to_record() {
        let event = ChartEvent {
            subject_id: 12345,
            hadm_id: Some(100),
            stay_id: Some(200),
            caregiver_id: Some(6770),
            charttime: 1700000000,
            storetime: Some(1700000060),
            itemid: 220045,
            value: Some("80".to_string()),
            valuenum: Some(80.0),
            valueuom: Some("/min".to_string()),
            warning: Some(0),
        };

        let record = chartevent_to_record(&event).unwrap();
        assert_eq!(record.metric_name, "12345|8867-4|/min");
        assert_eq!(record.value, 80.0);
        assert_eq!(record.resource_type, "Observation");
    }

    #[test]
    fn test_unmapped_itemid_returns_none() {
        let event = ChartEvent {
            subject_id: 12345,
            itemid: 999999,
            valuenum: Some(42.0),
            ..Default::default()
        };
        assert!(chartevent_to_record(&event).is_none());
    }

    #[test]
    fn test_iso_epoch_roundtrip() {
        // 1700000000 == 2023-11-14 22:13:20 UTC
        assert_eq!(epoch_to_iso(1_700_000_000), "2023-11-14 22:13:20");
        assert_eq!(iso_to_epoch("2023-11-14 22:13:20"), Some(1_700_000_000));
        // A de-identified MIMIC-future timestamp must round-trip too.
        let e = iso_to_epoch("2132-12-16 00:00:00").unwrap();
        assert_eq!(epoch_to_iso(e), "2132-12-16 00:00:00");
    }

    #[test]
    fn test_parse_charttime_both_forms() {
        // Integer epoch (synthetic) and ISO datetime (real) resolve identically.
        assert_eq!(parse_charttime("1700000000"), Some(1_700_000_000));
        assert_eq!(parse_charttime("2023-11-14 22:13:20"), Some(1_700_000_000));
        assert_eq!(parse_charttime("   "), None);
    }

    #[test]
    fn test_csv_roundtrip_real_shape() {
        // A ChartEvent written in the real 11-column ISO shape parses back
        // identically through the production loader.
        let events = vec![ChartEvent {
            subject_id: 10001,
            hadm_id: Some(20001),
            stay_id: Some(30001),
            caregiver_id: Some(6770),
            charttime: 1_700_000_000,
            storetime: Some(1_700_000_060),
            itemid: 220045,
            value: Some("84.0".to_string()),
            valuenum: Some(84.0),
            valueuom: Some("/min".to_string()),
            warning: Some(0),
        }];
        let tmp = std::env::temp_dir().join("emberdb_ce_roundtrip.csv");
        write_chartevents_csv(&tmp, &events).unwrap();
        let back = parse_chartevents_csv(&tmp).unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].subject_id, 10001);
        assert_eq!(back[0].caregiver_id, Some(6770));
        assert_eq!(back[0].charttime, 1_700_000_000);
        assert_eq!(back[0].storetime, Some(1_700_000_060));
        assert_eq!(back[0].itemid, 220045);
        assert_eq!(back[0].valuenum, Some(84.0));
        assert_eq!(back[0].valueuom.as_deref(), Some("/min"));
        assert_eq!(back[0].warning, Some(0));
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn test_legacy_8col_still_parses() {
        // Old 8-column integer-epoch layout must still load (new cols -> None).
        let tmp = std::env::temp_dir().join("emberdb_ce_legacy.csv");
        std::fs::write(
            &tmp,
            "subject_id,hadm_id,stay_id,charttime,itemid,value,valuenum,valueuom\n\
             10001,20001,30001,1700000000,220045,84.0,84.0,/min\n",
        )
        .unwrap();
        let back = parse_chartevents_csv(&tmp).unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].charttime, 1_700_000_000);
        assert_eq!(back[0].caregiver_id, None);
        assert_eq!(back[0].storetime, None);
        assert_eq!(back[0].warning, None);
        let _ = std::fs::remove_file(&tmp);
    }
}
