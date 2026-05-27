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

/// A single row from MIMIC-IV chartevents
#[derive(Debug, Clone)]
pub struct ChartEvent {
    pub subject_id: i64,
    pub hadm_id: Option<i64>,
    pub stay_id: Option<i64>,
    pub charttime: i64, // Unix timestamp
    pub itemid: i64,
    pub value: Option<String>,
    pub valuenum: Option<f64>,
    pub valueuom: Option<String>,
}

/// Parse a chartevents CSV file and return ChartEvent rows.
/// Expected columns: subject_id, hadm_id, stay_id, charttime, itemid, value, valuenum, valueuom
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
        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() < 8 {
            continue;
        }

        let subject_id = fields[0].trim().parse::<i64>().unwrap_or(0);
        let hadm_id = fields[1].trim().parse::<i64>().ok();
        let stay_id = fields[2].trim().parse::<i64>().ok();
        // charttime is expected as unix timestamp (integer seconds)
        let charttime = fields[3].trim().parse::<i64>().unwrap_or(0);
        let itemid = fields[4].trim().parse::<i64>().unwrap_or(0);
        let value = {
            let v = fields[5].trim();
            if v.is_empty() { None } else { Some(v.to_string()) }
        };
        let valuenum = fields[6].trim().parse::<f64>().ok();
        let valueuom = {
            let v = fields[7].trim();
            if v.is_empty() { None } else { Some(v.to_string()) }
        };

        events.push(ChartEvent {
            subject_id,
            hadm_id,
            stay_id,
            charttime,
            itemid,
            value,
            valuenum,
            valueuom,
        });
    }

    Ok(events)
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
            charttime: 1700000000,
            itemid: 220045,
            value: Some("80".to_string()),
            valuenum: Some(80.0),
            valueuom: Some("/min".to_string()),
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
            hadm_id: None,
            stay_id: None,
            charttime: 1700000000,
            itemid: 999999,
            value: None,
            valuenum: Some(42.0),
            valueuom: None,
        };
        assert!(chartevent_to_record(&event).is_none());
    }
}
