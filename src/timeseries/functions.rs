use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use crate::storage::Record;

/// Result of a metric analysis like trend, rate of change, etc.
#[derive(Debug, Serialize, Deserialize)]
pub struct AnalysisResult {
    pub metric_name: String,
    pub start_time: i64,
    pub end_time: i64,
    pub data_points: usize,
    pub result_type: String,
    pub value: f64,
    pub metadata: HashMap<String, String>,
}

/// Trend analysis for a time series
#[derive(Debug, Serialize, Deserialize)]
pub struct TrendAnalysis {
    pub metric_name: String,
    pub slope: f64,              // Rate of change per second
    pub r_squared: f64,          // Correlation coefficient (0-1)
    pub start_value: f64,        // Value at start of time range
    pub end_value: f64,          // Value at end of time range
    pub min_value: f64,          // Minimum value in range
    pub max_value: f64,          // Maximum value in range
    pub stddev: f64,             // Standard deviation
    pub data_points: usize,      // Number of points analyzed
    pub samples: Vec<(i64, f64)>,// Sample points (for visualization)
}

/// Statistics for a time period
#[derive(Debug, Serialize, Deserialize)]
pub struct TimeSeriesStats {
    pub metric_name: String,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub median: f64,
    pub stddev: f64,
    pub count: usize,
    pub percentiles: HashMap<String, f64>,
}

/// Outlier detection result
#[derive(Debug, Serialize, Deserialize)]
pub struct OutlierDetection {
    pub metric_name: String,
    pub outliers: Vec<OutlierPoint>,
    pub threshold: f64,
    pub method: String,
}

/// A single outlier point
#[derive(Debug, Serialize, Deserialize)]
pub struct OutlierPoint {
    pub timestamp: i64,
    pub value: f64,
    pub deviation: f64,  // How far from expected
    pub score: f64,      // 0-1 outlier score
}

/// Collection of time series functions
pub struct TimeSeriesFunctions;

impl TimeSeriesFunctions {
    /// Calculate linear regression (trend) for a set of data points
    pub fn calculate_trend(records: &[Record]) -> TrendAnalysis {
        if records.is_empty() {
            return TrendAnalysis {
                metric_name: "".to_string(),
                slope: 0.0,
                r_squared: 0.0,
                start_value: 0.0,
                end_value: 0.0,
                min_value: 0.0,
                max_value: 0.0,
                stddev: 0.0,
                data_points: 0,
                samples: vec![],
            };
        }
        
        let metric_name = records[0].metric_name.clone();
        
        // Extract x and y values (timestamp and value)
        let mut points: Vec<(f64, f64)> = records.iter()
            .map(|r| (r.timestamp as f64, r.value))
            .collect();
            
        // Sort by timestamp
        points.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        
        // Calculate linear regression
        let n = points.len() as f64;
        
        // Calculate means
        let mean_x = points.iter().map(|(x, _)| x).sum::<f64>() / n;
        let mean_y = points.iter().map(|(_, y)| y).sum::<f64>() / n;
        
        // Calculate slope and intercept
        let numerator: f64 = points.iter()
            .map(|(x, y)| (x - mean_x) * (y - mean_y))
            .sum();
            
        let denominator: f64 = points.iter()
            .map(|(x, _)| (x - mean_x).powi(2))
            .sum();
            
        let slope = if denominator != 0.0 { numerator / denominator } else { 0.0 };
        let intercept = mean_y - slope * mean_x;
        
        // Calculate R^2 (coefficient of determination)
        let ss_total: f64 = points.iter()
            .map(|(_, y)| (y - mean_y).powi(2))
            .sum();
            
        let ss_residual: f64 = points.iter()
            .map(|(x, y)| {
                let predicted = slope * x + intercept;
                (y - predicted).powi(2)
            })
            .sum();
            
        let r_squared = if ss_total != 0.0 { 1.0 - (ss_residual / ss_total) } else { 0.0 };
        
        // Calculate min, max, stddev
        let values: Vec<f64> = points.iter().map(|(_, y)| *y).collect();
        let min_value = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max_value = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        
        // Standard deviation
        let var_sum: f64 = values.iter().map(|y| (y - mean_y).powi(2)).sum();
        let stddev = (var_sum / n).sqrt();
        
        // Create sample points for visualization (take up to 20 evenly spaced points)
        let mut samples = Vec::new();
        let step = (points.len() / 20).max(1);
        for i in (0..points.len()).step_by(step) {
            let (x, y) = points[i];
            samples.push((x as i64, y));
        }
        
        // Make sure first and last points are included
        if !points.is_empty() {
            let (first_x, first_y) = points.first().unwrap();
            let (last_x, last_y) = points.last().unwrap();
            
            if samples.is_empty() || samples[0].0 != *first_x as i64 {
                samples.insert(0, (*first_x as i64, *first_y));
            }
            
            if samples.is_empty() || samples.last().unwrap().0 != *last_x as i64 {
                samples.push((*last_x as i64, *last_y));
            }
        }
        
        TrendAnalysis {
            metric_name,
            slope,
            r_squared,
            start_value: if !points.is_empty() { points.first().unwrap().1 } else { 0.0 },
            end_value: if !points.is_empty() { points.last().unwrap().1 } else { 0.0 },
            min_value,
            max_value,
            stddev,
            data_points: points.len(),
            samples,
        }
    }
    
    /// Calculate statistics for a time series
    pub fn calculate_stats(records: &[Record]) -> TimeSeriesStats {
        if records.is_empty() {
            return TimeSeriesStats {
                metric_name: "".to_string(),
                min: 0.0,
                max: 0.0,
                mean: 0.0,
                median: 0.0,
                stddev: 0.0,
                count: 0,
                percentiles: HashMap::new(),
            };
        }
        
        let metric_name = records[0].metric_name.clone();
        let mut values: Vec<f64> = records.iter().map(|r| r.value).collect();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let count = values.len();
        let min = values.first().copied().unwrap_or(0.0);
        let max = values.last().copied().unwrap_or(0.0);
        let mean = values.iter().sum::<f64>() / count as f64;
        
        // Calculate median
        let median = if count % 2 == 0 {
            (values[count / 2 - 1] + values[count / 2]) / 2.0
        } else {
            values[count / 2]
        };
        
        // Standard deviation
        let var_sum: f64 = values.iter().map(|v| (v - mean).powi(2)).sum();
        let stddev = (var_sum / count as f64).sqrt();
        
        // Calculate percentiles
        let mut percentiles = HashMap::new();
        let percentile_levels = [5, 10, 25, 75, 90, 95, 99];
        
        for &p in &percentile_levels {
            let idx = (p as f64 / 100.0 * (count as f64 - 1.0)).round() as usize;
            if idx < count {
                percentiles.insert(format!("p{}", p), values[idx]);
            }
        }
        
        TimeSeriesStats {
            metric_name,
            min,
            max,
            mean,
            median,
            stddev,
            count,
            percentiles,
        }
    }
    
    /// Detect outliers in a time series
    pub fn detect_outliers(records: &[Record], z_threshold: f64) -> OutlierDetection {
        if records.is_empty() {
            return OutlierDetection {
                metric_name: "".to_string(),
                outliers: vec![],
                threshold: z_threshold,
                method: "zscore".to_string(),
            };
        }
        
        let metric_name = records[0].metric_name.clone();
        let values: Vec<f64> = records.iter().map(|r| r.value).collect();
        
        // Calculate mean and standard deviation
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let var_sum: f64 = values.iter().map(|v| (v - mean).powi(2)).sum();
        let stddev = (var_sum / values.len() as f64).sqrt();
        
        // Find outliers based on Z-score
        let mut outliers = Vec::new();
        
        for (i, record) in records.iter().enumerate() {
            let z_score = if stddev > 0.0 { (record.value - mean) / stddev } else { 0.0 };
            let abs_z_score = z_score.abs();
            
            if abs_z_score > z_threshold {
                outliers.push(OutlierPoint {
                    timestamp: record.timestamp,
                    value: record.value,
                    deviation: record.value - mean,
                    score: abs_z_score / (abs_z_score + 1.0), // Normalize to 0-1
                });
            }
        }
        
        // Sort outliers by score (most extreme first)
        outliers.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        
        OutlierDetection {
            metric_name,
            outliers,
            threshold: z_threshold,
            method: "zscore".to_string(),
        }
    }
    
    /// Calculate rate of change (velocity) for a time series
    pub fn calculate_rate_of_change(records: &[Record], period_seconds: i64) -> Vec<Record> {
        if records.len() < 2 {
            return Vec::new();
        }
        
        // Sort records by timestamp
        let mut sorted_records = records.to_vec();
        sorted_records.sort_by_key(|r| r.timestamp);
        
        let mut result = Vec::new();
        let metric_name = format!("{}_rate", sorted_records[0].metric_name);
        
        for window in sorted_records.windows(2) {
            if window.len() < 2 {
                continue;
            }
            
            let r1 = &window[0];
            let r2 = &window[1];
            
            let time_diff = r2.timestamp - r1.timestamp;
            if time_diff <= 0 {
                continue; // Skip invalid time differences
            }
            
            // Calculate rate as change per specified period
            let value_diff = r2.value - r1.value;
            let rate = value_diff / (time_diff as f64) * (period_seconds as f64);
            
            // Create a new record at the end timestamp
            let mut context = r2.context.clone();
            context.insert("rate_period_seconds".to_string(), period_seconds.to_string());
            context.insert("original_metric".to_string(), r2.metric_name.clone());
            
            result.push(Record {
                timestamp: r2.timestamp,
                metric_name: metric_name.clone(),
                value: rate,
                context,
                resource_type: r2.resource_type.clone(),
            });
        }
        
        result
    }
} 