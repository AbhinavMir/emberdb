use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::path::Path;
use std::fs;
use crate::storage::Record;
use crate::timeseries::functions::{TrendAnalysis, OutlierDetection, TimeSeriesFunctions};

/// Configuration for pattern detection algorithms
#[derive(Debug, Serialize, Deserialize)]
pub struct DetectionConfig {
    pub global: GlobalConfig,
    pub seasonal: Option<SeasonalConfig>,
    pub multivariate: Option<MultivariateConfig>,
    pub changepoint: Option<ChangepointConfig>,
    pub moving_window: Option<MovingWindowConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalConfig {
    pub enable_all: bool,
    pub default_lookback_window: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SeasonalConfig {
    pub enabled: bool,
    pub min_data_points: usize,
    pub period: i64,
    pub method: SeasonalMethod,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SeasonalMethod {
    Additive,
    Multiplicative,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MultivariateConfig {
    pub enabled: bool,
    pub correlation_threshold: f64,
    pub groups: Vec<Vec<String>>,
    pub method: MultivariateMethod,
    pub threshold: f64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MultivariateMethod {
    Mahalanobis,
    IsolationForest,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChangepointConfig {
    pub enabled: bool,
    pub threshold: f64,
    pub method: ChangepointMethod,
    pub penalty: f64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChangepointMethod {
    Cusum,
    Pelt,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MovingWindowConfig {
    pub enabled: bool,
    pub window_size: i64,
    pub step_size: i64,
    pub method: WindowMethod,
    pub threshold: f64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WindowMethod {
    Trend,
    Volatility,
    Range,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SeasonalDecomposition {
    pub metric_name: String,
    pub trend: Vec<(i64, f64)>,
    pub seasonal: Vec<(i64, f64)>,
    pub residual: Vec<(i64, f64)>,
    pub period: i64,
    pub method: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MultivariateOutlier {
    pub timestamp: i64,
    pub metrics: Vec<String>,
    pub values: Vec<f64>,
    pub score: f64,
    pub threshold: f64,
    pub method: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MultivariateOutlierResult {
    pub group: Vec<String>,
    pub outliers: Vec<MultivariateOutlier>,
    pub method: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Changepoint {
    pub timestamp: i64,
    pub metric: String,
    pub before_mean: f64,
    pub after_mean: f64,
    pub change_magnitude: f64,
    pub confidence: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChangepointResult {
    pub metric: String,
    pub changepoints: Vec<Changepoint>,
    pub method: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WindowAnalysisPoint {
    pub window_start: i64,
    pub window_end: i64,
    pub value: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WindowAnalysisResult {
    pub metric: String,
    pub windows: Vec<WindowAnalysisPoint>,
    pub method: String,
    pub anomalous_windows: Vec<WindowAnalysisPoint>,
}

pub struct PatternDetector {
    config: DetectionConfig,
}

impl PatternDetector {
    /// Create a new pattern detector with default configuration
    pub fn new() -> Self {
        let config = DetectionConfig {
            global: GlobalConfig {
                enable_all: true,
                default_lookback_window: 86400,
            },
            seasonal: Some(SeasonalConfig {
                enabled: true,
                min_data_points: 24,
                period: 86400,
                method: SeasonalMethod::Additive,
            }),
            multivariate: Some(MultivariateConfig {
                enabled: true,
                correlation_threshold: 0.7,
                groups: vec![],
                method: MultivariateMethod::Mahalanobis,
                threshold: 3.0,
            }),
            changepoint: Some(ChangepointConfig {
                enabled: true,
                threshold: 2.0,
                method: ChangepointMethod::Cusum,
                penalty: 1.0,
            }),
            moving_window: Some(MovingWindowConfig {
                enabled: true,
                window_size: 3600,
                step_size: 900,
                method: WindowMethod::Volatility,
                threshold: 1.5,
            }),
        };
        
        PatternDetector { config }
    }
    
    /// Load configuration from a TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let config: DetectionConfig = toml::from_str(&content)?;
        Ok(PatternDetector { config })
    }
    
    /// Decompose a time series into trend, seasonal, and residual components
    pub fn seasonal_decomposition(&self, records: &[Record]) -> Result<SeasonalDecomposition, String> {
        if records.is_empty() {
            return Err("No data provided for seasonal decomposition".to_string());
        }
        
        let config = match &self.config.seasonal {
            Some(cfg) if cfg.enabled => cfg,
            _ => return Err("Seasonal decomposition not enabled in config".to_string()),
        };
        
        if records.len() < config.min_data_points {
            return Err(format!(
                "Not enough data points for seasonal decomposition. Need at least {}, got {}",
                config.min_data_points, records.len()
            ));
        }
        
        // Sort records by timestamp
        let mut sorted_records = records.to_vec();
        sorted_records.sort_by_key(|r| r.timestamp);
        
        // Extract time and value vectors
        let timestamps: Vec<i64> = sorted_records.iter().map(|r| r.timestamp).collect();
        let values: Vec<f64> = sorted_records.iter().map(|r| r.value).collect();
        
        // Calculate the trend using moving average
        let trend = self.calculate_moving_average(&timestamps, &values, config.period / 10);
        
        // Calculate seasonal component
        let mut seasonal: Vec<(i64, f64)> = Vec::new();
        let period_samples = self.determine_period_samples(&timestamps, config.period);
        
        // Calculate average seasonal pattern
        let seasonal_pattern = self.calculate_seasonal_pattern(
            &timestamps, &values, &trend, period_samples, &config.method
        );
        
        // Apply seasonal pattern to each timestamp
        for i in 0..timestamps.len() {
            let seasonal_idx = (i % period_samples) as usize;
            if seasonal_idx < seasonal_pattern.len() {
                seasonal.push((timestamps[i], seasonal_pattern[seasonal_idx]));
            }
        }
        
        // Calculate residual (original - trend - seasonal)
        let mut residual = Vec::new();
        for i in 0..timestamps.len() {
            let trend_value = trend.iter()
                .find(|(ts, _)| *ts == timestamps[i])
                .map(|(_, v)| *v)
                .unwrap_or(values[i]);
                
            let seasonal_value = seasonal.iter()
                .find(|(ts, _)| *ts == timestamps[i])
                .map(|(_, v)| *v)
                .unwrap_or(0.0);
                
            let res_value = match config.method {
                SeasonalMethod::Additive => values[i] - trend_value - seasonal_value,
                SeasonalMethod::Multiplicative => {
                    if trend_value != 0.0 && seasonal_value != 0.0 {
                        values[i] / (trend_value * seasonal_value)
                    } else {
                        values[i]
                    }
                }
            };
            
            residual.push((timestamps[i], res_value));
        }
        
        Ok(SeasonalDecomposition {
            metric_name: records[0].metric_name.clone(),
            trend,
            seasonal,
            residual,
            period: config.period,
            method: format!("{:?}", config.method),
        })
    }
    
    /// Detect multivariate outliers in a group of related metrics
    pub fn multivariate_outlier_detection(
        &self, 
        metric_records: &HashMap<String, Vec<Record>>,
    ) -> Result<Vec<MultivariateOutlierResult>, String> {
        let config = match &self.config.multivariate {
            Some(cfg) if cfg.enabled => cfg,
            _ => return Err("Multivariate outlier detection not enabled".to_string()),
        };
        
        let mut results = Vec::new();
        
        // Process predefined groups
        for group in &config.groups {
            // Skip if any metric in the group is missing
            let mut all_metrics_present = true;
            for metric in group {
                if !metric_records.contains_key(metric) {
                    all_metrics_present = false;
                    break;
                }
            }
            
            if !all_metrics_present {
                continue;
            }
            
            let result = self.detect_outliers_in_group(group, metric_records, config)?;
            results.push(result);
        }
        
        // Auto-detect correlated groups if no predefined groups or if no predefined group had all members present
        if config.groups.is_empty() || results.is_empty() {
            let correlated_groups = self.find_correlated_groups(metric_records, config.correlation_threshold);
            
            for group in correlated_groups {
                if group.len() < 2 {
                    continue; // Need at least 2 metrics for multivariate analysis
                }
                
                let result = self.detect_outliers_in_group(&group, metric_records, config)?;
                results.push(result);
            }
        }
        
        Ok(results)
    }
    
    /// Detect change points in a time series
    pub fn detect_changepoints(&self, records: &[Record]) -> Result<ChangepointResult, String> {
        if records.is_empty() {
            return Err("No data provided for changepoint detection".to_string());
        }
        
        let config = match &self.config.changepoint {
            Some(cfg) if cfg.enabled => cfg,
            _ => return Err("Changepoint detection not enabled in config".to_string()),
        };
        
        // Sort records by timestamp
        let mut sorted_records = records.to_vec();
        sorted_records.sort_by_key(|r| r.timestamp);
        
        let timestamps: Vec<i64> = sorted_records.iter().map(|r| r.timestamp).collect();
        let values: Vec<f64> = sorted_records.iter().map(|r| r.value).collect();
        
        let changepoints = match config.method {
            ChangepointMethod::Cusum => self.cusum_changepoint(&timestamps, &values, config.threshold),
            ChangepointMethod::Pelt => self.pelt_changepoint(&timestamps, &values, config.threshold, config.penalty),
        };
        
        Ok(ChangepointResult {
            metric: records[0].metric_name.clone(),
            changepoints,
            method: format!("{:?}", config.method),
        })
    }
    
    /// Perform moving window analysis on a time series
    pub fn moving_window_analysis(&self, records: &[Record]) -> Result<WindowAnalysisResult, String> {
        if records.is_empty() {
            return Err("No data provided for moving window analysis".to_string());
        }
        
        let config = match &self.config.moving_window {
            Some(cfg) if cfg.enabled => cfg,
            _ => return Err("Moving window analysis not enabled in config".to_string()),
        };
        
        // Sort records by timestamp
        let mut sorted_records = records.to_vec();
        sorted_records.sort_by_key(|r| r.timestamp);
        
        let timestamps: Vec<i64> = sorted_records.iter().map(|r| r.timestamp).collect();
        let values: Vec<f64> = sorted_records.iter().map(|r| r.value).collect();
        
        let earliest = *timestamps.first().unwrap_or(&0);
        let latest = *timestamps.last().unwrap_or(&0);
        
        let mut windows = Vec::new();
        let mut window_start = earliest;
        
        while window_start + config.window_size <= latest {
            let window_end = window_start + config.window_size;
            
            // Get data points in this window
            let window_indices: Vec<usize> = timestamps.iter()
                .enumerate()
                .filter_map(|(i, &ts)| if ts >= window_start && ts < window_end { Some(i) } else { None })
                .collect();
                
            if !window_indices.is_empty() {
                let window_values: Vec<f64> = window_indices.iter()
                    .map(|&i| values[i])
                    .collect();
                    
                let value = match config.method {
                    WindowMethod::Trend => {
                        // Linear regression slope within window
                        let window_timestamps: Vec<f64> = window_indices.iter()
                            .map(|&i| timestamps[i] as f64)
                            .collect();
                        self.calculate_slope(&window_timestamps, &window_values)
                    },
                    WindowMethod::Volatility => {
                        // Standard deviation within window
                        self.calculate_stddev(&window_values)
                    },
                    WindowMethod::Range => {
                        // Range (max - min) within window
                        let min = window_values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                        let max = window_values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
                        max - min
                    }
                };
                
                windows.push(WindowAnalysisPoint {
                    window_start,
                    window_end,
                    value,
                });
            }
            
            window_start += config.step_size;
        }
        
        // Detect anomalous windows
        let window_values: Vec<f64> = windows.iter().map(|w| w.value).collect();
        let mean = self.calculate_mean(&window_values);
        let stddev = self.calculate_stddev(&window_values);
        
        let anomalous_windows: Vec<WindowAnalysisPoint> = windows.iter()
            .filter(|w| (w.value - mean).abs() > config.threshold * stddev)
            .cloned()
            .collect();
        
        Ok(WindowAnalysisResult {
            metric: records[0].metric_name.clone(),
            windows,
            method: format!("{:?}", config.method),
            anomalous_windows,
        })
    }
    
    // Helper Methods
    
    fn calculate_moving_average(&self, timestamps: &[i64], values: &[f64], window_size: i64) -> Vec<(i64, f64)> {
        let mut result = Vec::new();
        
        for i in 0..timestamps.len() {
            let current_time = timestamps[i];
            let half_window = window_size / 2;
            
            // Find values within the window
            let window_values: Vec<f64> = timestamps.iter()
                .zip(values.iter())
                .filter(|&(ts, _)| (ts - current_time).abs() <= half_window)
                .map(|(_, &v)| v)
                .collect();
                
            if !window_values.is_empty() {
                let avg = window_values.iter().sum::<f64>() / window_values.len() as f64;
                result.push((current_time, avg));
            } else {
                result.push((current_time, values[i]));
            }
        }
        
        result
    }
    
    fn determine_period_samples(&self, timestamps: &[i64], period: i64) -> usize {
        if timestamps.len() <= 1 {
            return 1;
        }
        
        // Estimate number of samples in a period based on average sample interval
        let avg_interval = (timestamps.last().unwrap() - timestamps.first().unwrap()) as f64 / (timestamps.len() - 1) as f64;
        (period as f64 / avg_interval).round() as usize
    }
    
    fn calculate_seasonal_pattern(
        &self, 
        timestamps: &[i64], 
        values: &[f64], 
        trend: &[(i64, f64)],
        period_samples: usize,
        method: &SeasonalMethod
    ) -> Vec<f64> {
        let mut pattern = vec![0.0; period_samples];
        let mut counts = vec![0; period_samples];
        
        // Calculate detrended values and accumulate by position in cycle
        for i in 0..timestamps.len() {
            let trend_value = trend.iter()
                .find(|(ts, _)| *ts == timestamps[i])
                .map(|(_, v)| *v)
                .unwrap_or(values[i]);
                
            let position = (i % period_samples) as usize;
            
            match method {
                SeasonalMethod::Additive => {
                    pattern[position] += values[i] - trend_value;
                },
                SeasonalMethod::Multiplicative => {
                    if trend_value != 0.0 {
                        pattern[position] += values[i] / trend_value;
                    }
                }
            }
            
            counts[position] += 1;
        }
        
        // Calculate averages
        for i in 0..period_samples {
            if counts[i] > 0 {
                pattern[i] /= counts[i] as f64;
            }
        }
        
        // Ensure seasonal component sums to zero for additive or averages to 1 for multiplicative
        match method {
            SeasonalMethod::Additive => {
                let avg = pattern.iter().sum::<f64>() / pattern.len() as f64;
                for val in pattern.iter_mut() {
                    *val -= avg;
                }
            },
            SeasonalMethod::Multiplicative => {
                let avg = pattern.iter().sum::<f64>() / pattern.len() as f64;
                if avg != 0.0 {
                    for val in pattern.iter_mut() {
                        *val /= avg;
                    }
                }
            }
        }
        
        pattern
    }
    
    fn detect_outliers_in_group(
        &self,
        group: &[String],
        metric_records: &HashMap<String, Vec<Record>>,
        config: &MultivariateConfig
    ) -> Result<MultivariateOutlierResult, String> {
        // Create a single timeline with all metrics aligned
        let mut aligned_data: HashMap<i64, Vec<(String, f64)>> = HashMap::new();
        
        for metric in group {
            if let Some(records) = metric_records.get(metric) {
                for record in records {
                    aligned_data.entry(record.timestamp)
                        .or_insert_with(Vec::new)
                        .push((metric.clone(), record.value));
                }
            }
        }
        
        // Keep only timestamps where all metrics have values
        let timestamps: Vec<i64> = aligned_data.iter()
            .filter(|(_, values)| values.len() == group.len())
            .map(|(&ts, _)| ts)
            .collect();
            
        if timestamps.is_empty() {
            return Err("No aligned data points found for the metric group".to_string());
        }
        
        // Sort timestamps
        let mut sorted_timestamps = timestamps.clone();
        sorted_timestamps.sort();
        
        // Build the data matrix
        let mut data_matrix: Vec<Vec<f64>> = Vec::new();
        
        for &ts in &sorted_timestamps {
            let values = &aligned_data[&ts];
            
            // Sort values by metric name to ensure consistent order
            let mut ordered_values = values.clone();
            ordered_values.sort_by(|a, b| a.0.cmp(&b.0));
            
            data_matrix.push(ordered_values.iter().map(|(_, v)| *v).collect());
        }
        
        // Detect outliers
        let outliers = match config.method {
            MultivariateMethod::Mahalanobis => {
                self.mahalanobis_outliers(&sorted_timestamps, &data_matrix, group, config.threshold)
            },
            MultivariateMethod::IsolationForest => {
                self.isolation_forest_outliers(&sorted_timestamps, &data_matrix, group)
            }
        };
        
        Ok(MultivariateOutlierResult {
            group: group.to_vec(),
            outliers,
            method: format!("{:?}", config.method),
        })
    }
    
    fn find_correlated_groups(
        &self, 
        metric_records: &HashMap<String, Vec<Record>>,
        threshold: f64
    ) -> Vec<Vec<String>> {
        let metrics: Vec<String> = metric_records.keys().cloned().collect();
        let mut correlation_matrix: HashMap<(String, String), f64> = HashMap::new();
        
        // Calculate pairwise correlations
        for i in 0..metrics.len() {
            for j in i..metrics.len() {
                let metric1 = &metrics[i];
                let metric2 = &metrics[j];
                
                let correlation = if i == j {
                    1.0 // Perfect correlation with self
                } else {
                    self.calculate_correlation(
                        metric_records.get(metric1).unwrap(),
                        metric_records.get(metric2).unwrap()
                    )
                };
                
                correlation_matrix.insert((metric1.clone(), metric2.clone()), correlation);
                correlation_matrix.insert((metric2.clone(), metric1.clone()), correlation);
            }
        }
        
        // Group metrics with high correlation
        let mut visited = vec![false; metrics.len()];
        let mut groups = Vec::new();
        
        for i in 0..metrics.len() {
            if visited[i] {
                continue;
            }
            
            let mut group = vec![metrics[i].clone()];
            visited[i] = true;
            
            for j in 0..metrics.len() {
                if i == j || visited[j] {
                    continue;
                }
                
                let corr = correlation_matrix.get(&(metrics[i].clone(), metrics[j].clone())).unwrap_or(&0.0);
                if corr.abs() >= threshold {
                    group.push(metrics[j].clone());
                    visited[j] = true;
                }
            }
            
            if group.len() > 1 {
                groups.push(group);
            }
        }
        
        groups
    }
    
    fn calculate_correlation(&self, records1: &[Record], records2: &[Record]) -> f64 {
        // Create a map of timestamp to value for each metric
        let mut values1: HashMap<i64, f64> = HashMap::new();
        let mut values2: HashMap<i64, f64> = HashMap::new();
        
        for record in records1 {
            values1.insert(record.timestamp, record.value);
        }
        
        for record in records2 {
            values2.insert(record.timestamp, record.value);
        }
        
        // Find common timestamps
        let common_ts: Vec<i64> = values1.keys()
            .filter(|&ts| values2.contains_key(ts))
            .cloned()
            .collect();
            
        if common_ts.len() < 3 {
            return 0.0; // Not enough data points
        }
        
        // Extract aligned values
        let mut x: Vec<f64> = Vec::new();
        let mut y: Vec<f64> = Vec::new();
        
        for ts in common_ts {
            x.push(values1[&ts]);
            y.push(values2[&ts]);
        }
        
        // Calculate Pearson correlation
        let mean_x = self.calculate_mean(&x);
        let mean_y = self.calculate_mean(&y);
        
        let mut numerator = 0.0;
        let mut denom_x = 0.0;
        let mut denom_y = 0.0;
        
        for i in 0..x.len() {
            let dx = x[i] - mean_x;
            let dy = y[i] - mean_y;
            numerator += dx * dy;
            denom_x += dx * dx;
            denom_y += dy * dy;
        }
        
        if denom_x == 0.0 || denom_y == 0.0 {
            return 0.0;
        }
        
        numerator / (denom_x.sqrt() * denom_y.sqrt())
    }
    
    fn mahalanobis_outliers(
        &self, 
        timestamps: &[i64], 
        data: &[Vec<f64>], 
        metrics: &[String],
        threshold: f64
    ) -> Vec<MultivariateOutlier> {
        let n = data.len();
        let p = if n > 0 { data[0].len() } else { 0 };
        
        if n < p + 1 {
            return Vec::new(); // Not enough data points
        }
        
        // Calculate means
        let mut means = vec![0.0; p];
        for row in data {
            for j in 0..p {
                means[j] += row[j];
            }
        }
        
        for j in 0..p {
            means[j] /= n as f64;
        }
        
        // Calculate covariance matrix
        let mut cov = vec![vec![0.0; p]; p];
        
        for row in data {
            for i in 0..p {
                for j in 0..p {
                    cov[i][j] += (row[i] - means[i]) * (row[j] - means[j]);
                }
            }
        }
        
        for i in 0..p {
            for j in 0..p {
                cov[i][j] /= (n - 1) as f64;
            }
        }
        
        // Calculate inverse of covariance matrix (simplified approach)
        let inv_cov = match self.invert_matrix(&cov) {
            Some(inv) => inv,
            None => return Vec::new(), // Singular covariance matrix
        };
        
        // Calculate Mahalanobis distance for each point
        let mut outliers = Vec::new();
        
        for (idx, row) in data.iter().enumerate() {
            let mut distance = 0.0;
            
            for i in 0..p {
                for j in 0..p {
                    distance += (row[i] - means[i]) * inv_cov[i][j] * (row[j] - means[j]);
                }
            }
            
            distance = distance.sqrt();
            
            // Chi-squared critical value (p degrees of freedom)
            if distance > threshold {
                outliers.push(MultivariateOutlier {
                    timestamp: timestamps[idx],
                    metrics: metrics.to_vec(),
                    values: row.clone(),
                    score: distance,
                    threshold,
                    method: "Mahalanobis".to_string(),
                });
            }
        }
        
        outliers
    }
    
    fn isolation_forest_outliers(
        &self, 
        timestamps: &[i64], 
        data: &[Vec<f64>], 
        metrics: &[String]
    ) -> Vec<MultivariateOutlier> {
        // Simple implementation of Isolation Forest for multivariate outlier detection
        // This is a placeholder - in production, you'd use a proper ML implementation
        
        if data.is_empty() {
            return Vec::new();
        }
        
        let dimension = data[0].len();
        
        // Calculate standard deviations for each dimension
        let mut means = vec![0.0; dimension];
        let mut variances = vec![0.0; dimension];
        
        // Calculate means
        for row in data {
            for j in 0..dimension {
                means[j] += row[j];
            }
        }
        
        for j in 0..dimension {
            means[j] /= data.len() as f64;
        }
        
        // Calculate variances
        for row in data {
            for j in 0..dimension {
                variances[j] += (row[j] - means[j]).powi(2);
            }
        }
        
        for j in 0..dimension {
            variances[j] /= data.len() as f64;
        }
        
        // Calculate Z-scores for each point
        let mut outliers = Vec::new();
        
        for (idx, row) in data.iter().enumerate() {
            let mut z_scores = Vec::new();
            
            for j in 0..dimension {
                let std_dev = variances[j].sqrt();
                if std_dev > 0.0 {
                    z_scores.push((row[j] - means[j]) / std_dev);
                } else {
                    z_scores.push(0.0);
                }
            }
            
            // Use max absolute Z-score as anomaly score (simplified approach)
            let max_zscore = z_scores.iter()
                .fold(0.0, |max, &z| max.max(z.abs()));
                
            if max_zscore > 3.0 { // Threshold of 3 sigma
                outliers.push(MultivariateOutlier {
                    timestamp: timestamps[idx],
                    metrics: metrics.to_vec(),
                    values: row.clone(),
                    score: max_zscore,
                    threshold: 3.0,
                    method: "IsolationForest".to_string(),
                });
            }
        }
        
        outliers
    }
    
    fn cusum_changepoint(&self, timestamps: &[i64], values: &[f64], threshold: f64) -> Vec<Changepoint> {
        let mut changepoints = Vec::new();
        
        if values.len() < 10 {
            return changepoints; // Not enough data
        }
        
        // CUSUM algorithm for change detection
        let mean = self.calculate_mean(values);
        let std_dev = self.calculate_stddev(values);
        
        if std_dev == 0.0 {
            return changepoints; // No variation in data
        }
        
        let k = 0.5 * std_dev; // Sensitivity parameter
        let h = threshold * std_dev; // Decision threshold
        
        let mut s_pos = 0.0;
        let mut s_neg = 0.0;
        let mut last_change = 0;
        
        for i in 0..values.len() {
            let x = values[i];
            
            // Upper CUSUM
            s_pos = (s_pos + (x - mean - k)).max(0.0);
            
            // Lower CUSUM
            s_neg = (s_neg + (mean - k - x)).max(0.0);
            
            // Check for change
            if s_pos > h || s_neg > h {
                // Calculate means before and after
                let before_mean = if last_change > 0 {
                    values[last_change..i].iter().sum::<f64>() / (i - last_change) as f64
                } else {
                    values[..i].iter().sum::<f64>() / i as f64
                };
                
                let after_mean = values[i..].iter().sum::<f64>() / (values.len() - i) as f64;
                
                changepoints.push(Changepoint {
                    timestamp: timestamps[i],
                    metric: String::new(), // Will be filled in later
                    before_mean,
                    after_mean,
                    change_magnitude: (after_mean - before_mean).abs(),
                    confidence: (s_pos.max(s_neg) / h).min(1.0),
                });
                
                // Reset
                s_pos = 0.0;
                s_neg = 0.0;
                last_change = i;
            }
        }
        
        changepoints
    }
    
    fn pelt_changepoint(&self, timestamps: &[i64], values: &[f64], threshold: f64, penalty: f64) -> Vec<Changepoint> {
        // Simplified PELT algorithm
        // In practice, you'd use a more sophisticated implementation
        
        if values.len() < 20 {
            return Vec::new(); // Not enough data
        }
        
        let min_segment_length = 5; // Minimum points between changes
        let std_dev = self.calculate_stddev(values);
        let n = values.len();
        
        // Initialize cost function (negative log-likelihood for Gaussian)
        let mut best_cost = vec![f64::INFINITY; n + 1];
        best_cost[0] = 0.0;
        
        // Last changepoint
        let mut last_changepoint = vec![0; n + 1];
        
        // For each possible endpoint
        for t in min_segment_length..=n {
            // For each possible last changepoint before t
            let mut min_cost = f64::INFINITY;
            let mut best_s = 0;
            
            for s in (0..=(t - min_segment_length)).rev() {
                // Cost for segment (s,t)
                let segment = &values[s..t];
                let segment_cost = if segment.len() > 1 {
                    let segment_var = segment.iter()
                        .map(|&x| {
                            let mean = segment.iter().sum::<f64>() / segment.len() as f64;
                            (x - mean).powi(2)
                        })
                        .sum::<f64>() / (segment.len() - 1) as f64;
                        
                    (segment.len() as f64) * segment_var.ln() / 2.0
                } else {
                    0.0
                };
                
                let cost = best_cost[s] + segment_cost + penalty;
                
                if cost < min_cost {
                    min_cost = cost;
                    best_s = s;
                }
            }
            
            best_cost[t] = min_cost;
            last_changepoint[t] = best_s;
        }
        
        // Backtrack to find changepoints
        let mut cp_indices = Vec::new();
        let mut t = n;
        
        while t > 0 {
            let s = last_changepoint[t];
            if s > 0 {
                cp_indices.push(s);
            }
            t = s;
        }
        
        // Sort and convert to Changepoint structs
        cp_indices.sort();
        
        let mut changepoints = Vec::new();
        
        for i in 0..cp_indices.len() {
            let idx = cp_indices[i];
            
            let start_idx = if i > 0 { cp_indices[i-1] } else { 0 };
            let end_idx = if i < cp_indices.len() - 1 { cp_indices[i+1] } else { n };
            
            let before_mean = values[start_idx..idx].iter().sum::<f64>() / (idx - start_idx) as f64;
            let after_mean = values[idx..end_idx].iter().sum::<f64>() / (end_idx - idx) as f64;
            let change_magnitude = (after_mean - before_mean).abs();
            
            if change_magnitude > threshold * std_dev {
                changepoints.push(Changepoint {
                    timestamp: timestamps[idx],
                    metric: String::new(), // Will be filled later
                    before_mean,
                    after_mean,
                    change_magnitude,
                    confidence: (change_magnitude / (threshold * std_dev)).min(1.0),
                });
            }
        }
        
        changepoints
    }
    
    // Basic statistical functions
    
    fn calculate_mean(&self, values: &[f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        values.iter().sum::<f64>() / values.len() as f64
    }
    
    fn calculate_stddev(&self, values: &[f64]) -> f64 {
        if values.len() <= 1 {
            return 0.0;
        }
        
        let mean = self.calculate_mean(values);
        let variance = values.iter()
            .map(|&v| (v - mean).powi(2))
            .sum::<f64>() / (values.len() - 1) as f64;
            
        variance.sqrt()
    }
    
    fn calculate_slope(&self, x: &[f64], y: &[f64]) -> f64 {
        if x.len() != y.len() || x.len() < 2 {
            return 0.0;
        }
        
        let n = x.len() as f64;
        let sum_x: f64 = x.iter().sum();
        let sum_y: f64 = y.iter().sum();
        let sum_xy: f64 = x.iter().zip(y.iter()).map(|(&xi, &yi)| xi * yi).sum();
        let sum_xx: f64 = x.iter().map(|&xi| xi * xi).sum();
        
        let numerator = n * sum_xy - sum_x * sum_y;
        let denominator = n * sum_xx - sum_x * sum_x;
        
        if denominator == 0.0 {
            return 0.0;
        }
        
        numerator / denominator
    }
    
    // Matrix operations for Mahalanobis distance
    
    fn invert_matrix(&self, matrix: &[Vec<f64>]) -> Option<Vec<Vec<f64>>> {
        let n = matrix.len();
        if n == 0 || matrix[0].len() != n {
            return None; // Not a square matrix
        }
        
        // Special case for 1x1 matrix
        if n == 1 {
            if matrix[0][0] == 0.0 {
                return None; // Singular
            }
            return Some(vec![vec![1.0 / matrix[0][0]]]);
        }
        
        // Special case for 2x2 matrix
        if n == 2 {
            let det = matrix[0][0] * matrix[1][1] - matrix[0][1] * matrix[1][0];
            if det == 0.0 {
                return None; // Singular
            }
            
            let inv_det = 1.0 / det;
            return Some(vec![
                vec![matrix[1][1] * inv_det, -matrix[0][1] * inv_det],
                vec![-matrix[1][0] * inv_det, matrix[0][0] * inv_det]
            ]);
        }
        
        // For larger matrices, we'd use a proper linear algebra library.
        // This is a simplified approach that works for most positive definite matrices
        // common in covariance calculations, but isn't robust for all matrices.
        
        // First, compute diagonal regularization to avoid singularity
        let mut regularized = matrix.to_vec();
        for i in 0..n {
            regularized[i][i] += 1e-6; // Small regularization
        }
        
        // Identity matrix
        let mut identity = vec![vec![0.0; n]; n];
        for i in 0..n {
            identity[i][i] = 1.0;
        }
        
        // Gauss-Jordan elimination
        let mut augmented = regularized.clone();
        for i in 0..n {
            for j in 0..n {
                augmented[i].push(identity[i][j]);
            }
        }
        
        // Forward elimination
        for i in 0..n {
            // Find pivot
            let mut max_row = i;
            let mut max_val = augmented[i][i].abs();
            
            for k in (i+1)..n {
                if augmented[k][i].abs() > max_val {
                    max_val = augmented[k][i].abs();
                    max_row = k;
                }
            }
            
            if max_val < 1e-10 {
                return None; // Singular
            }
            
            // Swap rows if needed
            if max_row != i {
                augmented.swap(i, max_row);
            }
            
            // Scale pivot row
            let pivot = augmented[i][i];
            for j in 0..(2*n) {
                augmented[i][j] /= pivot;
            }
            
            // Eliminate other rows
            for k in 0..n {
                if k != i {
                    let factor = augmented[k][i];
                    for j in 0..(2*n) {
                        augmented[k][j] -= factor * augmented[i][j];
                    }
                }
            }
        }
        
        // Extract inverse
        let mut inverse = vec![vec![0.0; n]; n];
        for i in 0..n {
            for j in 0..n {
                inverse[i][j] = augmented[i][j+n];
            }
        }
        
        Some(inverse)
    }
} 