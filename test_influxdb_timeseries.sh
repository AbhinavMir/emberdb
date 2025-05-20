#!/bin/bash

# Script to benchmark InfluxDB with similar data to test_timeseries.sh (1M entries)

set -e
INFLUX_URL="http://localhost:8086"
INFLUX_ORG="emberdb-benchmark"
INFLUX_BUCKET="monitoring"
INFLUX_TOKEN="my-super-secret-token"
PATIENT_ID="123"
CURRENT_TIME=$(date +%s)
START_TIME=$((CURRENT_TIME - 24*3600))  # 24 hours ago

# Configuration for large dataset - REDUCED for testing
HEART_RATE_COUNT=1000   # Reduced from 250000
BLOOD_PRESSURE_COUNT=1000  # Reduced from 250000
SPO2_COUNT=1000  # Reduced from 250000
ECG_COUNT=1000  # Reduced from 250000
BATCH_SIZE=200  # Reduced from 5000
DEBUG=true  # Enable debug logging

# Debug log function
debug_log() {
  if [ "$DEBUG" = true ]; then
    echo "[DEBUG] $(date +"%H:%M:%S") - $1"
  fi
}

# Start total benchmark
BENCHMARK_START=$(date +%s.%N)

echo "==== Setting up InfluxDB connection ===="
# Check if InfluxDB is available
if ! curl -s "${INFLUX_URL}/ping" > /dev/null; then
  echo "Error: InfluxDB is not available at ${INFLUX_URL}"
  echo "Please ensure the container is running with: docker ps | grep influxdb"
  exit 1
fi

# Create line protocol data for heart rate
echo "==== Adding Heart Rate Observations with Trend ($HEART_RATE_COUNT entries) ===="
HR_START=$(date +%s.%N)

# Use batching for better performance
current_batch=0
HR_DATA=""
debug_log "Starting heart rate batch creation"

for ((i=0; i<$HEART_RATE_COUNT; i++)); do
  # Calculate timestamp with microsecond precision to ensure unique timestamps
  time_offset=$((i*60))  # One per minute instead of hour to get more data points
  timestamp=$((START_TIME + time_offset))
  
  # Generate heart rate with upward trend (70 to 90) plus some noise
  base_hr=$((70 + (i*20/$HEART_RATE_COUNT)))
  noise=$((RANDOM % 5 - 2))
  hr=$((base_hr + noise))
  
  # Add to line protocol format
  HR_DATA="${HR_DATA}heart_rate,patient_id=${PATIENT_ID},code=8867-4 value=${hr} ${timestamp}000000000
"
  current_batch=$((current_batch + 1))
  
  # Every BATCH_SIZE entries, send to InfluxDB
  if [ $current_batch -eq $BATCH_SIZE ] || [ $i -eq $((HEART_RATE_COUNT - 1)) ]; then
    echo "Processing heart rate batch: entries $((i-current_batch+1)) to $i..."
    
    debug_log "Preparing data payload, size: $(echo -n "$HR_DATA" | wc -c) bytes"
    debug_log "Sending request to $INFLUX_URL/api/v2/write"
    
    curl_result=$(curl -s -XPOST "${INFLUX_URL}/api/v2/write?org=${INFLUX_ORG}&bucket=${INFLUX_BUCKET}&precision=ns" \
      -H "Authorization: Token ${INFLUX_TOKEN}" \
      -H "Content-Type: text/plain; charset=utf-8" \
      --data-binary "${HR_DATA}" 2>&1)
    
    if [ $? -ne 0 ]; then
      echo "Error: cURL failed with status $?"
      echo "Output: $curl_result"
      exit 1
    fi
    
    debug_log "Batch request complete"
    HR_DATA=""
    current_batch=0
  fi
  
  # Add progress log every 100 entries
  if [ $((i % 100)) -eq 0 ] && [ $i -gt 0 ]; then
    echo "Processed $i of $HEART_RATE_COUNT heart rate entries..."
  fi
done

HR_END=$(date +%s.%N)
HR_ELAPSED=$(echo "$HR_END - $HR_START" | bc)
echo "Heart rate data loading took $HR_ELAPSED seconds"

# Create line protocol data for blood pressure
echo "==== Adding Blood Pressure Observations with Fluctuations ($BLOOD_PRESSURE_COUNT entries) ===="
BP_START=$(date +%s.%N)

# Use batching for better performance
current_batch=0
BP_DATA=""
debug_log "Starting blood pressure batch creation"

for ((i=0; i<$BLOOD_PRESSURE_COUNT; i++)); do
  time_offset=$((i*60))  # One per minute
  timestamp=$((START_TIME + time_offset))
  
  # Systolic with slight trend up and noise
  base_systolic=$((120 + (i*10/$BLOOD_PRESSURE_COUNT)))
  noise_s=$((RANDOM % 8 - 4))
  systolic=$((base_systolic + noise_s))
  
  # Diastolic with less change
  base_diastolic=$((80 + (i*4/$BLOOD_PRESSURE_COUNT)))
  noise_d=$((RANDOM % 6 - 3))
  diastolic=$((base_diastolic + noise_d))
  
  # Add to line protocol format
  BP_DATA="${BP_DATA}blood_pressure,patient_id=${PATIENT_ID},code=85354-9,component=systolic value=${systolic} ${timestamp}000000000
blood_pressure,patient_id=${PATIENT_ID},code=85354-9,component=diastolic value=${diastolic} ${timestamp}000000000
"
  current_batch=$((current_batch + 1))
  
  # Every BATCH_SIZE entries, send to InfluxDB
  if [ $current_batch -eq $BATCH_SIZE ] || [ $i -eq $((BLOOD_PRESSURE_COUNT - 1)) ]; then
    echo "Processing blood pressure batch: entries $((i-current_batch+1)) to $i..."
    
    debug_log "Preparing data payload, size: $(echo -n "$BP_DATA" | wc -c) bytes"
    debug_log "Sending request to $INFLUX_URL/api/v2/write"
    
    curl -s -XPOST "${INFLUX_URL}/api/v2/write?org=${INFLUX_ORG}&bucket=${INFLUX_BUCKET}&precision=ns" \
      -H "Authorization: Token ${INFLUX_TOKEN}" \
      -H "Content-Type: text/plain; charset=utf-8" \
      --data-binary "${BP_DATA}" > /dev/null
    
    debug_log "Batch request complete"
    BP_DATA=""
    current_batch=0
  fi
  
  # Add progress log every 100 entries
  if [ $((i % 100)) -eq 0 ] && [ $i -gt 0 ]; then
    echo "Processed $i of $BLOOD_PRESSURE_COUNT blood pressure entries..."
  fi
done

BP_END=$(date +%s.%N)
BP_ELAPSED=$(echo "$BP_END - $BP_START" | bc)
echo "Blood pressure data loading took $BP_ELAPSED seconds"

# Create line protocol data for SpO2
echo "==== Adding Oxygen Saturation with Outliers ($SPO2_COUNT entries) ===="
SPO2_START=$(date +%s.%N)

# Use batching for better performance
current_batch=0
SPO2_DATA=""
debug_log "Starting SpO2 batch creation"

for ((i=0; i<$SPO2_COUNT; i++)); do
  time_offset=$((i*60))  # One per minute
  timestamp=$((START_TIME + time_offset))
  
  # Normal oxygen saturation is 95-100%, add outliers periodically
  if [ $((i % 100)) -eq 50 ]; then
    # Outlier low - increased frequency for smaller dataset
    spo2=88
  elif [ $((i % 200)) -eq 150 ]; then
    # Another outlier (not as extreme)
    spo2=92
  else
    # Normal readings with minor noise
    base_spo2=98
    noise=$((RANDOM % 3 - 1))
    spo2=$((base_spo2 + noise))
    # Ensure we don't exceed 100%
    if [ $spo2 -gt 100 ]; then
      spo2=100
    fi
  fi
  
  # Add to line protocol format
  SPO2_DATA="${SPO2_DATA}spo2,patient_id=${PATIENT_ID},code=59408-5 value=${spo2} ${timestamp}000000000
"
  current_batch=$((current_batch + 1))
  
  # Every BATCH_SIZE entries, send to InfluxDB
  if [ $current_batch -eq $BATCH_SIZE ] || [ $i -eq $((SPO2_COUNT - 1)) ]; then
    echo "Processing SpO2 batch: entries $((i-current_batch+1)) to $i..."
    
    debug_log "Preparing data payload, size: $(echo -n "$SPO2_DATA" | wc -c) bytes"
    debug_log "Sending request to $INFLUX_URL/api/v2/write"
    
    curl -s -XPOST "${INFLUX_URL}/api/v2/write?org=${INFLUX_ORG}&bucket=${INFLUX_BUCKET}&precision=ns" \
      -H "Authorization: Token ${INFLUX_TOKEN}" \
      -H "Content-Type: text/plain; charset=utf-8" \
      --data-binary "${SPO2_DATA}" > /dev/null
    
    debug_log "Batch request complete"  
    SPO2_DATA=""
    current_batch=0
  fi
  
  # Add progress log every 100 entries
  if [ $((i % 100)) -eq 0 ] && [ $i -gt 0 ]; then
    echo "Processed $i of $SPO2_COUNT SpO2 entries..."
  fi
done

SPO2_END=$(date +%s.%N)
SPO2_ELAPSED=$(echo "$SPO2_END - $SPO2_START" | bc)
echo "Oxygen saturation data loading took $SPO2_ELAPSED seconds"

# Create ECG data
echo "==== Adding ECG Sampled Data ($ECG_COUNT entries) ===="
ECG_START=$(date +%s.%N)

# Use batching for better performance
current_batch=0
ECG_DATA=""
debug_log "Starting ECG batch creation"

for ((i=0; i<$ECG_COUNT; i++)); do
  # Calculate a realistic timestamp for ECG samples
  timestamp=$((START_TIME + (i/10)))  # Simulate 10Hz ECG sampling rate
  
  # Create a realistic ECG waveform pattern
  cycle_position=$((i % 25))  # Position within a 25-sample cycle (2.5 sec at 10Hz)
  
  if [ $cycle_position -eq 0 ]; then
    # R peak
    val="1.5"
  elif [ $cycle_position -eq 1 ]; then
    # S wave
    val="-0.5"
  elif [ $cycle_position -eq 5 ]; then
    # T wave
    val="0.75"
  else
    # Baseline with noise
    noise=$(awk -v seed=$RANDOM 'BEGIN {srand(seed); print rand()*0.1-0.05}')
    val=$noise
  fi
  
  # Add to line protocol format
  ECG_DATA="${ECG_DATA}ecg,patient_id=${PATIENT_ID},code=11524-6 value=${val} ${timestamp}000000000
"
  current_batch=$((current_batch + 1))
  
  # Every BATCH_SIZE entries, send to InfluxDB
  if [ $current_batch -eq $BATCH_SIZE ] || [ $i -eq $((ECG_COUNT - 1)) ]; then
    echo "Processing ECG batch: entries $((i-current_batch+1)) to $i..."
    
    debug_log "Preparing data payload, size: $(echo -n "$ECG_DATA" | wc -c) bytes"
    debug_log "Sending request to $INFLUX_URL/api/v2/write"
    
    curl -s -XPOST "${INFLUX_URL}/api/v2/write?org=${INFLUX_ORG}&bucket=${INFLUX_BUCKET}&precision=ns" \
      -H "Authorization: Token ${INFLUX_TOKEN}" \
      -H "Content-Type: text/plain; charset=utf-8" \
      --data-binary "${ECG_DATA}" > /dev/null
    
    debug_log "Batch request complete"
    ECG_DATA=""
    current_batch=0
  fi
  
  # Add progress log every 100 entries
  if [ $((i % 100)) -eq 0 ] && [ $i -gt 0 ]; then
    echo "Processed $i of $ECG_COUNT ECG entries..."
  fi
done

ECG_END=$(date +%s.%N)
ECG_ELAPSED=$(echo "$ECG_END - $ECG_START" | bc)
echo "ECG data loading took $ECG_ELAPSED seconds"

# Calculate data loading total time
DATA_LOADING_TOTAL=$(echo "$HR_ELAPSED + $BP_ELAPSED + $SPO2_ELAPSED + $ECG_ELAPSED" | bc)
echo -e "\nTotal data loading time: $DATA_LOADING_TOTAL seconds"

echo "Total entries loaded: $((HEART_RATE_COUNT + BLOOD_PRESSURE_COUNT + SPO2_COUNT + ECG_COUNT))"

sleep 2  # Give the server time to process records

echo -e "\n==== Testing Time-Series Queries ===="

# 1. Heart Rate Trend Analysis
echo -e "\n1. Trend Analysis for Heart Rate:"
TREND_HR_START=$(date +%s.%N)
debug_log "Running heart rate trend query"

curl -s -XPOST "${INFLUX_URL}/api/v2/query?org=${INFLUX_ORG}" \
  -H "Authorization: Token ${INFLUX_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "Accept: application/csv" \
  -d @- << EOF | head -5
{
  "query": "from(bucket: \"${INFLUX_BUCKET}\") |> range(start: ${START_TIME}) |> filter(fn: (r) => r._measurement == \"heart_rate\" and r.patient_id == \"${PATIENT_ID}\") |> aggregateWindow(every: 1h, fn: mean) |> yield(name: \"trend\")"
}
EOF
TREND_HR_END=$(date +%s.%N)
TREND_HR_ELAPSED=$(echo "$TREND_HR_END - $TREND_HR_START" | bc)
echo "Heart rate trend query took $TREND_HR_ELAPSED seconds"

# 2. SpO2 Statistics
echo -e "\n2. Statistics for Oxygen Saturation:"
STATS_SPO2_START=$(date +%s.%N)
debug_log "Running SpO2 statistics query"

curl -s -XPOST "${INFLUX_URL}/api/v2/query?org=${INFLUX_ORG}" \
  -H "Authorization: Token ${INFLUX_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "Accept: application/csv" \
  -d @- << EOF | head -5
{
  "query": "from(bucket: \"${INFLUX_BUCKET}\") |> range(start: ${START_TIME}) |> filter(fn: (r) => r._measurement == \"spo2\" and r.patient_id == \"${PATIENT_ID}\") |> mean() |> yield(name: \"mean\")"
}
EOF
STATS_SPO2_END=$(date +%s.%N)
STATS_SPO2_ELAPSED=$(echo "$STATS_SPO2_END - $STATS_SPO2_START" | bc)
echo "Oxygen saturation stats query took $STATS_SPO2_ELAPSED seconds"

# 3. SpO2 Outlier Detection - Fixed syntax
echo -e "\n3. Outlier Detection for Oxygen Saturation (Z-score):"
OUTLIERS_START=$(date +%s.%N)
debug_log "Running outlier detection query"

curl -s -XPOST "${INFLUX_URL}/api/v2/query?org=${INFLUX_ORG}" \
  -H "Authorization: Token ${INFLUX_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "Accept: application/csv" \
  -d @- << EOF | head -5
{
  "query": "meanVal = from(bucket: \"${INFLUX_BUCKET}\") |> range(start: ${START_TIME}) |> filter(fn: (r) => r._measurement == \"spo2\" and r.patient_id == \"${PATIENT_ID}\") |> mean() |> findRecord(fn: (key) => true, idx: 0)._value\nstdDev = from(bucket: \"${INFLUX_BUCKET}\") |> range(start: ${START_TIME}) |> filter(fn: (r) => r._measurement == \"spo2\" and r.patient_id == \"${PATIENT_ID}\") |> stddev() |> findRecord(fn: (key) => true, idx: 0)._value\nthreshold = 1.5\n\nfrom(bucket: \"${INFLUX_BUCKET}\") |> range(start: ${START_TIME}) |> filter(fn: (r) => r._measurement == \"spo2\" and r.patient_id == \"${PATIENT_ID}\") |> map(fn: (r) => ({r with zScore: (r._value - meanVal) / stdDev})) |> filter(fn: (r) => abs(r.zScore) > threshold)"
}
EOF
OUTLIERS_END=$(date +%s.%N)
OUTLIERS_ELAPSED=$(echo "$OUTLIERS_END - $OUTLIERS_START" | bc)
echo "Outlier detection query took $OUTLIERS_ELAPSED seconds"

# 4. Systolic BP Rate of Change
echo -e "\n4. Rate of Change for Blood Pressure (Systolic):"
RATE_SYS_START=$(date +%s.%N)
debug_log "Running systolic BP rate of change query"

curl -s -XPOST "${INFLUX_URL}/api/v2/query?org=${INFLUX_ORG}" \
  -H "Authorization: Token ${INFLUX_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "Accept: application/csv" \
  -d @- << EOF | head -5
{
  "query": "from(bucket: \"${INFLUX_BUCKET}\") |> range(start: ${START_TIME}) |> filter(fn: (r) => r._measurement == \"blood_pressure\" and r.component == \"systolic\" and r.patient_id == \"${PATIENT_ID}\") |> aggregateWindow(every: 1h, fn: mean) |> derivative(unit: 4h, nonNegative: false) |> yield(name: \"rate\")"
}
EOF
RATE_SYS_END=$(date +%s.%N)
RATE_SYS_ELAPSED=$(echo "$RATE_SYS_END - $RATE_SYS_START" | bc)
echo "Systolic BP rate of change query took $RATE_SYS_ELAPSED seconds"

# 5. Diastolic BP Rate of Change
echo -e "\n5. Rate of Change for Blood Pressure (Diastolic):"
RATE_DIA_START=$(date +%s.%N)
debug_log "Running diastolic BP rate of change query"

curl -s -XPOST "${INFLUX_URL}/api/v2/query?org=${INFLUX_ORG}" \
  -H "Authorization: Token ${INFLUX_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "Accept: application/csv" \
  -d @- << EOF | head -5
{
  "query": "from(bucket: \"${INFLUX_BUCKET}\") |> range(start: ${START_TIME}) |> filter(fn: (r) => r._measurement == \"blood_pressure\" and r.component == \"diastolic\" and r.patient_id == \"${PATIENT_ID}\") |> aggregateWindow(every: 1h, fn: mean) |> derivative(unit: 4h, nonNegative: false) |> yield(name: \"rate\")"
}
EOF
RATE_DIA_END=$(date +%s.%N)
RATE_DIA_ELAPSED=$(echo "$RATE_DIA_END - $RATE_DIA_START" | bc)
echo "Diastolic BP rate of change query took $RATE_DIA_ELAPSED seconds"

# 6. ECG Trend
echo -e "\n6. ECG Sampled Data:"
ECG_TREND_START=$(date +%s.%N)
debug_log "Running ECG trend query"

curl -s -XPOST "${INFLUX_URL}/api/v2/query?org=${INFLUX_ORG}" \
  -H "Authorization: Token ${INFLUX_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "Accept: application/csv" \
  -d @- << EOF | head -5
{
  "query": "from(bucket: \"${INFLUX_BUCKET}\") |> range(start: ${START_TIME}) |> filter(fn: (r) => r._measurement == \"ecg\" and r.patient_id == \"${PATIENT_ID}\") |> limit(n: 20) |> yield(name: \"ecg_samples\")"
}
EOF
ECG_TREND_END=$(date +%s.%N)
ECG_TREND_ELAPSED=$(echo "$ECG_TREND_END - $ECG_TREND_START" | bc)
echo "ECG trend query took $ECG_TREND_ELAPSED seconds"

# 7. All Trends - Fixed syntax
echo -e "\n7. All Trends by Resource Type:"
ALL_TRENDS_START=$(date +%s.%N)
debug_log "Running all trends query"

curl -s -XPOST "${INFLUX_URL}/api/v2/query?org=${INFLUX_ORG}" \
  -H "Authorization: Token ${INFLUX_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "Accept: application/csv" \
  -d @- << EOF | head -5
{
  "query": "heartRate = from(bucket: \"${INFLUX_BUCKET}\") |> range(start: ${START_TIME}) |> filter(fn: (r) => r._measurement == \"heart_rate\") |> mean() |> map(fn: (r) => ({_value: r._value, metric: \"heart_rate\"}))\nbloodPressure = from(bucket: \"${INFLUX_BUCKET}\") |> range(start: ${START_TIME}) |> filter(fn: (r) => r._measurement == \"blood_pressure\") |> mean() |> map(fn: (r) => ({_value: r._value, metric: \"blood_pressure\"}))\nspo2 = from(bucket: \"${INFLUX_BUCKET}\") |> range(start: ${START_TIME}) |> filter(fn: (r) => r._measurement == \"spo2\") |> mean() |> map(fn: (r) => ({_value: r._value, metric: \"spo2\"}))\necg = from(bucket: \"${INFLUX_BUCKET}\") |> range(start: ${START_TIME}) |> filter(fn: (r) => r._measurement == \"ecg\") |> mean() |> map(fn: (r) => ({_value: r._value, metric: \"ecg\"}))\nunion(tables: [heartRate, bloodPressure, spo2, ecg])"
}
EOF
ALL_TRENDS_END=$(date +%s.%N)
ALL_TRENDS_ELAPSED=$(echo "$ALL_TRENDS_END - $ALL_TRENDS_START" | bc)
echo "All trends query took $ALL_TRENDS_ELAPSED seconds"

# Calculate query time total
QUERY_TOTAL=$(echo "$TREND_HR_ELAPSED + $STATS_SPO2_ELAPSED + $OUTLIERS_ELAPSED + $RATE_SYS_ELAPSED + $RATE_DIA_ELAPSED + $ECG_TREND_ELAPSED + $ALL_TRENDS_ELAPSED" | bc)
echo -e "\nTotal query execution time: $QUERY_TOTAL seconds"

# Calculate total benchmark time
BENCHMARK_END=$(date +%s.%N)
TOTAL_TIME=$(echo "$BENCHMARK_END - $BENCHMARK_START" | bc)

echo -e "\n==== INFLUXDB BENCHMARK SUMMARY ===="
echo "Data loading time: $DATA_LOADING_TOTAL seconds"
echo "Query execution time: $QUERY_TOTAL seconds"
echo "Total execution time: $TOTAL_TIME seconds"
echo -e "\nTest completed!" 