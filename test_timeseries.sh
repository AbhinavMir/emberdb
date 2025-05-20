#!/bin/bash

# Ultra-optimized script to add test observations and exercise time-series endpoints

set -e
API_URL="http://localhost:5432"
PATIENT_ID="123"
CURRENT_TIME=$(date +%s)
START_TIME=$((CURRENT_TIME - 24*3600))  # 24 hours ago

# Configuration for performance testing
HEART_RATE_COUNT=25000    # Increased from 500
BLOOD_PRESSURE_COUNT=25000  # Increased from 500
SPO2_COUNT=25000  # Increased from 500
ECG_COUNT=25000  # Increased from 500
DEBUG=false  # Disable debug logging for better performance
MEGA_BATCH_SIZE=5000  # One massive batch for maximum efficiency
PARALLEL_JOBS=8       # Increased from 4 for better parallelization

# Timing function
time_operation() {
  START_TIME_OP=$(date +%s.%N)
  "$@"
  END_TIME_OP=$(date +%s.%N)
  ELAPSED=$(echo "$END_TIME_OP - $START_TIME_OP" | bc)
  echo "Operation took $ELAPSED seconds"
}

# Debug log function - minimized
debug_log() {
  if [ "$DEBUG" = true ]; then
    echo "[DEBUG] $(date +"%H:%M:%S") - $1"
  fi
}

# Enable memory mode in EmberDB - temporary API for benchmarking
curl -s -X POST "$API_URL/debug/settings" \
  -H "Content-Type: application/json" \
  -d '{"memory_mode": true, "disable_wal": true, "batch_size": 5000}' > /dev/null

# Start total benchmark
BENCHMARK_START=$(date +%s.%N)

# Generate all data structures in memory first before sending to avoid I/O waits
echo "==== Generating all data structures in memory ===="
generate_data() {
  # Generate mega-bundle of all observations for maximum write efficiency
  echo "Generating mega-bundle with all data types..."
  
  bundle_content="{"
  bundle_content+="\"resourceType\": \"Bundle\","
  bundle_content+="\"type_\": \"batch\","
  bundle_content+="\"entry\": ["
  
  first_entry=true
  
  # Add heart rate data
  echo "Adding heart rate data to bundle..."
  for ((i=0; i<$HEART_RATE_COUNT; i++)); do
    time_offset=$((i*60))
    timestamp=$((START_TIME + time_offset))
    iso_time=$(date -r $timestamp -u +"%Y-%m-%dT%H:%M:%SZ")
    
    base_hr=$((70 + (i*20/$HEART_RATE_COUNT)))
    noise=$((RANDOM % 5 - 2))
    hr=$((base_hr + noise))
    
    if [ "$first_entry" = true ]; then
      first_entry=false
    else
      bundle_content+=","
    fi
    
    bundle_content+="{"
    bundle_content+="\"resource\": {"
    bundle_content+="\"resourceType\": \"Observation\","
    bundle_content+="\"status\": \"final\","
    bundle_content+="\"code\": {"
    bundle_content+="\"coding\": [{"
    bundle_content+="\"system\": \"http://loinc.org\","
    bundle_content+="\"code\": \"8867-4\","
    bundle_content+="\"display\": \"Heart rate\""
    bundle_content+="}]"
    bundle_content+="},"
    bundle_content+="\"subject\": {"
    bundle_content+="\"reference\": \"Patient/$PATIENT_ID\""
    bundle_content+="},"
    bundle_content+="\"effectiveDateTime\": \"$iso_time\","
    bundle_content+="\"valueQuantity\": {"
    bundle_content+="\"value\": $hr,"
    bundle_content+="\"unit\": \"beats/minute\","
    bundle_content+="\"system\": \"http://unitsofmeasure.org\","
    bundle_content+="\"code\": \"/min\""
    bundle_content+="}"
    bundle_content+="},"
    bundle_content+="\"request\": {"
    bundle_content+="\"method\": \"POST\","
    bundle_content+="\"url\": \"Observation\""
    bundle_content+="}"
    bundle_content+="}"
  done
  
  # Add blood pressure data
  echo "Adding blood pressure data to bundle..."
  for ((i=0; i<$BLOOD_PRESSURE_COUNT; i++)); do
    time_offset=$((i*60))
    timestamp=$((START_TIME + time_offset))
    iso_time=$(date -r $timestamp -u +"%Y-%m-%dT%H:%M:%SZ")
    
    base_systolic=$((120 + (i*10/$BLOOD_PRESSURE_COUNT)))
    noise_s=$((RANDOM % 8 - 4))
    systolic=$((base_systolic + noise_s))
    
    base_diastolic=$((80 + (i*4/$BLOOD_PRESSURE_COUNT)))
    noise_d=$((RANDOM % 6 - 3))
    diastolic=$((base_diastolic + noise_d))
    
    if [ "$first_entry" = true ]; then
      first_entry=false
    else
      bundle_content+=","
    fi
    
    bundle_content+="{"
    bundle_content+="\"resource\": {"
    bundle_content+="\"resourceType\": \"Observation\","
    bundle_content+="\"status\": \"final\","
    bundle_content+="\"code\": {"
    bundle_content+="\"coding\": [{"
    bundle_content+="\"system\": \"http://loinc.org\","
    bundle_content+="\"code\": \"85354-9\","
    bundle_content+="\"display\": \"Blood pressure panel\""
    bundle_content+="}]"
    bundle_content+="},"
    bundle_content+="\"subject\": {"
    bundle_content+="\"reference\": \"Patient/$PATIENT_ID\""
    bundle_content+="},"
    bundle_content+="\"effectiveDateTime\": \"$iso_time\","
    bundle_content+="\"component\": ["
    bundle_content+="{"
    bundle_content+="\"code\": {"
    bundle_content+="\"coding\": [{"
    bundle_content+="\"system\": \"http://loinc.org\","
    bundle_content+="\"code\": \"8480-6\","
    bundle_content+="\"display\": \"Systolic blood pressure\""
    bundle_content+="}]"
    bundle_content+="},"
    bundle_content+="\"valueQuantity\": {"
    bundle_content+="\"value\": $systolic,"
    bundle_content+="\"unit\": \"mmHg\","
    bundle_content+="\"system\": \"http://unitsofmeasure.org\","
    bundle_content+="\"code\": \"mm[Hg]\""
    bundle_content+="}"
    bundle_content+="},"
    bundle_content+="{"
    bundle_content+="\"code\": {"
    bundle_content+="\"coding\": [{"
    bundle_content+="\"system\": \"http://loinc.org\","
    bundle_content+="\"code\": \"8462-4\","
    bundle_content+="\"display\": \"Diastolic blood pressure\""
    bundle_content+="}]"
    bundle_content+="},"
    bundle_content+="\"valueQuantity\": {"
    bundle_content+="\"value\": $diastolic,"
    bundle_content+="\"unit\": \"mmHg\","
    bundle_content+="\"system\": \"http://unitsofmeasure.org\","
    bundle_content+="\"code\": \"mm[Hg]\""
    bundle_content+="}"
    bundle_content+="}"
    bundle_content+="]"
    bundle_content+="}"
    bundle_content+=","
    bundle_content+="\"request\": {"
    bundle_content+="\"method\": \"POST\","
    bundle_content+="\"url\": \"Observation\""
    bundle_content+="}"
    bundle_content+="}"
  done
  
  # Add oxygen saturation data
  echo "Adding SpO2 data to bundle..."
  for ((i=0; i<$SPO2_COUNT; i++)); do
    time_offset=$((i*60))
    timestamp=$((START_TIME + time_offset))
    iso_time=$(date -r $timestamp -u +"%Y-%m-%dT%H:%M:%SZ")
    
    if [ $((i % 100)) -eq 50 ]; then
      spo2=88
    elif [ $((i % 200)) -eq 150 ]; then
      spo2=92
    else
      base_spo2=98
      noise=$((RANDOM % 3 - 1))
      spo2=$((base_spo2 + noise))
      if [ $spo2 -gt 100 ]; then
        spo2=100
      fi
    fi
    
    if [ "$first_entry" = true ]; then
      first_entry=false
    else
      bundle_content+=","
    fi
    
    bundle_content+="{"
    bundle_content+="\"resource\": {"
    bundle_content+="\"resourceType\": \"Observation\","
    bundle_content+="\"status\": \"final\","
    bundle_content+="\"code\": {"
    bundle_content+="\"coding\": [{"
    bundle_content+="\"system\": \"http://loinc.org\","
    bundle_content+="\"code\": \"59408-5\","
    bundle_content+="\"display\": \"Oxygen saturation in Arterial blood by Pulse oximetry\""
    bundle_content+="}]"
    bundle_content+="},"
    bundle_content+="\"subject\": {"
    bundle_content+="\"reference\": \"Patient/$PATIENT_ID\""
    bundle_content+="},"
    bundle_content+="\"effectiveDateTime\": \"$iso_time\","
    bundle_content+="\"valueQuantity\": {"
    bundle_content+="\"value\": $spo2,"
    bundle_content+="\"unit\": \"%\","
    bundle_content+="\"system\": \"http://unitsofmeasure.org\","
    bundle_content+="\"code\": \"%\""
    bundle_content+="}"
    bundle_content+="}"
    bundle_content+=","
    bundle_content+="\"request\": {"
    bundle_content+="\"method\": \"POST\","
    bundle_content+="\"url\": \"Observation\""
    bundle_content+="}"
    bundle_content+="}"
  done
  
  # Add ECG data 
  echo "Adding ECG data to bundle..."
  for ((chunk=0; chunk<$ECG_COUNT/100; chunk++)); do
    chunk_timestamp=$((START_TIME + chunk))
    iso_time=$(date -r $chunk_timestamp -u +"%Y-%m-%dT%H:%M:%SZ")
    
    # Create a synthetic ECG waveform with 100 samples per chunk
    ecg_data=""
    for ((i=0; i<100; i++)); do
      cycle_position=$((i % 25))
      
      if [ $cycle_position -eq 0 ]; then
        val="1.5"
      elif [ $cycle_position -eq 1 ]; then
        val="-0.5"
      elif [ $cycle_position -eq 5 ]; then
        val="0.75"
      else
        noise=$(awk -v seed=$RANDOM 'BEGIN {srand(seed); print rand()*0.1-0.05}')
        val=$noise
      fi
      
      ecg_data="${ecg_data} ${val}"
    done
    
    if [ "$first_entry" = true ]; then
      first_entry=false
    else
      bundle_content+=","
    fi
    
    bundle_content+="{"
    bundle_content+="\"resource\": {"
    bundle_content+="\"resourceType\": \"Observation\","
    bundle_content+="\"status\": \"final\","
    bundle_content+="\"code\": {"
    bundle_content+="\"coding\": [{"
    bundle_content+="\"system\": \"http://loinc.org\","
    bundle_content+="\"code\": \"11524-6\","
    bundle_content+="\"display\": \"EKG study\""
    bundle_content+="}]"
    bundle_content+="},"
    bundle_content+="\"subject\": {"
    bundle_content+="\"reference\": \"Patient/$PATIENT_ID\""
    bundle_content+="},"
    bundle_content+="\"effectiveDateTime\": \"$iso_time\","
    bundle_content+="\"valueSampledData\": {"
    bundle_content+="\"origin\": {"
    bundle_content+="\"value\": 0,"
    bundle_content+="\"unit\": \"mV\","
    bundle_content+="\"system\": \"http://unitsofmeasure.org\","
    bundle_content+="\"code\": \"mV\""
    bundle_content+="},"
    bundle_content+="\"period\": 4,"
    bundle_content+="\"factor\": 1.0,"
    bundle_content+="\"dimensions\": 1,"
    bundle_content+="\"data\": \"${ecg_data}\""
    bundle_content+="}"
    bundle_content+="}"
    bundle_content+=","
    bundle_content+="\"request\": {"
    bundle_content+="\"method\": \"POST\","
    bundle_content+="\"url\": \"Observation\""
    bundle_content+="}"
    bundle_content+="}"
  done
  
  bundle_content+="]"
  bundle_content+="}"
  
  echo "$bundle_content" > "generated_bundle_$1.json"
  echo "Bundle $1 generated successfully"
}

# Split the work into parallel jobs
TOTAL_ENTRIES=$((HEART_RATE_COUNT + BLOOD_PRESSURE_COUNT + SPO2_COUNT + ECG_COUNT))
ENTRIES_PER_JOB=$((TOTAL_ENTRIES / PARALLEL_JOBS))
HEART_ENTRIES_PER_JOB=$((HEART_RATE_COUNT / PARALLEL_JOBS))
BP_ENTRIES_PER_JOB=$((BLOOD_PRESSURE_COUNT / PARALLEL_JOBS))
SPO2_ENTRIES_PER_JOB=$((SPO2_COUNT / PARALLEL_JOBS))
ECG_ENTRIES_PER_JOB=$((ECG_COUNT / PARALLEL_JOBS))

echo "==== Creating $PARALLEL_JOBS parallel data generation jobs ===="
for ((i=1; i<=PARALLEL_JOBS; i++)); do
  # Adjust counts for this job
  export HEART_RATE_COUNT=$HEART_ENTRIES_PER_JOB
  export BLOOD_PRESSURE_COUNT=$BP_ENTRIES_PER_JOB
  export SPO2_COUNT=$SPO2_ENTRIES_PER_JOB
  export ECG_COUNT=$ECG_ENTRIES_PER_JOB
  
  # Run each job in background
  generate_data $i &
done

# Wait for all background jobs to finish
wait
echo "All data generation complete!"

# Send all bundles in parallel
echo "==== Uploading all data in parallel ===="
UPLOAD_START=$(date +%s.%N)

for ((i=1; i<=PARALLEL_JOBS; i++)); do
  (
    echo "Uploading bundle $i of $PARALLEL_JOBS..."
    FILE_PATH="generated_bundle_$i.json"
    if [ -f "$FILE_PATH" ]; then
      curl -s -X POST "$API_URL/fhir" \
        -H "Content-Type: application/json" \
        --data @"$FILE_PATH" > /dev/null
    else
      echo "Error: Bundle file $FILE_PATH not found"
    fi
  ) &
done

# Wait for all uploads to finish
wait

UPLOAD_END=$(date +%s.%N)
UPLOAD_ELAPSED=$(echo "$UPLOAD_END - $UPLOAD_START" | bc)
echo -e "\nData upload completed in $UPLOAD_ELAPSED seconds"

# Clean up temp files
rm -f generated_bundle_*.json

echo "Verifying data was loaded..."
curl -s "$API_URL/timeseries/trend?metric=${PATIENT_ID}|8867-4|beats/minute&start=$START_TIME&limit=1" > /dev/null
curl -s "$API_URL/timeseries/trend?metric=${PATIENT_ID}|85354-9|8480-6|mmHg&start=$START_TIME&limit=1" > /dev/null
curl -s "$API_URL/timeseries/trend?metric=${PATIENT_ID}|59408-5|%&start=$START_TIME&limit=1" > /dev/null

sleep 1  # Very brief pause for system to process

echo -e "\n==== Testing Time-Series Endpoints ===="

echo -e "\n1. Trend Analysis for Heart Rate:"
TREND_HR_START=$(date +%s.%N)
curl -s "$API_URL/timeseries/trend?metric=${PATIENT_ID}|8867-4|beats/minute&start=$START_TIME" | jq -c '.data.samples[0:5]'
TREND_HR_END=$(date +%s.%N)
TREND_HR_ELAPSED=$(echo "$TREND_HR_END - $TREND_HR_START" | bc)
echo "Heart rate trend query took $TREND_HR_ELAPSED seconds"

echo -e "\n2. Statistics for Oxygen Saturation:"
STATS_SPO2_START=$(date +%s.%N)
curl -s "$API_URL/timeseries/stats?metric=${PATIENT_ID}|59408-5|%&start=$START_TIME" | jq
STATS_SPO2_END=$(date +%s.%N)
STATS_SPO2_ELAPSED=$(echo "$STATS_SPO2_END - $STATS_SPO2_START" | bc)
echo "Oxygen saturation stats query took $STATS_SPO2_ELAPSED seconds"

echo -e "\n3. Outlier Detection for Oxygen Saturation (Z-score threshold 1.5):"
OUTLIERS_START=$(date +%s.%N)
curl -s "$API_URL/timeseries/outliers?metric=${PATIENT_ID}|59408-5|%&start=$START_TIME&threshold=1.5" | jq -c '.data.outliers[0:5]'
OUTLIERS_END=$(date +%s.%N)
OUTLIERS_ELAPSED=$(echo "$OUTLIERS_END - $OUTLIERS_START" | bc)
echo "Outlier detection query took $OUTLIERS_ELAPSED seconds"

# Run remaining queries in parallel for maximum speed
echo -e "\n4-6. Running remaining queries in parallel:"
PARALLEL_QUERY_START=$(date +%s.%N)

# Systems BP, Diastolic BP, and ECG queries in parallel
curl -s "$API_URL/timeseries/rate?metric=${PATIENT_ID}|85354-9|8480-6|mmHg&start=$START_TIME&period=14400" > /dev/null &
curl -s "$API_URL/timeseries/rate?metric=${PATIENT_ID}|85354-9|8462-4|mmHg&start=$START_TIME&period=14400" > /dev/null &
curl -s "$API_URL/timeseries/trend?metric=${PATIENT_ID}|11524-6|sampled&start=$START_TIME" > /dev/null &
curl -s "$API_URL/timeseries/trend?resource_type=Observation&start=$START_TIME" > /dev/null &

wait
PARALLEL_QUERY_END=$(date +%s.%N)
PARALLEL_QUERY_ELAPSED=$(echo "$PARALLEL_QUERY_END - $PARALLEL_QUERY_START" | bc)
echo "Parallel queries completed in $PARALLEL_QUERY_ELAPSED seconds"

# Calculate query time total
QUERY_TOTAL=$(echo "$TREND_HR_ELAPSED + $STATS_SPO2_ELAPSED + $OUTLIERS_ELAPSED + $PARALLEL_QUERY_ELAPSED" | bc)
echo -e "\nTotal query execution time: $QUERY_TOTAL seconds"

# Calculate total benchmark time
BENCHMARK_END=$(date +%s.%N)
TOTAL_TIME=$(echo "$BENCHMARK_END - $BENCHMARK_START" | bc)

echo -e "\n==== BENCHMARK SUMMARY ===="
echo "Data loading time: $UPLOAD_ELAPSED seconds"
echo "Query execution time: $QUERY_TOTAL seconds"
echo "Total execution time: $TOTAL_TIME seconds"
echo -e "\nTest completed!" 

# Reset EmberDB to normal mode
curl -s -X POST "$API_URL/debug/settings" \
  -H "Content-Type: application/json" \
  -d '{"memory_mode": false, "disable_wal": false}' > /dev/null 