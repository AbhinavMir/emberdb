#!/bin/bash

# Script to add test observations and exercise time-series endpoints (1M entries)

set -e
API_URL="http://localhost:5432"
PATIENT_ID="123"
CURRENT_TIME=$(date +%s)
START_TIME=$((CURRENT_TIME - 24*3600))  # 24 hours ago

# Configuration for large dataset
HEART_RATE_COUNT=250000
BLOOD_PRESSURE_COUNT=250000
SPO2_COUNT=250000
ECG_COUNT=250000
BATCH_SIZE=100  # FHIR servers typically handle smaller batches than InfluxDB

# Timing function
time_operation() {
  START_TIME_OP=$(date +%s.%N)
  "$@"
  END_TIME_OP=$(date +%s.%N)
  ELAPSED=$(echo "$END_TIME_OP - $START_TIME_OP" | bc)
  echo "Operation took $ELAPSED seconds"
}

# Start total benchmark
BENCHMARK_START=$(date +%s.%N)

echo "==== Adding Heart Rate Observations with Trend ($HEART_RATE_COUNT entries) ===="
# Add heart rate observations with a clear upward trend
HR_START=$(date +%s.%N)

# Use batching for better performance
current_batch=0
BATCH_DATA="["

for ((i=0; i<$HEART_RATE_COUNT; i++)); do
  # Calculate timestamp with more granularity
  time_offset=$((i*60))  # One per minute instead of hour to get more data points
  timestamp=$((START_TIME + time_offset))
  iso_time=$(date -r $timestamp -u +"%Y-%m-%dT%H:%M:%SZ")
  
  # Generate heart rate with upward trend (70 to 90) plus some noise
  base_hr=$((70 + (i*20/$HEART_RATE_COUNT)))
  noise=$((RANDOM % 5 - 2))
  hr=$((base_hr + noise))
  
  # Add comma if not the first entry in the batch
  if [ $current_batch -gt 0 ]; then
    BATCH_DATA="${BATCH_DATA},"
  fi
  
  # Create FHIR Observation resource
  BATCH_DATA="${BATCH_DATA}
  {
    \"resourceType\": \"Observation\",
    \"status\": \"final\",
    \"code\": {
      \"coding\": [
        {
          \"system\": \"http://loinc.org\",
          \"code\": \"8867-4\",
          \"display\": \"Heart rate\"
        }
      ]
    },
    \"subject\": {
      \"reference\": \"Patient/${PATIENT_ID}\"
    },
    \"effectiveDateTime\": \"${iso_time}\",
    \"valueQuantity\": {
      \"value\": ${hr},
      \"unit\": \"beats/minute\",
      \"system\": \"http://unitsofmeasure.org\",
      \"code\": \"/min\"
    }
  }"
  
  current_batch=$((current_batch + 1))
  
  # Every BATCH_SIZE entries, send to the server
  if [ $current_batch -eq $BATCH_SIZE ] || [ $i -eq $((HEART_RATE_COUNT - 1)) ]; then
    if [ $((i % 10000)) -eq 0 ]; then
      echo "Adding heart rate batch at entry $i..."
    fi
    
    # Close the JSON array
    BATCH_DATA="${BATCH_DATA}]"
    
    # Send the batch to the server
    curl -s -X POST "$API_URL/fhir/Bundle" \
      -H "Content-Type: application/json" \
      -d "{
        \"resourceType\": \"Bundle\",
        \"type\": \"batch\",
        \"entry\": $(echo "$BATCH_DATA" | jq -c '[ .[] | {"resource": ., "request": {"method": "POST", "url": "Observation"}} ]')
      }" > /dev/null
    
    # Reset for the next batch
    BATCH_DATA="["
    current_batch=0
  fi
done

HR_END=$(date +%s.%N)
HR_ELAPSED=$(echo "$HR_END - $HR_START" | bc)
echo "Heart rate data loading took $HR_ELAPSED seconds"

echo "==== Adding Blood Pressure Observations with Fluctuations ($BLOOD_PRESSURE_COUNT entries) ===="
# Add blood pressure observations with some fluctuations
BP_START=$(date +%s.%N)

# Use batching for better performance
current_batch=0
BATCH_DATA="["

for ((i=0; i<$BLOOD_PRESSURE_COUNT; i++)); do
  time_offset=$((i*60))  # One per minute
  timestamp=$((START_TIME + time_offset))
  iso_time=$(date -r $timestamp -u +"%Y-%m-%dT%H:%M:%SZ")
  
  # Systolic with slight trend up and noise
  base_systolic=$((120 + (i*10/$BLOOD_PRESSURE_COUNT)))
  noise_s=$((RANDOM % 8 - 4))
  systolic=$((base_systolic + noise_s))
  
  # Diastolic with less change
  base_diastolic=$((80 + (i*4/$BLOOD_PRESSURE_COUNT)))
  noise_d=$((RANDOM % 6 - 3))
  diastolic=$((base_diastolic + noise_d))
  
  # Add comma if not the first entry in the batch
  if [ $current_batch -gt 0 ]; then
    BATCH_DATA="${BATCH_DATA},"
  fi
  
  # Create FHIR Observation resource
  BATCH_DATA="${BATCH_DATA}
  {
    \"resourceType\": \"Observation\",
    \"status\": \"final\",
    \"code\": {
      \"coding\": [
        {
          \"system\": \"http://loinc.org\",
          \"code\": \"85354-9\",
          \"display\": \"Blood pressure panel\"
        }
      ]
    },
    \"subject\": {
      \"reference\": \"Patient/${PATIENT_ID}\"
    },
    \"effectiveDateTime\": \"${iso_time}\",
    \"component\": [
      {
        \"code\": {
          \"coding\": [
            {
              \"system\": \"http://loinc.org\",
              \"code\": \"8480-6\",
              \"display\": \"Systolic blood pressure\"
            }
          ]
        },
        \"valueQuantity\": {
          \"value\": ${systolic},
          \"unit\": \"mmHg\",
          \"system\": \"http://unitsofmeasure.org\",
          \"code\": \"mm[Hg]\"
        }
      },
      {
        \"code\": {
          \"coding\": [
            {
              \"system\": \"http://loinc.org\",
              \"code\": \"8462-4\",
              \"display\": \"Diastolic blood pressure\"
            }
          ]
        },
        \"valueQuantity\": {
          \"value\": ${diastolic},
          \"unit\": \"mmHg\",
          \"system\": \"http://unitsofmeasure.org\",
          \"code\": \"mm[Hg]\"
        }
      }
    ]
  }"
  
  current_batch=$((current_batch + 1))
  
  # Every BATCH_SIZE entries, send to the server
  if [ $current_batch -eq $BATCH_SIZE ] || [ $i -eq $((BLOOD_PRESSURE_COUNT - 1)) ]; then
    if [ $((i % 10000)) -eq 0 ]; then
      echo "Adding blood pressure batch at entry $i..."
    fi
    
    # Close the JSON array
    BATCH_DATA="${BATCH_DATA}]"
    
    # Send the batch to the server
    curl -s -X POST "$API_URL/fhir/Bundle" \
      -H "Content-Type: application/json" \
      -d "{
        \"resourceType\": \"Bundle\",
        \"type\": \"batch\",
        \"entry\": $(echo "$BATCH_DATA" | jq -c '[ .[] | {"resource": ., "request": {"method": "POST", "url": "Observation"}} ]')
      }" > /dev/null
    
    # Reset for the next batch
    BATCH_DATA="["
    current_batch=0
  fi
done

BP_END=$(date +%s.%N)
BP_ELAPSED=$(echo "$BP_END - $BP_START" | bc)
echo "Blood pressure data loading took $BP_ELAPSED seconds"

echo "==== Adding Oxygen Saturation with Outliers ($SPO2_COUNT entries) ===="
# Add oxygen saturation with outliers
SPO2_START=$(date +%s.%N)

# Use batching for better performance
current_batch=0
BATCH_DATA="["

for ((i=0; i<$SPO2_COUNT; i++)); do
  time_offset=$((i*60))  # One per minute
  timestamp=$((START_TIME + time_offset))
  iso_time=$(date -r $timestamp -u +"%Y-%m-%dT%H:%M:%SZ")
  
  # Normal oxygen saturation is 95-100%, add outliers periodically
  if [ $((i % 10000)) -eq 5000 ]; then
    # Outlier low
    spo2=88
  elif [ $((i % 20000)) -eq 15000 ]; then
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
  
  # Add comma if not the first entry in the batch
  if [ $current_batch -gt 0 ]; then
    BATCH_DATA="${BATCH_DATA},"
  fi
  
  # Create FHIR Observation resource
  BATCH_DATA="${BATCH_DATA}
  {
    \"resourceType\": \"Observation\",
    \"status\": \"final\",
    \"code\": {
      \"coding\": [
        {
          \"system\": \"http://loinc.org\",
          \"code\": \"59408-5\",
          \"display\": \"Oxygen saturation in Arterial blood by Pulse oximetry\"
        }
      ]
    },
    \"subject\": {
      \"reference\": \"Patient/${PATIENT_ID}\"
    },
    \"effectiveDateTime\": \"${iso_time}\",
    \"valueQuantity\": {
      \"value\": ${spo2},
      \"unit\": \"%\",
      \"system\": \"http://unitsofmeasure.org\",
      \"code\": \"%\"
    }
  }"
  
  current_batch=$((current_batch + 1))
  
  # Every BATCH_SIZE entries, send to the server
  if [ $current_batch -eq $BATCH_SIZE ] || [ $i -eq $((SPO2_COUNT - 1)) ]; then
    if [ $((i % 10000)) -eq 0 ]; then
      echo "Adding SpO2 batch at entry $i..."
    fi
    
    # Close the JSON array
    BATCH_DATA="${BATCH_DATA}]"
    
    # Send the batch to the server
    curl -s -X POST "$API_URL/fhir/Bundle" \
      -H "Content-Type: application/json" \
      -d "{
        \"resourceType\": \"Bundle\",
        \"type\": \"batch\",
        \"entry\": $(echo "$BATCH_DATA" | jq -c '[ .[] | {"resource": ., "request": {"method": "POST", "url": "Observation"}} ]')
      }" > /dev/null
    
    # Reset for the next batch
    BATCH_DATA="["
    current_batch=0
  fi
done

SPO2_END=$(date +%s.%N)
SPO2_ELAPSED=$(echo "$SPO2_END - $SPO2_START" | bc)
echo "Oxygen saturation data loading took $SPO2_ELAPSED seconds"

echo "==== Adding ECG Sampled Data ($ECG_COUNT entries) ===="
# For ECG, we'll use a different approach since we need to create sampled data
ECG_START=$(date +%s.%N)

# Create ECG data in smaller chunks due to the size
ECG_CHUNKS=$((ECG_COUNT / 1000))  # Split into 1000-sample chunks

for ((chunk=0; chunk<$ECG_CHUNKS; chunk++)); do
  if [ $((chunk % 10)) -eq 0 ]; then
    echo "Processing ECG chunk $chunk of $ECG_CHUNKS..."
  fi

  # Base timestamp for this chunk
  chunk_timestamp=$((START_TIME + chunk))
  iso_time=$(date -r $chunk_timestamp -u +"%Y-%m-%dT%H:%M:%SZ")
  
  # Create a synthetic ECG waveform with 1000 samples per chunk
  ecg_data=""
  for ((i=0; i<1000; i++)); do
    # A simplified ECG-like pattern (repeating every 250 samples)
    cycle_position=$((i % 250))
    
    if [ $cycle_position -eq 0 ]; then
      # R peak
      val="1.5"
    elif [ $cycle_position -eq 1 ]; then
      # S wave
      val="-0.5"
    elif [ $cycle_position -eq 25 ]; then
      # T wave
      val="0.75"
    else
      # Baseline with noise
      noise=$(awk -v seed=$RANDOM 'BEGIN {srand(seed); print rand()*0.1-0.05}')
      val=$noise
    fi
    
    ecg_data="${ecg_data} ${val}"
  done
  
  # Send the ECG data
  curl -s -X POST "$API_URL/fhir/Observation" \
    -H "Content-Type: application/json" \
    -d "{
      \"resourceType\": \"Observation\",
      \"status\": \"final\", 
      \"code\": {
        \"coding\": [
          {
            \"system\": \"http://loinc.org\",
            \"code\": \"11524-6\",
            \"display\": \"EKG study\"
          }
        ]
      },
      \"subject\": {
        \"reference\": \"Patient/${PATIENT_ID}\"
      },
      \"effectiveDateTime\": \"${iso_time}\",
      \"valueSampledData\": {
        \"origin\": {
          \"value\": 0,
          \"unit\": \"mV\",
          \"system\": \"http://unitsofmeasure.org\",
          \"code\": \"mV\"
        },
        \"period\": 4,
        \"factor\": 1.0,
        \"dimensions\": 1,
        \"data\": \"${ecg_data}\"
      }
    }" > /dev/null
done

ECG_END=$(date +%s.%N)
ECG_ELAPSED=$(echo "$ECG_END - $ECG_START" | bc)
echo "ECG data loading took $ECG_ELAPSED seconds"

# Calculate data loading total time
DATA_LOADING_TOTAL=$(echo "$HR_ELAPSED + $BP_ELAPSED + $SPO2_ELAPSED + $ECG_ELAPSED" | bc)
echo -e "\nTotal data loading time: $DATA_LOADING_TOTAL seconds"

echo "Total entries loaded: $((HEART_RATE_COUNT + BLOOD_PRESSURE_COUNT + SPO2_COUNT + ECG_COUNT))"

sleep 2  # Give the server time to process records

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

echo -e "\n4. Rate of Change for Blood Pressure (Systolic):"
RATE_SYS_START=$(date +%s.%N)
curl -s "$API_URL/timeseries/rate?metric=${PATIENT_ID}|85354-9|8480-6|mmHg&start=$START_TIME&period=14400" | jq -c '.[0:5]'
RATE_SYS_END=$(date +%s.%N)
RATE_SYS_ELAPSED=$(echo "$RATE_SYS_END - $RATE_SYS_START" | bc)
echo "Systolic BP rate of change query took $RATE_SYS_ELAPSED seconds"

echo -e "\n5. Rate of Change for Blood Pressure (Diastolic):"
RATE_DIA_START=$(date +%s.%N)
curl -s "$API_URL/timeseries/rate?metric=${PATIENT_ID}|85354-9|8462-4|mmHg&start=$START_TIME&period=14400" | jq -c '.[0:5]'
RATE_DIA_END=$(date +%s.%N)
RATE_DIA_ELAPSED=$(echo "$RATE_DIA_END - $RATE_DIA_START" | bc)
echo "Diastolic BP rate of change query took $RATE_DIA_ELAPSED seconds"

echo -e "\n6. ECG Sampled Data:"
ECG_TREND_START=$(date +%s.%N)
curl -s "$API_URL/timeseries/trend?metric=${PATIENT_ID}|11524-6|sampled&start=$START_TIME" | jq -c '.data.samples[0:5]'
ECG_TREND_END=$(date +%s.%N)
ECG_TREND_ELAPSED=$(echo "$ECG_TREND_END - $ECG_TREND_START" | bc)
echo "ECG trend query took $ECG_TREND_ELAPSED seconds"

echo -e "\n7. All Trends by Resource Type (Observation):"
ALL_TRENDS_START=$(date +%s.%N)
curl -s "$API_URL/timeseries/trend?resource_type=Observation&start=$START_TIME" | jq -c '.data[0].samples[0:5]'
ALL_TRENDS_END=$(date +%s.%N)
ALL_TRENDS_ELAPSED=$(echo "$ALL_TRENDS_END - $ALL_TRENDS_START" | bc)
echo "All trends query took $ALL_TRENDS_ELAPSED seconds"

# Calculate query time total
QUERY_TOTAL=$(echo "$TREND_HR_ELAPSED + $STATS_SPO2_ELAPSED + $OUTLIERS_ELAPSED + $RATE_SYS_ELAPSED + $RATE_DIA_ELAPSED + $ECG_TREND_ELAPSED + $ALL_TRENDS_ELAPSED" | bc)
echo -e "\nTotal query execution time: $QUERY_TOTAL seconds"

# Calculate total benchmark time
BENCHMARK_END=$(date +%s.%N)
TOTAL_TIME=$(echo "$BENCHMARK_END - $BENCHMARK_START" | bc)

echo -e "\n==== BENCHMARK SUMMARY ===="
echo "Data loading time: $DATA_LOADING_TOTAL seconds"
echo "Query execution time: $QUERY_TOTAL seconds"
echo "Total execution time: $TOTAL_TIME seconds"
echo -e "\nTest completed!" 