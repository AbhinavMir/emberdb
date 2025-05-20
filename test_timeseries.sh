#!/bin/bash

# Script to add test observations and exercise time-series endpoints

set -e
API_URL="http://localhost:5432"
PATIENT_ID="123"
CURRENT_TIME=$(date +%s)
START_TIME=$((CURRENT_TIME - 24*3600))  # 24 hours ago

echo "==== Adding Heart Rate Observations with Trend ===="
# Add heart rate observations with a clear upward trend
for i in {0..23}; do
  timestamp=$((START_TIME + i*3600))  # One per hour
  iso_time=$(date -r $timestamp -u +"%Y-%m-%dT%H:%M:%SZ")
  
  # Generate heart rate with upward trend (70 to 90) plus some noise
  base_hr=$((70 + i*20/23))
  noise=$((RANDOM % 5 - 2))
  hr=$((base_hr + noise))
  
  echo "Adding heart rate $hr at $iso_time"
  curl -s -X POST "$API_URL/fhir/Observation" \
    -H "Content-Type: application/json" \
    -d '{
      "resourceType": "Observation",
      "status": "final",
      "code": {
        "coding": [
          {
            "system": "http://loinc.org",
            "code": "8867-4",
            "display": "Heart rate"
          }
        ]
      },
      "subject": {
        "reference": "Patient/'$PATIENT_ID'"
      },
      "effectiveDateTime": "'$iso_time'",
      "valueQuantity": {
        "value": '$hr',
        "unit": "beats/minute",
        "system": "http://unitsofmeasure.org",
        "code": "/min"
      }
    }' > /dev/null
done

echo "==== Adding Blood Pressure Observations with Fluctuations ===="
# Add blood pressure observations with some fluctuations
for i in {0..11}; do
  timestamp=$((START_TIME + i*7200))  # Every 2 hours
  iso_time=$(date -r $timestamp -u +"%Y-%m-%dT%H:%M:%SZ")
  
  # Systolic with slight trend up and noise
  base_systolic=$((120 + i*10/11))
  noise_s=$((RANDOM % 8 - 4))
  systolic=$((base_systolic + noise_s))
  
  # Diastolic with less change
  base_diastolic=$((80 + i*4/11))
  noise_d=$((RANDOM % 6 - 3))
  diastolic=$((base_diastolic + noise_d))
  
  echo "Adding BP $systolic/$diastolic at $iso_time"
  curl -s -X POST "$API_URL/fhir/Observation" \
    -H "Content-Type: application/json" \
    -d '{
      "resourceType": "Observation",
      "status": "final",
      "code": {
        "coding": [
          {
            "system": "http://loinc.org",
            "code": "85354-9",
            "display": "Blood pressure panel"
          }
        ]
      },
      "subject": {
        "reference": "Patient/'$PATIENT_ID'"
      },
      "effectiveDateTime": "'$iso_time'",
      "component": [
        {
          "code": {
            "coding": [
              {
                "system": "http://loinc.org",
                "code": "8480-6",
                "display": "Systolic blood pressure"
              }
            ]
          },
          "valueQuantity": {
            "value": '$systolic',
            "unit": "mmHg",
            "system": "http://unitsofmeasure.org",
            "code": "mm[Hg]"
          }
        },
        {
          "code": {
            "coding": [
              {
                "system": "http://loinc.org",
                "code": "8462-4",
                "display": "Diastolic blood pressure"
              }
            ]
          },
          "valueQuantity": {
            "value": '$diastolic',
            "unit": "mmHg",
            "system": "http://unitsofmeasure.org",
            "code": "mm[Hg]"
          }
        }
      ]
    }' > /dev/null
done

echo "==== Adding Oxygen Saturation with Outliers ===="
# Add oxygen saturation with outliers
for i in {0..19}; do
  timestamp=$((START_TIME + i*4320))  # Every 72 minutes for better spread
  iso_time=$(date -r $timestamp -u +"%Y-%m-%dT%H:%M:%SZ")
  
  # Normal oxygen saturation is 95-100%, add outliers at specific points
  if [ $i -eq 5 ]; then
    # Outlier low
    spo2=88
  elif [ $i -eq 15 ]; then
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
  
  echo "Adding SpO2 $spo2% at $iso_time"
  curl -s -X POST "$API_URL/fhir/Observation" \
    -H "Content-Type: application/json" \
    -d '{
      "resourceType": "Observation",
      "status": "final",
      "code": {
        "coding": [
          {
            "system": "http://loinc.org",
            "code": "59408-5",
            "display": "Oxygen saturation in Arterial blood by Pulse oximetry"
          }
        ]
      },
      "subject": {
        "reference": "Patient/'$PATIENT_ID'"
      },
      "effectiveDateTime": "'$iso_time'",
      "valueQuantity": {
        "value": '$spo2',
        "unit": "%",
        "system": "http://unitsofmeasure.org",
        "code": "%"
      }
    }' > /dev/null
done

echo "==== Adding ECG Sampled Data ===="
# Add sampled ECG data - create data outside of curl call to avoid issues with negative values
timestamp=$START_TIME
iso_time=$(date -r $timestamp -u +"%Y-%m-%dT%H:%M:%SZ")

# Create a synthetic ECG waveform with the data properly formatted
ecg_data=""
for i in {0..99}; do
  # A simplified ECG-like pattern
  if [ $((i % 25)) -eq 0 ]; then
    # R peak
    val="1.5"
  elif [ $((i % 25)) -eq 1 ]; then
    # S wave
    val="-0.5"
  elif [ $((i % 25)) -eq 5 ]; then
    # T wave
    val="0.75"
  else
    # Baseline with noise
    noise=$(awk -v seed=$RANDOM 'BEGIN {srand(seed); print rand()*0.1-0.05}')
    val=$noise
  fi
  
  ecg_data="${ecg_data} ${val}"
done

echo "Adding ECG sampled data at $iso_time"
# Send the ECG data properly
curl -s -X POST "$API_URL/fhir/Observation" \
  -H "Content-Type: application/json" \
  -d @- <<EOF
{
  "resourceType": "Observation",
  "status": "final", 
  "code": {
    "coding": [
      {
        "system": "http://loinc.org",
        "code": "11524-6",
        "display": "EKG study"
      }
    ]
  },
  "subject": {
    "reference": "Patient/${PATIENT_ID}"
  },
  "effectiveDateTime": "${iso_time}",
  "valueSampledData": {
    "origin": {
      "value": 0,
      "unit": "mV",
      "system": "http://unitsofmeasure.org",
      "code": "mV"
    },
    "period": 10,
    "factor": 1.0,
    "dimensions": 1,
    "data": "${ecg_data}"
  }
}
EOF

sleep 2  # Give the server time to process records

echo -e "\n==== Testing Time-Series Endpoints ===="

echo -e "\n1. Trend Analysis for Heart Rate:"
curl -s "$API_URL/timeseries/trend?metric=${PATIENT_ID}|8867-4|beats/minute&start=$START_TIME" | jq

echo -e "\n2. Statistics for Oxygen Saturation:"
curl -s "$API_URL/timeseries/stats?metric=${PATIENT_ID}|59408-5|%&start=$START_TIME" | jq

echo -e "\n3. Outlier Detection for Oxygen Saturation (Z-score threshold 1.5):"
curl -s "$API_URL/timeseries/outliers?metric=${PATIENT_ID}|59408-5|%&start=$START_TIME&threshold=1.5" | jq

echo -e "\n4. Rate of Change for Blood Pressure (Systolic):"
curl -s "$API_URL/timeseries/rate?metric=${PATIENT_ID}|85354-9|8480-6|mmHg&start=$START_TIME&period=14400" | jq

echo -e "\n5. Rate of Change for Blood Pressure (Diastolic):"
curl -s "$API_URL/timeseries/rate?metric=${PATIENT_ID}|85354-9|8462-4|mmHg&start=$START_TIME&period=14400" | jq

echo -e "\n6. ECG Sampled Data:"
curl -s "$API_URL/timeseries/trend?metric=${PATIENT_ID}|11524-6|sampled&start=$START_TIME" | jq

echo -e "\n7. All Trends by Resource Type (Observation):"
curl -s "$API_URL/timeseries/trend?resource_type=Observation&start=$START_TIME" | jq

echo -e "\nTest completed!" 