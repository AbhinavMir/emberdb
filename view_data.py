#!/usr/bin/env python3

import json
import sys
import os
import struct
from datetime import datetime

def format_timestamp(unix_ts):
    return datetime.fromtimestamp(unix_ts).strftime('%Y-%m-%d %H:%M:%S')

def view_chunk(file_path):
    print(f"\n===== CHUNK FILE: {file_path} =====")
    
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    print(f"Time range: {format_timestamp(data['start_time'])} to {format_timestamp(data['end_time'])}")
    print(f"Compression: {data['compression_state']}")
    print(f"Metadata:")
    print(f"  - Created: {format_timestamp(data['metadata']['created_at'])}")
    print(f"  - Last Access: {format_timestamp(data['metadata']['last_access'])}")
    print(f"  - Compression Ratio: {data['metadata']['compression_ratio']}")
    print(f"  - Record Count: {data['metadata']['record_count']}")
    
    print("\nRecords:")
    for metric, records in data['records'].items():
        print(f"\nMetric: {metric}")
        for i, record in enumerate(records, 1):
            print(f"  {i}. Time: {format_timestamp(record['timestamp'])}, Value: {record['value']}")

def view_wal(file_path):
    print(f"\n===== WAL FILE: {file_path} =====")
    
    records = []
    with open(file_path, 'rb') as f:
        while True:
            try:
                # Read 4-byte size header
                size_bytes = f.read(4)
                if not size_bytes or len(size_bytes) < 4:
                    break
                
                record_size = struct.unpack('>I', size_bytes)[0]
                
                # Read record data
                record_data = f.read(record_size)
                if not record_data or len(record_data) < record_size:
                    break
                
                record = json.loads(record_data)
                records.append(record)
            except Exception as e:
                print(f"Error reading WAL: {e}")
                break
    
    print(f"Found {len(records)} records in WAL")
    
    for i, record in enumerate(records, 1):
        print(f"\nRecord {i}:")
        print(f"  Metric: {record['metric_name']}")
        print(f"  Time: {format_timestamp(record['timestamp'])}")
        print(f"  Value: {record['value']}")

def main():
    data_dir = './data'
    
    # View chunks
    chunks_dir = os.path.join(data_dir, 'chunks')
    chunk_files = [f for f in os.listdir(chunks_dir) if f.endswith('.chunk')]
    
    if chunk_files:
        for chunk_file in sorted(chunk_files):
            view_chunk(os.path.join(chunks_dir, chunk_file))
    else:
        print("No chunk files found")
    
    # View WAL
    wal_path = os.path.join(data_dir, 'wal', 'records.wal')
    if os.path.exists(wal_path):
        view_wal(wal_path)
    else:
        print("No WAL file found")

if __name__ == "__main__":
    main() 