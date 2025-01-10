# Understanding EmberDB's Storage System: A Beginner's Guide

Let me explain EmberDB's storage system using simple analogies and plain English.

## The Big Picture

Imagine you're organizing a massive library of medical measurements. Each measurement (like heart rate, blood pressure) is like a book entry with three pieces of information:
- When it was recorded (timestamp)
- What was measured (metric_name)
- The actual measurement (value)

## Core Components

### 1. Record (The Individual Entry)
````rust:src/storage/mod.rs
pub struct Record {
    pub timestamp: i64,      // When it was recorded
    pub metric_name: String, // What was measured
    pub value: f64,         // The actual measurement
}
````
Think of a Record like a single diary entry: "At 2:30 PM (timestamp), my heart rate (metric_name) was 72 beats per minute (value)."

### 2. TimeChunk (The Filing Cabinet)

A TimeChunk is like a filing cabinet that only stores records from a specific time period. For example, one cabinet might contain all measurements from 9:00 AM to 10:00 AM.

#### Key Properties:
````rust:src/storage/chunk.rs
pub struct TimeChunk {
    start_time: i64,                           // When this chunk starts
    end_time: i64,                            // When this chunk ends
    records: HashMap<String, Vec<Record>>,    // The actual data
    metadata: ChunkMetadata,                  // Information about the chunk
    compression_state: CompressionState,      // Is it compressed?
}
````

#### Important Methods:

1. **Creating a New Cabinet** (`new`)
```rust
TimeChunk::new(start_time, end_time)
```
Like setting up a new empty filing cabinet with labels for its time period.

2. **Adding Records** (`append`)
```rust
chunk.append(record)
```
Like filing a new piece of paper in the cabinet. It checks if the record belongs in this time period first.

3. **Finding Records** (`get_range`, `get_metric`, `get_latest`)
- `get_range`: Find all records between two times (like "show me all heart rates between 9:15 and 9:45")
- `get_metric`: Get all records for one type of measurement (like "show me all heart rates")
- `get_latest`: Get the most recent measurement (like "what's the latest blood pressure?")

4. **Summarizing Data** (`summarize`)
```rust
chunk.summarize("heart_rate")
```
Like getting a quick report: "For heart rate: lowest was 60, highest was 80, average was 72, and we took 100 measurements."

5. **Organization Methods**
- `compress`: Like using a vacuum bag to make the files take up less space
- `validate`: Making sure all the files are in the right cabinet
- `merge`: Combining two cabinets into one
- `cleanup`: Getting rid of unnecessary papers

### 3. ChunkMetadata (The Cabinet Label)
````rust
pub struct ChunkMetadata {
    created_at: i64,         // When we created this cabinet
    last_access: i64,        // When someone last looked at it
    compression_ratio: f64,  // How well we compressed it
    record_count: usize,     // How many records it contains
    size_bytes: usize,       // How much space it takes up
}
````
Like a label on the cabinet showing when it was set up, last used, and how full it is.

### 4. Error Handling
````rust
pub enum ChunkError {
    OutOfTimeRange(&'static str),    // Wrong time period
    CompressionFailed(&'static str), // Couldn't compress
    DiskWriteFailed(&'static str),   // Couldn't save to disk
    ValidationFailed(&'static str),  // Something's wrong with the data
    DataCorrupted(&'static str),     // Data is damaged
    IndexError(&'static str),        // Can't find what you're looking for
}
````
These are like different types of problems that might occur, each with its own explanation.

## Real-World Analogy

Imagine you're a nurse monitoring patients in a hospital:
- Each `Record` is like a single vital sign reading
- Each `TimeChunk` is like an hour's worth of readings for all patients
- The `metadata` is like the summary sheet at the nurse's station
- `compression` is like archiving old records to save space
- `get_range` is like looking up a patient's readings during your shift
- `summarize` is like preparing a report for the doctor

## Technical Details for Non-DB Folks

1. **Organization**: Data is grouped by time periods for easy access
2. **Efficiency**: Similar to having filing cabinets for different time periods instead of one huge pile of papers
3. **Safety**: Built-in checks ensure data is stored correctly and can be found reliably
4. **Space Management**: Old data can be compressed to save space, like archiving old medical records

Would you like me to elaborate on any particular aspect?
