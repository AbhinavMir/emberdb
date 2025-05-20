from(bucket: "monitoring")
  |> range(start: 1747687000)
  |> filter(fn: (r) => r._measurement == "heart_rate" and r.patient_id == "123")
  |> yield(name: "trend")
