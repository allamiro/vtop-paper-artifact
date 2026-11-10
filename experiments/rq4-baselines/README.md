# RQ4: Comparison with existing telemetry-to-object-storage systems

Hypotheses: H4.1–H4.3 ([../../hypotheses.md](../../hypotheses.md)).

## Systems (pin exact versions in `configs/` before running)

| ID | System | Configuration | Sources it can take |
|---|---|---|---|
| S-VTOP | VTOP (chosen snapshot) | strong verification, defaults | Kafka, file, spool |
| C1 | Confluent S3 Sink connector | exactly-once-capable settings per vendor docs (deterministic partitioner, scheduled rotation off) and a default config | Kafka |
| C2 | Vector | `aws_s3` sink, end-to-end acknowledgements on and off, disk buffer | Kafka, file |
| C3 | Fluent Bit | `s3` output with filesystem storage | file, (Kafka input if available) |
| C4 | Apache Flink | Kafka source + FileSink to S3, checkpointing on | Kafka, file |

Each row's configuration text and the doc version it follows go in
`configs/<id>/`. A system that cannot run a cell is reported as "not
applicable", not reconfigured into something else.

## Shared conditions

Same host type, CPU/memory limits, same MinIO, same workload generator
and seeds, same target object size (as close as each system allows),
same compression.

## E-RQ4-B

1. **No-fault performance:** throughput, end-to-end latency (record
   produced → object visible and offset committed), CPU, memory.
2. **Fault matrix:** F1–F5, F7 from RQ1 mapped to each system's
   pipeline stages (process kill at equivalent points), plus mutations
   M1–M4 from RQ2 after upload.
3. **Outcomes per cell:** lost records, duplicate records/objects,
   corrupted object accepted (offset committed over a mutated object),
   recovery time.

## Output

- Table: system × fault point → {lost, dup, corrupt-accepted, recovery s}
- Figure: throughput vs. commit latency per system
- Capability matrix (H4.3) from versioned documentation, citing the
  version.

## As run (2026-11-10)

Not run yet.
