# E-RQ4-B configuration (C13: one baseline, author decision D2, 2026-11-11)

Driver: `../code/exp_baseline.py`; fault proxy: `../code/s3_fault_proxy.py`.

## Pinned components

| Component | Version | How it is pinned |
|---|---|---|
| Kafka | `apache/kafka:3.9.1` (KRaft, one node) | image id in `run.json` |
| Object store | MinIO RELEASE.2025-09-07T16-13-09Z, commit `07c3a429` | built from source by vtop-engine `docker/minio/Dockerfile` (MinIO no longer publishes images); image id in `run.json` |
| Baseline | Confluent Platform `cp-kafka-connect:7.9.0`, distributed worker | image id in `run.json` |
| Baseline plugin | Confluent `kafka-connect-s3` 12.1.15 (Confluent Hub, released 2026-10-26, Confluent Community License) | zip SHA-1 `bcc46c39bfc1cd3feff8b0ff48c9b58d2c2e2241` checked against the Hub record; SHA-256 `7ba0ea1fbf32251b99a39f47bfcc35a0a49abdeb0a427fbf4866e14df0a32dc7` |
| System under test | VTOP v0.7.0 (`6c8f386`), `s3_native`, BLAKE3, `require_strong_verification: true`, 4 MiB batches | `vtop_commit` in `run.json` |

The plugin zip and the extracted plugin live in `.tmp/rq4/` (git-ignored); they are fetched from the
Hub URL in the table and checked as above.

## Connector configuration (per trial; written to each trial's `connector.json`)

The configuration Confluent documents for exactly-once delivery to S3: the deterministic
`DefaultPartitioner`, rotation by record count only (`flush.size` = 40,000, no time-based rotation),
`ByteArrayFormat` with `\n` separators so objects decode to the topic's records byte for byte.
Offsets are flushed every second (`offset.flush.interval.ms` = 1000). Because no time-based
rotation is configured, the records after the last full 40,000-record file are never written; the
drain target for the baseline is therefore the last full file per partition, and that tail is not
counted as loss.

## Faults

| Fault | Injection | Workload |
|---|---|---|
| `corrupt` | the proxy relays MinIO's 200 for the N-th data-object write (N uniform in 2..8) after flipping one byte of the stored object | `ssh-p1` (655,147 records, 1 partition) |
| `lost` | the same, but the stored object is deleted | `ssh-p1` |
| `kill` | SIGKILL of the sink (VTOP process / Connect worker container) 3 times at uniform 1–5 s into each life, then a drain | `hdfs-p4` (2,000,000 HDFS records, 4 partitions) |

Manifests are never faulted. The proxy alters the object after MinIO accepted it and before the
client sees the acknowledgement, so request signatures are unaffected; the client receives a
normal 200.

## Oracle

Independent of both systems: objects are listed and read directly from MinIO (not through the
proxy), decoded, and compared record by record with the topic's ground truth (console-consumer
dump). Per partition it computes the contiguous intact prefix and the committed offset of the
consumer group on the broker. Outcomes per trial:

- `unsafe_final`: partitions whose committed offset exceeds the intact prefix;
- `lost_committed_records`: committed records with no intact copy anywhere in the archive;
- `complete`: the intact prefix reaches the drain target;
- `duplicate_objects` / `duplicate_records`: overlapping intact coverage.
