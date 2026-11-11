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

## C23: Vector and Fluent Bit (author decision 2026-11-11, extends C13)

Same driver, fault proxy, Kafka topics (`ssh-p1`, `hdfs-p4`), MinIO and oracle as the Confluent
campaign; one system per campaign (`--systems vector`, `--systems fluentbit`). Each sink runs as one
Docker container per trial on the `rq4net` network, reads Kafka on `rq4-kafka:29092`, and writes
through the proxy at `host.docker.internal:9100` to its own bucket (`vector`, `fluentbit`). The
per-trial configuration is generated into the trial's raw folder (`vector.yaml` /
`fluent-bit.conf`); the text below is exactly what the driver writes, with `<tag>` the trial tag.
Matrix and repetitions as in Confluent's campaign (`20261111-v070-rq4/run.json` cells): `corrupt` 10
and `lost` 10 on `ssh-p1`, `kill` 5 on `hdfs-p4` (3 SIGKILLs of the sink container at uniform
1-5 s into each life, `docker start` after each), seed 20261111, drain timeout 900 s.

### Pinned components

| Component | Version | Image id (also in each `run.json`) |
|---|---|---|
| Vector | `timberio/vector:0.61.0-debian` (released 2026-10-29; `vector 0.61.0 (aarch64-unknown-linux-gnu 17e017e)`) | `sha256:f6c23b5a7e16fb2838d33619ef7acf4bad2289a7cf22bc90efc2371f520c4ea2` |
| Fluent Bit | `fluent/fluent-bit:5.2.0` (released 2026-11-09, latest stable at run time) | `sha256:a48190bf67679495a332fd61025421a46e7e4d168380185a8e16da2e9acc3126` |

Source read for the commit semantics: Vector tag `v0.61.0` (`17e017e3c07bca3f2048f7cc4ec618a23d9289c5`),
Fluent Bit tag `v5.2.0` (`e0149076aeff009d35fd495e55379cb78f1eb53f`).

### Vector: when is the Kafka offset committed?

After the S3 PUT response. With end-to-end acknowledgements on, the kafka source stores a message's
offset only when the batch holding it is acknowledged as delivered, and librdkafka commits stored
offsets periodically:

- `src/sources/kafka.rs` (v0.61.0) L1263-1268: the consumer is created with
  `enable.auto.commit=true`, `auto.commit.interval.ms=commit_interval_ms` (default 5000) and
  `enable.auto.offset.store=false`; L643-647: `consumer.store_offset(...)` is called only when the
  ordered finalizer returns `BatchStatus::Delivered` (in offset order, per partition).
- `src/sinks/s3_common/service.rs` L113-146: one `PutObject` per batch with a `Content-MD5` of the
  body; L59-60: a successful response maps to `EventStatus::Delivered`. Nothing reads the object
  back.
- Docs: kafka source, "delivery: at-least-once, acknowledgements: yes", `commit_interval_ms`
  default 5000 ms, source-level `acknowledgements` deprecated in favour of sink/global
  (https://vector.dev/docs/reference/configuration/sources/kafka/); aws_s3 sink, at-least-once
  with acknowledgements, writes with `s3:PutObject`, `batch.timeout_secs` default 300
  (https://vector.dev/docs/reference/configuration/sinks/aws_s3/); end-to-end acknowledgements,
  the sink "captures the status of the response from the downstream service" and the source
  acknowledges upstream once notified
  (https://vector.dev/docs/architecture/end-to-end-acknowledgements/).

So the analogue of Confluent's exactly-once configuration is the documented at-least-once one:
acknowledgements enabled on the sink, default memory buffer (a disk buffer would let the source be
acknowledged at the buffer instead of at S3), no compression, newline-framed raw text.

```yaml
data_dir: /var/lib/vector
sources:
  kafka_in:
    type: kafka
    bootstrap_servers: "rq4-kafka:29092"
    group_id: "rq4-vector-<tag>"
    topics: ["<topic>"]
    auto_offset_reset: earliest
    decoding:
      codec: bytes
sinks:
  s3_out:
    type: aws_s3
    inputs: [kafka_in]
    bucket: vector
    region: us-east-1
    endpoint: "http://host.docker.internal:9100"
    force_path_style: true
    key_prefix: "<tag>/"
    filename_time_format: "%s"
    filename_append_uuid: true
    filename_extension: log
    compression: none
    encoding:
      codec: text
    framing:
      method: newline_delimited
    batch:
      max_events: 40000        # same record count per object as Confluent's flush.size
      max_bytes: 100000000     # not binding
      timeout_secs: 5          # flushes the tail, so the drain target is the whole topic
    acknowledgements:
      enabled: true
```

Credentials come from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` in the container environment.

### Fluent Bit: when is the Kafka offset committed?

When the records enter Fluent Bit's input buffer, before any output runs. Fluent Bit has no
end-to-end acknowledgement from an output back to an input.

- `plugins/in_kafka/in_kafka.c` (v5.2.0) L213-219: after each poll cycle the records are appended
  to the input's chunk (`flb_input_log_append`), and if that succeeds and `enable_auto_commit` is
  off (the default) the plugin calls `rd_kafka_commit(ctx->kafka.rk, NULL, 0)`, a synchronous
  commit of the consumed position. With `enable_auto_commit` on, librdkafka commits in the
  background instead; neither path waits for an output.
- `plugins/out_s3/s3.c` (v5.2.0) `s3_flush_logs` (L4981-5129): the engine's flush of a chunk to the
  s3 output copies it into the plugin's own `store_dir` buffer and returns `FLB_OK`; the
  `PutObject` happens later, when the buffered file reaches the upload size or `upload_timeout`
  (timer at least every 6 s). `send_content_md5` adds a `Content-MD5` header; nothing reads the
  object back.
- Docs: kafka input, `enable_auto_commit` default `false`, no delivery guarantee stated
  (https://docs.fluentbit.io/manual/pipeline/inputs/kafka); s3 output, `use_put_object`,
  `send_content_md5`, `store_dir`, buffered files sent on restart, `log_key`, `retry_limit`
  default 5, "S3 manages its own buffering and retries internally"
  (https://docs.fluentbit.io/manual/pipeline/outputs/s3); filesystem buffering "less prone to data
  loss", no formal guarantee (https://docs.fluentbit.io/manual/administration/buffering-and-storage).

Most reliable documented configuration used: filesystem storage for the input
(`storage.type filesystem`, `storage.sync full`, `storage.checksum on`), the default synchronous
commit (`enable_auto_commit` off), `PutObject` with `Content-MD5`, the plugin's persistent
`store_dir` (inside the container file system, which survives `docker kill` + `docker start`),
default `retry_limit`. `log_key payload` writes the raw Kafka value of each record followed by `\n`,
so objects decode to the topic's records byte for byte. Records of all partitions share one tag and
are interleaved in one object.

```ini
[SERVICE]
    flush           1
    log_level       info
    storage.path    /tmp/flb-storage/
    storage.sync    full
    storage.checksum on

[INPUT]
    name            kafka
    tag             rq4
    brokers         rq4-kafka:29092
    topics          <topic>
    group_id        rq4-fluentbit-<tag>
    format          none
    storage.type    filesystem
    rdkafka.auto.offset.reset earliest

[OUTPUT]
    name            s3
    match           *
    bucket          fluentbit
    region          us-east-1
    endpoint        http://host.docker.internal:9100
    use_put_object  On
    send_content_md5 On
    total_file_size 4M
    upload_timeout  10s
    store_dir       /tmp/fluent-bit/s3
    s3_key_format   /<tag>/%Y%m%dT%H%M%S-$UUID.log
    log_key         payload
```

### Oracle changes for these sinks

- **Locating records.** Vector and Fluent Bit objects carry no offsets (Confluent's key holds the
  start offset; VTOP's manifest holds the range). `locate()` in the driver finds every line of an
  object in the topic by content: a line extends the open run of its partition if it equals that
  run's next record, otherwise it opens a run at the offset where the topic holds it. All 655,147
  `ssh-p1` lines are distinct; 1,521 of the 2,000,000 `hdfs-p4` lines are in duplicate groups, so a
  duplicate line is resolved by the next line, and the count of such lines is recorded per trial
  (`ambiguous_lines`). An object with any line not in the topic is not intact, as a whole (one
  not-intact interval per partition it holds). The judge (`judge()`) is unchanged.
- **Object counts.** `objects` / `bad_objects_final` / `duplicate_objects` count intervals, i.e.
  (object, partition) runs, as the judge sees them; `s3_objects` / `s3_objects_bad` count S3
  objects. For `ssh-p1` (one partition) they coincide unless an object holds a replayed gap.
- **Drain target and end of a run.** Both sinks flush by time (Vector `batch.timeout_secs` 5,
  Fluent Bit `upload_timeout` 10 s), so the drain target is the whole topic (Confluent's was the
  last full 40,000-record file). Because a committed offset at the target does not show that the
  archive is written (Fluent Bit commits on ingest), a run (and each kill loop) ends when the
  committed offset is at the target **and** the archive holds at least the topic's bytes, or, if
  that never happens (e.g. a lost object), when the object listing has not changed for 30 s after
  the commit reached the target; the reason is in `drain_end`. The oracle is also run once when the
  commit is first seen at the target (`at_target`); it reads the archive after reading the commit,
  so it can only under-state a gap between commit and archive.
- Outcome fields are the same as Confluent's (`unsafe_at_kill`, `unsafe_final`,
  `lost_committed_records`, `complete`, `missing_records`, `duplicate_*`), plus `at_target`,
  `drain_end`, `s3_objects*`, `unmatched_lines`, `ambiguous_lines` and per-object `object_stats`.
