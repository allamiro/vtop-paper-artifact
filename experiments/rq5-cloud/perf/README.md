# E-RQ5-PERF: VTOP vs Confluent S3 Sink, Vector and Fluent Bit on EC2 + Amazon S3 (H4.2)

**Status: prepared, not launched.** No AWS call that creates, launches, uploads or spends was made for this stage.
Read-only calls only: describe security groups, instance-type offerings, AMIs and subnets, the T4g credit default,
and one `RunInstances --dry-run` per region (it confirmed the launch is allowed, see [Unverified](#not-verified)).
The local dry run is [below](#local-dry-run-2026-11-21). **Launch needs the author's "go"** (skill step 1, D5). It
also needs answers to the [decisions](#decisions-for-the-author).

**Why.** The earlier evaluation had no quantitative comparison with prior
similar work. RQ4 (H4.1) compared safety with the three fault-matched sinks. This stage compares performance
and cost with the **same** three systems (same images, plugin and configurations as RQ4) writing to Amazon S3.
Two legs:

- **us-east-1**, next to the bucket;
- **ap-southeast-2 (Sydney)**, writing to the us-east-1 bucket (author scope addition, 2026-11-21).

The Sydney leg shows how each tool, not only VTOP, copes with distance.

**Hypothesis H4.2** (`research/hypotheses.md`): VTOP's verification gating costs throughput or latency relative
to the response-based baselines on equal hardware and batch sizes. The prediction is that VTOP is slower than at
least one baseline in steady state, and that the cost is the price of H2.x detection.

**Storage-fault phase (author decision D13, us-east-1 only).** It repeats RQ4's acknowledged-but-corrupted and
acknowledged-but-lost trials on the same hosts and systems against real S3, without a proxy. This is the cloud
counterpart of H4.1. See [Storage-fault phase](#storage-fault-phase-d13-cloud-counterpart-of-rq4).

The design separates the price of verification from the price of the architecture. VTOP runs in three modes:

- size-only (no content check);
- store SHA-256 (S3 computes it; one HEAD per batch);
- BLAKE3 read-back (one GET per batch).

## Design

| Factor | Value | Why |
|---|---|---|
| Systems | VTOP **v0.10.2** (0abf116, gnu aarch64 archive 5872e5a3…df39); Confluent **cp-kafka-connect 7.9.0 + kafka-connect-s3 12.1.15**; **Vector 0.61.0**; **Fluent Bit 5.2.0** | as RQ4. The images are pinned by the RQ4 `run.json` index digests (`perf.py` `IMAGES`), the plugin zip by SHA-256 `7ba0ea1f…2dc7`. **The tag `fluent/fluent-bit:5.2.0` now resolves to a different index** (`e2718c47…`, checked 2026-11-21), so we pin RQ4's `a48190bf…` (still pullable) |
| Source | Kafka 3.9.1 (`apache/kafka` RQ4 digest), KRaft, one node on its own host. Topics are compressed with **lz4** by the producer | All four systems consume Kafka. lz4 is decoded by all four clients (librdkafka for VTOP, Vector and Fluent Bit; the Java client for Connect). It keeps the sink's inbound traffic about 4× below the uncompressed stream and far below the t4g.large network baseline. Without it, the Kafka fetch, not the sink, could hit the instance allowance |
| Throughput topic | `hdfs-p4`: the first **11,040,000** HDFS lines (Loghub, 1.43 GiB of payload). Line *i* goes to partition *i* mod 4, so 2,760,000 records per partition = exactly 23 × 120,000 | HDFS is the larger RQ4 dataset. **4 partitions as RQ4's `hdfs-p4`**, so per-partition sinks (VTOP, Confluent) work in parallel. 23 objects per partition give a 10–90% steady window of ≥ 18 objects per partition. The exact multiple of `flush.size` means Confluent (no time rotation, RQ4) has **no unwritten tail**, so its drain target is the whole topic like everyone else's. Every record is consumed back once after loading and compared byte for byte with the file |
| Live topic | a fresh 1-partition topic per run; the first **1,800,000** HDFS lines produced at **30,000 records/s (≈ 4.0 MiB/s) for 60 s** by a separate producer process | One partition removes the per-partition vs global batching difference from the latency figure. At 4 MiB/s a 16 MiB object fills in ≈ 4 s, before the common 10 s age, so objects close **on size for all four systems**. The fill wait is the same for everyone, and latency differences come from the pipeline (upload, verification, commit cadence) |
| SSH | **not used** | SSH is 70 MiB: fewer than 5 objects of 16 MiB in total, so no steady state at D6's object size. Author decision 1 |
| Hosts | **2 × t4g.large** (2 Graviton2 cores, 8 GiB) in **one subnet**, one leg at a time: broker host (Kafka, producer, observer, oracle) and sink host (one system at a time) | equal hardware for every system: same instance, sequential runs. The Connect JVM needs memory, and t4g.medium's 4 GiB is tight for a 2 GiB worker heap plus the OS. One subnet keeps private traffic free and gives both hosts the same path |
| Arch | **arm64 everywhere** | Every image has a `linux/arm64` manifest (checked: kafka, cp-kafka-connect, vector, fluent-bit). The pinned VTOP archive is aarch64, as in COST and WAN. t4g.large has 2 physical cores at USD 0.0672/h; t3.large has 1 core × 2 threads at 0.0832/h (Price List, 2026-11-21) |
| Root volume | 30 GiB gp3 (launch.sh `ROOT_GB`) | The cp-kafka-connect arm64 image is ≈ 3.7 GB unpacked |
| Order | 4 warm-up runs (one per system, excluded); then rounds: round *r* holds every (type, config) cell that has a rep *r*, shuffled (`random.Random(SEED=20261123)`); order in `run.json` | blocks out time drift; each round has every system |
| Reps | us-east-1: tput 5, live 3, kill 3. Sydney: tput 3, live 2, kill 2, no gzip variant | see [Reps](#reps-and-the-sydney-leg) |

### Run types and metrics

| Type | Configs | What is measured |
|---|---|---|
| `tput` (backlog drain) | all 7 | **Steady "done" rate (primary)**: payload bytes both archived intact and source-committed, between the first observation at ≥ 10% and at ≥ 90% of the topic, divided by the time between them. The **committed rate** and the **archived rate** (intact records visible in S3) are also given; Vector's 5 s commit cadence makes its committed curve coarse. Records/s. **CPU seconds** of the sink in that window (cgroup v2 `cpu.stat`, sampled every 0.5 s; for VTOP also `wait4` rusage). CPU s/GiB and cores. **Peak memory**: cgroup `memory.stat anon` sampled, plus `memory.peak` and VTOP's `ru_maxrss`. Objects, raw MiB and records per object, compression ratio, multipart parts. **Bytes to/from S3**: nft counters on non-private addresses; the sink host talks to nothing else public during runs, and the SSM agent is stopped. Sink NIC receive rate, disk writes, ENA allowance counters on both hosts. End-to-end rate |
| `live` | 6 (no gzip variant) | per record: **end-to-end latency = max(first LIST that shows an intact object holding it, first committed-offset poll beyond it) − its produce() call time**, all on the broker clock (the producer and the observer run there). Components are reported separately (object-visible, commit); Fluent Bit commits before the object exists. Percentiles p1–p100 and a 0.1 s histogram per run. Resolution: LIST every 0.5 s, commit poll 0.25 s |
| `kill` | VTOP store SHA-256, Confluent, Vector, Fluent Bit | tput with **one SIGKILL** when 40% of the records are committed or archived, then an immediate restart (VTOP: same command; containers: `docker kill -s KILL` + `docker start`, as RQ4). Measured from the kill: process back, ready (Connect: REST + task RUNNING), **first commit beyond the pre-kill commit**, first new object, and **re-steady**, the first time *t* after the restart at which the committed rate over (t−W, t] ≥ 0.9 × the pre-kill rate (W = max(10 s, 3 × the median inter-commit gap)). Duplicates and completeness come from the oracle |

**Correctness on every run.** Run by the broker after the sink stops. RQ4's `locate()` and `judge()` are imported
**unchanged** from `rq4-baselines/code/exp_baseline.py`:

- `locate()` finds every line of every object in the topic by content (no system metadata is used);
- `judge()` computes, per partition, the intact prefix, committed vs intact (**unsafe**), **lost committed
  records**, completeness and **duplicates**.

The only adaptation is the data source. The ground truth is a memory-lean view of the HDFS file (mmap plus a line
offset array). The content index is a sorted 64-bit hash array, and every hit is confirmed byte for byte. It is
installed in RQ4's `_INDEX` cache, so `locate()` runs as written. The dry run cross-checks it.

**A run passes** when all of the following hold:

- the system started;
- the drain ended with the commit at the target and the archive holding the topic;
- the oracle says complete, 0 unsafe and 0 lost committed;
- for VTOP, the log's `verification_passed` object_check equals the mode's text, and the ledger class is as
  expected;
- for kill runs, the kill happened before the drain.

`analyze.py` refuses any unsafe or lost-committed run outright.

**S3 requests.** Amazon S3 cannot count requests per client without new permissions (see Decision 6). So the
numbers are built from two parts:

1. **measured per object** in the dry run, where every request of every system went through
   `reqcount_proxy.py` (RQ4's `s3_fault_proxy.py`, imported unchanged, in pass-through mode) to MinIO;
2. **multiplied** by the cloud object counts (LIST, and ETag part counts).

This gives requests/GiB and USD/GiB, marked as estimates.

**Cost per GiB** (analyze.py) has three parts:

- request cost (estimate, above);
- the sink instance's on-demand price ÷ the measured committed GiB/h;
- for Sydney, the measured public bytes × the inter-region price.

### Fairness

The configurations are recorded verbatim per run: `configs/<run>.json` in the broker results, holding the
rendered VTOP YAML, the Connect worker environment and connector JSON, `vector.yaml` and `fluent-bit.conf`.
Changes from the RQ4 configurations (`../../rq4-baselines/configs/README.md`):

| Item | VTOP | Confluent | Vector | Fluent Bit |
|---|---|---|---|---|
| Durable/acknowledged mode (as RQ4) | commit after verification (three modes) | `DefaultPartitioner`, rotation by record count only, `ByteArrayFormat` + `\n` (exactly-once config) | `acknowledgements.enabled: true`, memory buffer | filesystem input storage, `storage.sync full`, `storage.checksum on`, PutObject + `send_content_md5` |
| Object target 16 MiB (D6) | `max_bytes` 16 MiB | `flush.size` **120,000** records (= 16.0 MiB at the topic's mean 139.5 B/line) | `max_events` 120,000, `max_bytes` 1e9 (not binding: Vector counts in-memory event size, not raw bytes) | `total_file_size` **16777216** (written in bytes: Fluent Bit reads `16M` as 16,000,000) |
| Compression | zstd 3 (plus a **gzip 6 variant** in store-SHA-256 mode, codec-matched to Confluent) | **gzip** (`s3.compression.type`; only `none`/`gzip` exist for ByteArrayFormat), level −1 = zlib 6 | zstd 3 | zstd, **level 1, fixed in the source** (`flb_zstd.c` calls `ZSTD_compress(..., 1)`) |
| Age / time flush | `max_batch_age_seconds` 10 | none (RQ4; time rotation would change the EOS configuration) | `timeout_secs` 10 (RQ4: 5) | `upload_timeout 10s` (RQ4: 10 s) |
| Parallelism | `max_concurrent_batches` 8 (default, as COST) | **`tasks.max` 4** = partitions (RQ4: 1); author decision 3 | default | default |
| Other | receipts off (engine default; COST had them on); multipart threshold 64 MiB → every object a single PUT | `offset.flush.interval.ms` 1000 (RQ4), fresh worker JVM, group and internal topics per run | `commit_interval_ms` default 5,000 | `enable_auto_commit` off (sync commit on ingest, RQ4) |
| Endpoint/credentials | us-east-1, instance role via IMDSv2 | same | same | same |

**What still differs, by design:**

- Fluent Bit commits the offset when records enter its buffer, not when they are in S3 (RQ4 source reading).
  Its "commit" latency is therefore not a durability point, so the latency uses max(object, commit).
- Vector commits every 5 s, and Confluent's worker every 1 s, so the commit cadence is part of each system's
  latency.
- Batches are per partition for VTOP and Confluent and global for Vector and Fluent Bit. On the 4-partition
  topic the per-object record count is the same, but objects mix partitions for the latter two.
- The containers run with `--network host`, and VTOP runs as a native process in its own cgroup. Neither has CPU
  or memory limits. All four get the same 2 cores and 8 GiB.

**Equal network limits.** Both hosts are t4g.large: 0.512 Gbps baseline and 5 Gbps burst (AWS instance-type
page). The design keeps every system under the baseline. The fetch is lz4. Uploads are compressed (about 10×
on HDFS for zstd-3 and gzip-6, measured on 6 × 16 MiB samples), so at 100 MiB/s of payload the sink sends about
10 MB/s to S3. Read-back doubles the S3 bytes for one mode. Runs are interleaved, so any credit depletion hits
all systems, and every run records the ENA `bw_*`/`pps_allowance_exceeded` deltas on **both** hosts.
`analyze.py` reports them per config. E-RQ5-COST found allowance events on t4g.medium at ≈ 45 MiB/s
(LOG.md, H3.5). Here the instance has twice the baseline, and only compressed bytes leave the host.

**CPU credits.** T4g launches unlimited in both regions (`GetDefaultCreditSpecification`, checked 2026-11-21), so
no system is throttled to the 30% baseline. The surplus is billed (cost table).

### Storage-fault phase (D13): cloud counterpart of RQ4

**What is reused from RQ4** (`rq4-baselines/code/exp_baseline.py`, `configs/README.md`):

| Item | Value |
|---|---|
| Faults | `corrupt` (one byte in the middle flipped, same size, as `s3_fault_proxy.py`) and `lost` (deleted) |
| Workload | topic `ssh-p1`: the whole SSH log, 655,147 records, 1 partition. Objects are RQ4-sized: 40,000 records (Confluent `flush.size`, Vector `max_events`) and 4 MiB (VTOP `max_bytes`, Fluent Bit `total_file_size`) |
| Which write | `nth_faulted_write` N uniform in 2..8, drawn from RQ4's seed 20261111 in schedule order |
| Faulted objects | data objects only; manifests are never faulted |
| Oracle | RQ4's oracle (`locate` + `judge`) and its fields `unsafe_final`, `lost_committed_records`, `complete`, `missing_records`, duplicates |
| Confluent drain target | the last full 40,000-record file (its tail is never written, as RQ4) |

The systems are the perf configurations. VTOP runs in BLAKE3 read-back mode, as RQ4 (decision 12). After every
VTOP fault trial the agent also runs `vtopctl scrub` and `audit-coverage --check-storage`, to show whether VTOP
detects the damage after the fact. The baselines have no such facility.

**How it differs from the MinIO proxy method (to report with the results).** In RQ4 the proxy damaged the object
inside MinIO *before* relaying the 200 to the client, so every system was told "success" for an object that was
already damaged. Here the S3 path stays native:

- the broker host lists the run's prefix every **0.1 s**;
- the N-th data object to appear is damaged as soon as it is listed, with the existing permissions (`GetObject`,
  then `PutObject` of the flipped bytes or `DeleteObject`). A listed object has been acknowledged by S3, so the
  damage always lands after the acknowledgement, as in RQ4.

That damage **races the sink's own next step**, and the race decides which case a trial is:

- **Fluent Bit commits on ingest**, before it uploads. Every trial is therefore `commit_before_object_visible`,
  RQ4's case.
- **Vector** commits every 5 s and **Confluent** about 1 s after the file commit, so the damage normally lands
  before their commit (`damage_before_commit`), RQ4's case.
- **VTOP** HEADs or GETs the object milliseconds after the PUT returns, before a 0.1 s LIST poll can see it. So
  VTOP is expected to verify the intact object first, then commit, with the damage landing **after verification**
  (`damage_after_commit`). That case is outside VTOP's guarantee, and outside every system's. It is RQ4's fault
  only when the damage precedes verification, which a native injector cannot force.

Each trial is therefore classified from measured times:

- `t_visible`, `t_damaged` (broker clock);
- the first committed-offset poll that enters the damaged object's records, as `damage_before_commit`,
  `damage_after_commit`, `commit_before_object_visible`, or ambiguous within one 0.25 s poll;
- for VTOP, its own `verification_passed` time for that object (engine log timestamp joined through the ledger's
  batch → object URI): `before_verification` or `after_verification`.

`analyze.py` reports, per system and fault:

- unsafe trials overall;
- unsafe trials among the RQ4-comparable ones (`damage_before_commit`, `commit_before_object_visible`);
- VTOP's scrub and audit exit codes.

**Expected reading:**

- baselines: the RQ4 result reproduced on S3 (commit past the damaged object);
- VTOP: no RQ4-comparable trial, but post-commit damage detected by `scrub`/`audit-coverage`.

If a VTOP trial does land before verification, it tests the gate directly. All of this is a prediction, not a
result.

**Trials: 5 per system and fault** (40 trials, ≈ 45–60 min of both hosts, ≈ USD 0.3–0.4 incl. the credit bound
and ≈ 24k LIST requests). RQ4 used 10. Its outcomes were deterministic per system (Confluent 20/20 unsafe, VTOP
0/20), so 5 trials show a deterministic pattern. The timing classification also needs a few samples per system.
10 trials cost about USD 0.3 more and add about 1 h (decision 11).

### Reps and the Sydney leg

- **us-east-1:** 5 tput reps give a bootstrap CI on medians as in COST/WAN, with 39 tput runs.
- **Sydney:** 3/2/2 reps, and no gzip variant. Distance effects in E-RQ5-WAN were large: −42% to −66% for VTOP
  alone, CIs a few % wide with 10 reps. So 3 reps resolve the per-system distance effect, which is the purpose of
  this leg. Every extra Sydney tput rep costs ≈ 0.15 GB × 6 configs of inter-region upload (≈ USD 0.09) plus
  ≈ 25 min of two instances.
- **Trade-off:** 10 tput reps in us-east-1 would narrow the CIs about 1.4× and add ≈ 1.5–2 h (≈ USD 0.4–0.6).
  Decision 4.

**Sydney specifics.**

- The broker sits with the sink in Sydney, so the long-distance link is sink→S3, as in E-RQ5-WAN. The oracle's
  GETs also cross the ocean (download USD 0.02/GB), but they run between runs.
- The sink's S3 clients all use region us-east-1 (the bucket).
- **RTT is measured as `wan.py` does (its functions are imported):**
  - `rtt_probe` on the broker (20 TCP handshakes to both S3 names plus 20 HeadBucket) at the start and end;
  - `tcp_rtt` on the sink (20 handshakes to `<bucket>.s3.us-east-1.amazonaws.com`) at the start and end;
  - 5 handshakes before every run.
- The live rate (4 MiB/s) is kept the same as in us-east-1. A system that cannot sustain it across 196 ms shows
  a growing latency, and that is a result.
- The run timeout is 1,200 s, and the run stalls out after 240 s without progress.

## Files

| File | Role |
|---|---|
| `perf.py` | `agent` (sink host; stdlib only), `orchestrate` (broker host), `produce` (live producer subprocess); configs, observer, native fault injector, oracle glue, metrics, recovery, latency, cleanup, incremental result upload |
| `user-data-broker.sh` | AL2023 arm64. In order: failsafe `shutdown -h +390`; EXIT trap; refuses an unfilled `SESSION`; Docker, Python 3.12/3.11 venv (boto3 1.44.0, zstandard 0.25.0, blake3 1.0.11, numpy 2.3.5, confluent-kafka 2.16.2; aarch64 wheels checked); Kafka on the private IP; HDFS_v1.zip and SSH.tar.gz from `cost/input/` (MD5, log SHA-256 and size checked); code; waits ≤ 45 min for `perf/coord/<session>/sink.json`; `perf.py orchestrate`. Per-leg matrix by region (fault phase in us-east-1 only). The trap deletes `perf/data/<session>/` unless `KEEP_DATA`, aborts open multipart uploads there, writes `exit.json` and uploads to `perf/results/<session>-broker-<id>/`. 10.6 KB |
| `user-data-sink.sh` | AL2023 arm64. In order: failsafe `+395`; refuses an unfilled session **or engine pin**; Docker, nftables, ethtool; VTOP release (pinned SHA-256 + `SHA256SUMS`, `vtopctl --version` must contain 0.10.2); the three images by digest; the plugin zip from `perf/input/` (SHA-256); announces `sink.json`; runs `perf.py agent` until `/shutdown` or 30 min without contact; the trap uploads to `perf/results/<session>-sink-<id>/`. 7.8 KB |
| `launch.sh` | one leg: identity check, fills `SESSION` into copies, `--check-params`, stages code (`perf.py`, `wan.py`, `cost.py`, `smoke/oracle.py`, `rq4 exp_baseline.py`, `lib/vtoplab.py`) and the plugin zip, launches **sink then broker** in one subnet, waits, downloads both result folders, lists leftovers |
| `analyze.py` | statistics per leg, the distance effect across legs, and `--make-request-profile` from a dry run's `requests.jsonl` |
| `reqcount_proxy.py` | dry run only: per-request log in front of MinIO |
| `dryrun.sh` | the local dry run (below) |
| `dryrun-20261121/`, `request-profile-dryrun.json` | dry-run evidence and the measured per-object S3 request profile used by `analyze.py --request-profile` |

## Launch (only after the author's go and the decisions below)

From the repo root, with the operator profile:

1. **Pre-flight (read-only).** Credits should be `unlimited`, and no instance should be left over.
   ```bash
   export AWS_PROFILE=vtop-op
   for R in us-east-1 ap-southeast-2; do
     aws ec2 get-default-credit-specification --region $R --instance-family t4g
     aws ec2 describe-instances --region $R --filters Name=tag:project,Values=vtop-eval \
       Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId'
   done
   aws s3 ls s3://vtop-eval-XXXXXXXXXXXX/cost/input/HDFS_v1.zip     # staged for E-RQ5-COST
   ```
2. **Checks:** `bash -n` and `shellcheck` on the four scripts. Optionally re-run `dryrun.sh`.
3. **Leg 1, us-east-1** (performance + fault phase, expected ≈ 4.4 h; the launcher waits up to 410 min):
   ```bash
   research/experiments/rq5-cloud/perf/launch.sh use1 2>&1 | tee research/experiments/rq5-cloud/perf/launch-use1-$(date -u +%Y%m%dT%H%MZ).log
   ```
   Record both instance IDs and the session in `../LOG.md` as the launcher prints them.
4. **Read the first rounds while it runs.** Read `s3://…/perf/results/<session>-broker-<id>/results/runs.jsonl`
   (uploaded after every run). If a system fails to start on EC2 (credentials via IMDS, plugin loading), stop the
   leg: `aws ec2 terminate-instances --instance-ids <both>`.
5. **Leg 2, Sydney**, after leg 1 is read (same command, `apse2`). It can run in parallel with leg 1 once the
   first rounds of leg 1 pass; both legs write to separate session prefixes.
6. **Analyze:**
   ```bash
   P=research/experiments/rq5-cloud/perf
   U=$P/results/<use1-session>-broker-<id>/results; S=$P/results/<apse2-session>-broker-<id>/results
   research/.venv/bin/python $P/analyze.py $U --out $U/analysis --request-profile $P/request-profile-dryrun.json \
     --expect-reps tput=5,live=3,kill=3
   research/.venv/bin/python $P/analyze.py $S --out $S/analysis --request-profile $P/request-profile-dryrun.json \
     --expect-reps tput=3,live=2,kill=2
   research/.venv/bin/python $P/analyze.py $U $S --out $P/results/combined --request-profile $P/request-profile-dryrun.json
   ```
   (`request-profile-dryrun.json` is regenerated from a dry run with
   `analyze.py --make-request-profile <dry>/requests.jsonl <dry>/broker --out $P/request-profile-dryrun.json`.)
   Then record the result in `../LOG.md` and in `hypotheses.md` (H4.2), with the author.
7. **Clean up** (skill step 6). `exit.json` → `data_objects_left` should be 0; the data is kept on purpose if a run
   failed.
   ```bash
   aws s3 rm --recursive s3://vtop-eval-XXXXXXXXXXXX/perf/data/<session>/
   aws s3api list-multipart-uploads --bucket vtop-eval-XXXXXXXXXXXX --prefix perf/ --query 'Uploads[].[Key,UploadId]' --output text
   aws s3 rm --recursive s3://vtop-eval-XXXXXXXXXXXX/perf/coord/
   # keep perf/results/; perf/code/ and perf/input/ may go after the campaign
   aws ce get-cost-and-usage --time-period Start=<day>,End=<day+1> --granularity DAILY --metrics UnblendedCost \
     --group-by Type=DIMENSION,Key=REGION
   ```

## Time and cost (estimate; prices read 2026-11-21)

**Time.** These are estimates, not measurements, because no system has run on Graviton2 against S3 yet. The
first rounds give the real numbers, and the deadline (no new run after 285 min) and the failsafe (330/335 min)
bound the cost either way.

| Part | us-east-1 | Sydney |
|---|---|---|
| bootstrap: dnf, pip, 4 GB of images, VTOP, input; topic load ≈ 1–2 min + byte-exact read-back ≈ 2–4 min | 15–20 min | 20–25 min |
| tput run: start (VTOP 1 s, Vector/Fluent Bit ≈ 3 s, Connect 40–90 s) + drain of 1.43 GiB at an assumed 15–60 MiB/s + oracle (11 M lines, Python, ≈ 40–90 s on Graviton2) | 2–4 min × 39 | 3–6 min × 22 |
| live run: 10 s settle + 60 s + tail + oracle | ≈ 2 min × 18 | 2–4 min × 12 |
| kill run | 2.5–4.5 min × 12 | 4–7 min × 8 |
| fault trial (SSH, 70 MiB; Connect start dominates) | 0.7–2 min × 40 | – |
| **Expected per leg** | **≈ 4.4 h** (2 hosts → 8.8 instance-hours) | **≈ 3.2 h** (6.4 instance-hours) |
| **Cap per leg** (failsafe 390/395 min; no new run after 345 min, Sydney 285 min) | 6.5 h × 2 hosts | same |

**Transfer and requests.**

- **Data volume.** Per tput or kill run, ≈ 0.155 GB of compressed objects goes up and is read once by the
  observer and oracle. Per live run, ≈ 0.025 GB.
- **Sydney volume.**
  - Up: ≈ 5.0 GB (+ 0.2 GB of results and coordination).
  - Down: ≈ 5.7 GB (oracle + BLAKE3 read-back + inputs).
- **Requests per run.** ≈ 400 PUT/LIST-class and ≈ 400 GET-class. These come from about 92 objects × 1–3 PUT-class
  requests, the observer's LIST every second, and the observer/oracle GETs. Sydney runs are longer, so they make
  more LISTs.

| Item | Price used | us-east-1 expected | Sydney expected |
|---|---|---:|---:|
| t4g.large on demand [1] | 0.0672 / 0.0848 USD/h | 0.47 | 0.54 |
| public IPv4 [2] | 0.005 USD/h | 0.035 | 0.032 |
| gp3 30 GiB × 2 [3] | 0.08 / 0.096 USD/GB-month | 0.02 | 0.03 |
| S3 requests, ≈ 28k / ≈ 45k PUT-class [4] | 0.005 per 1,000 PUT/COPY/POST/LIST; 0.0004 per 1,000 GET | 0.16 | 0.25 |
| fault phase (D13): ≈ 0.9 h × 2 hosts + public IPv4 + ≈ 24k LIST at 10/s [1][2][4] | as above | 0.27 | – |
| inter-region upload Sydney → us-east-1 [5] | 0.098 USD/GB | 0 | 0.51 |
| inter-region download us-east-1 → Sydney [5] | 0.02 USD/GB | 0 | 0.11 |
| same-region EC2↔S3 and same-AZ private traffic [5][6] | free | 0 | 0 |
| **subtotal** | | **≈ 0.96** | **≈ 1.47** |
| CPU surplus credits, upper bound (both hosts at 2 × 100% − 30% baseline = 1.4 vCPU-h/h) [7] | 0.04 USD/vCPU-h | ≤ 0.50 | ≤ 0.36 |
| **Expected, with the credit bound** | | **≈ 1.5** | **≈ 1.8** |
| **Cap** (2 hosts × 6.5 h at instance + IPv4 + EBS + credits; S3 and transfer at 1.5× plan) | | **≈ 2.1** | **≈ 3.2** |

**Both legs incl. the fault phase: expected ≈ USD 3.3, cap ≈ USD 5.3.** The monthly budget is USD 10. With COST, WAN and the HDFS
re-check this month, the 50% alert (USD 5) may trigger, and the forecast alert may trigger with it.

Ways to make it cheaper, with what each one loses:

- us-east-1 with 3 tput reps: −0.3 USD, wider CIs.
- No warm-up runs: −0.15 USD, risks first-run effects.
- A half-size Sydney topic: −0.3 USD, and the topic would no longer be identical across legs.

Sources (read 2026-11-21):

1. AWS public price list, EC2 on-demand Linux, `b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2-ondemand-without-sec-sel/<region>/Linux/index.json`, publication 2026-11-21T00:48:16Z: us-east-1 t4g.large 0.0672, t3.large 0.0832; Sydney t4g.large 0.0848, t3.large 0.1056 USD/h.
2. <https://aws.amazon.com/vpc/pricing/> (as cost/README source 2).
3. cost/README source 3 (us-east-1 0.08) and wan/README source 3 (Sydney 0.096), EBS price list 2026-11-20.
4. cost/README source 5, AmazonS3 us-east-1 offer (Requests-Tier1 USD 0.005 per 1,000; Tier2 USD 0.004 per 10,000).
5. AWS Price List API, AWSDataTransfer offer version 20261119105841 (publicationDate 2026-11-19T10:58:41Z), files `ap-southeast-2` and `us-east-1`, re-read 2026-11-21:
   - "$0.098 per GB - Asia Pacific (Sydney) data transfer to US East (Northern Virginia)";
   - "$0.02 per GB - US East (Northern Virginia) data transfer to Asia Pacific (Sydney)";
   - "$0.00 per GB - Asia Pacific (Sydney) data transfer from US East (Northern Virginia)" (inbound);
   - intra-region "regional data transfer … between EC2 AZs" is $0.010/GB in us-east-1, avoided by the single
     subnet.
6. <https://aws.amazon.com/s3/pricing/>: transfer to AWS services in the same Region is free (cost/README source 6).
7. <https://aws.amazon.com/ec2/instance-types/t4/>: "$0.04 per vCPU-hour" (cost/README source 4). The Sydney rate is
   assumed equal (not read per region).
8. Instance limits: <https://docs.aws.amazon.com/ec2/latest/instancetypes/gp.html>:
   - t4g.large and t3.large: 0.512 / 5.0 Gbps network, 86.88 / 347.50 MB/s EBS;
   - t4g.large: 2 cores × 1 thread;
   - t3.large: 1 core × 2 threads.

## Local dry run (2026-11-21)

**All numbers below are noise.** The dry run ran on one macOS host (Apple M5 Max, 18 cores) with MinIO behind a
Python counting proxy. Another agent's Docker soak job (`vtop-727-*`) was running at the same time, so Docker was
slow and contended. The dry run exercises the code paths, not performance.

**Setup** (`dryrun.sh`):

- containers on network `rq5perf-net` (explicit subnet 10.231.77.0/24, because Docker's default pools were
  exhausted by other projects): Kafka 3.9.1 and MinIO RELEASE.2025-09-07 (RQ4 images), the three sink images by
  their RQ4 digests, and a cgroup helper;
- the v0.10.2 `aarch64-apple-darwin` vtopctl, and the agent and orchestrator as local processes (Python 3.11,
  confluent-kafka 2.16.2);
- ports 19092/19400/19401/18080/18083;
- dry-run object size 2 MiB / 15,000 records; tput topic 4 × 20 × 15,000 HDFS records (160 MiB); live topic
  225,000 records at 7,500 rec/s for 30 s; fault topic: the whole SSH log (655,146 lines) at RQ4's object size.

Only `rq5perf-*` containers were created, and all were removed by the script's trap (checked after every run).
Other containers were not touched. Evidence: `dryrun-20261121/` (`run.json`, `runs.jsonl`, `ANALYSIS.md`,
`summary.json`, rendered configs; `dry2/requests.jsonl.gz` = every S3 request). The derived per-object profile is
`request-profile-dryrun.json`.

**Runs.**

- `dry2`: the full matrix, one rep (7 tput, 6 live, 4 kill). 16/17 passed.
  - The one failure was correctly caught by the `killed_before_drain` check: on the Mac, Confluent (`tasks.max` 4)
    drained the 160 MiB topic in ≈ 1.5 s, so the SIGKILL landed after the drain.
  - `dry1`, a first partial run, found three things, all fixed: the object size must scale with the topic; the
    kill trigger must also use archived records; and the steady window needs more than two commits.
- `dry3`: the changed paths plus the **fault phase, 4 systems × 2 faults × 1 trial**. 14/14 passed.
- `dry4`: a regression check after the last fixes. 3/3 passed.

| System | tput (dry2/dry3), MiB/s | CPU s/GiB | Peak anon MiB | live p50 / p99 s (steady window) | SIGKILL: first commit / re-steady s, duplicates |
|---|---|---:|---:|---|---|
| VTOP size-only / store SHA-256 / BLAKE3 / SHA-256+gzip6 | committed 9.0 / 4.7–9.6 / 7.8 / 9.0 | 10.8 / 12.9–15.4 / 15.5 / 21.1 | 115–150 | 1.55–1.57 / 2.47–2.49 (store SHA-256) | 0.98–1.01 / 10.1, **0 duplicates** |
| Confluent | committed 102, archived 129 | 14.8 | 1,320 | 1.45 / 2.47 (dry2, all records) | kill landed after the drain on the Mac (run failed by design) |
| Vector | archived 32–43; commits every 5 s, so ≤ 2 commit points in a 5 s drain | – | 570–580 | 3.41 / 6.26 (dry2) | 10.5 / –, **600,000–615,000 duplicate records** (5 s commit interval, as RQ4) |
| Fluent Bit | committed 4.8, archived 5.0 | 12.4 | 146 | 1.68 / 2.67 | **45.5** / 54.4: the new consumer waits for the killed member's 45 s session timeout; 0 duplicates |

**Observed behaviours worth checking in the cloud:**

- **VTOP drained the 4 partitions mostly one after another** (commit series in `dry2`; 0.07–0.19 cores), so it was
  latency-bound per batch, not CPU-bound.
- **Fluent Bit's objects overshoot `total_file_size`:** 4.9 MiB against 2 MiB, because the size is checked on a
  timer.
- **Measured S3 request profile per data object** (MinIO, `request-profile-dryrun.json`):

  | System | Requests per data object |
  |---|---|
  | VTOP size-only | 2 PUT + 2 HEAD |
  | VTOP store SHA-256 | 2 PUT + 3 HEAD |
  | VTOP BLAKE3 | 2 PUT + 3 HEAD + 2 GET |
  | Confluent | CreateMultipartUpload + UploadPart + Complete, even for one part, + 0.08 HeadBucket |
  | Vector | 1 PUT (+ HeadBucket at start) |
  | Fluent Bit | 1 PUT |

- **Live:** the steady window matters. Without it, VTOP's and Fluent Bit's p99 included the last age-flushed
  partial object (10.6–14.5 s).

**Fault phase (dry3; MinIO emulates the overwrite and delete; injector = the broker's listing).**

- **Every trial behaved as predicted.** The damage landed 8–24 ms after the object was first listed.
- **Confluent and Vector:** `damage_before_commit` (0.9 s and 2.5–3.6 s before their commit), commit past it,
  `unsafe_final` 1, **40,000 lost committed records** (RQ4 reproduced).
- **Fluent Bit:** `commit_before_object_visible`, unsafe, 41,202 / 42,486 lost committed records.
- **VTOP:** the damage landed **after VTOP's own verification** (log time via the ledger), within one poll of its
  commit. So VTOP committed an object that was intact when verified, and the final state is unsafe
  (38,511 / 39,003 records). This is post-verification damage, outside the guarantee. **`vtopctl scrub` detected it
  in both trials (rc 1)**. `audit-coverage --check-storage` detected the deletion (rc 1) but not the same-size
  corruption (rc 0).
- This confirms the race described in [Storage-fault phase](#storage-fault-phase-d13-cloud-counterpart-of-rq4).

**Checks:**

- `bash -n` and `shellcheck` are clean on `user-data-broker.sh`, `user-data-sink.sh`, `launch.sh` and `dryrun.sh`.
- `--check-params` refuses an unfilled session (rc 2) and an unfilled engine pin (rc 1).
- Filled copies are 10.6 KB and 7.8 KB, under the 16 KB user-data limit.
- `pyflakes` is clean on the three Python files.
- `RunInstances --dry-run` passed in both regions (see below).

**Not exercised locally:**

- Linux cgroup paths and the nft and ENA counters (Linux only);
- IMDS credentials inside containers;
- the broker's topic load at 11 M records;
- the `wan.py` RTT probe (lab endpoints only);
- the real S3 request mix.

## Decisions for the author

1. **Datasets:** HDFS only (11.04 M records on 4 partitions for tput/kill; 1.8 M records on 1 partition for
   latency). SSH is dropped because it is too small for steady state at 16 MiB objects. The alternative is SSH as
   an extra single-partition tput cell, which is noisy (≤ 5 objects).
2. **Instance type and architecture:** 2 × t4g.large, arm64 (vs t3.large x86_64: +24% price, one physical core).
3. **Confluent `tasks.max` = 4** (one task per partition, the documented scaling knob) instead of RQ4's 1. With
   1, Confluent runs gzip on one thread on a 2-core host, and reviewers can call that unfair. This does not
   change its delivery semantics.
4. **Reps:** us-east-1 5/3/3 and Sydney 3/2/2 (≈ USD 3.3 total with the fault phase). Or 10 tput reps in us-east-1 (+ ≈ USD 0.5,
   + ≈ 2 h).
5. **Codec fairness:** each system's closest codec is used (VTOP and Vector zstd-3, Fluent Bit zstd-1 fixed,
   Confluent gzip-6), plus the **VTOP gzip-6 variant** for a codec-matched comparison with Confluent
   (us-east-1 only). Keep or drop.
6. **S3 request counts:** estimated (dry-run profile × cloud object counts). A measured count on AWS needs **new
   permissions**, which were not requested:
   - for the operator: `s3:PutMetricsConfiguration` and `s3:GetMetricsConfiguration` on the bucket, plus
     `cloudwatch:GetMetricData`;
   - one request-metrics filter per system prefix. CloudWatch S3 request metrics are billed as custom metrics,
     about USD 0.30 per metric-month, prorated hourly.

   S3 server access logging or CloudTrail data events are the alternatives, and also need new permissions.
7. **Common age 10 s** for VTOP, Vector and Fluent Bit (RQ4: VTOP 1–2 s, Vector 5 s, Fluent Bit 10 s). Confluent
   keeps no time rotation (RQ4 EOS config), so its drain target is whole objects. The topic sizes are exact
   multiples of `flush.size`, so nothing is left unwritten.
8. **VTOP receipts off** (engine default) here, whereas COST had them on. The method is still proven by the log
   `verification_passed` text and the ledger class on every run.
9. **Budget:** both legs and the fault phase (≈ USD 3.3 expected, ≈ 5.3 cap) within the USD 10/month budget, which may raise the 50%
   e-mail alert.
10. **Order of legs:** us-east-1 first, Sydney after its first rounds are read (recommended), or both in parallel.
11. **Fault phase size:** 5 trials per system × fault (40 trials, ≈ USD 0.3–0.4) or RQ4's 10 (≈ USD 0.7, + ≈ 1 h).
12. **VTOP mode in the fault phase:** BLAKE3 read-back (as RQ4) or store SHA-256 (the cheaper strong mode on S3).
    The race with the injector is the same either way.
13. **Reporting the fault phase:** expect few or no RQ4-comparable VTOP trials (verification wins the race). The
    proposal is to report the timing classes as measured, keep RQ4's MinIO-proxy result as the controlled
    experiment, and present the cloud phase as a check on real S3 that reproduces the baselines' behaviour and
    shows VTOP's after-the-fact detection. A proxy on EC2 would force the RQ4 case, but it would make the S3 path
    non-native and need request re-signing, so it is not proposed.

## Not verified

- **Nothing has run on EC2.** These have not been exercised:
  - the cgroup v2 paths on AL2023 (`system.slice/docker-<id>.scope`, the agent-created `rq5perf-<run>` cgroup for
    VTOP; the macOS dry run read the same files through a helper container);
  - the nft counters and ENA counters;
  - IMDSv2 credentials inside the Confluent, Vector and Fluent Bit containers (host network, hop limit 2);
  - Connect start-up time on Graviton2;
  - the oracle's speed on 11 M lines on Graviton2.

  The first round shows each of these. A system that cannot start fails its run, the run is recorded, and the
  leg stops after 8 failures.
- **Permissions:** `RunInstances --dry-run` with this exact shape (t4g.large, 30 GiB gp3 root, IMDSv2 hop limit
  2, instance profile, tags, default subnet) answered "Request would have succeeded" in us-east-1
  (subnet-06f60d660fa1815b6, us-east-1a) and ap-southeast-2 (subnet-b6993cff, ap-southeast-2a). Both default
  security groups allow all traffic between members of the group (describe-security-groups), so broker:9092 and
  sink:18080 are reachable with no new rule. The instance role's S3 actions cover the coordination files,
  results, deletes and aborts (cost/README "Not verified").
- **Throughput and time estimates:** as stated above. No Graviton2 number exists yet for any of the four systems
  on Kafka.
- **S3 request profile:** measured against MinIO. The AWS SDKs may differ against S3 (for example, the checksum
  headers and retries).
- **Fluent Bit tag drift:** the RQ4 digest is used. Whether the tag `5.2.0` was re-pushed with a functional change
  has not been checked.
