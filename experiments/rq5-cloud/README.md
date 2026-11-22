# RQ5 (cloud): VTOP on a public cloud — EC2 + Amazon S3

Board task: C27. Hypotheses: H3.5 (new), H4.2 (performance trade-off, previously NOT TESTED)
([../../hypotheses.md](../../hypotheses.md)). Running notes: [LOG.md](LOG.md). Lessons: [LESSONS.md](LESSONS.md).

## Why

Two gaps of the earlier evaluation: the experiments were done on a single laptop, which does not show the
applicability on a large-scale cloud platform, and the technique was not quantitatively compared with prior
similar work. All earlier
results ran on one macOS host (S3 over a local Docker network, emulated latency). This experiment moves the
storage to Amazon S3 and the engine and sources to separate EC2 hosts, and adds the missing performance
comparison with the three fault-matched sinks.

## Experiments (staged; each stage needs the author's go-ahead before it spends money)

| ID | Purpose | Scale | Status |
|---|---|---|---|
| E-RQ5-SMOKE | Feasibility: v0.7.0 on EC2 against real S3; does each verification mode (size-only, store SHA-256, BLAKE3 read-back) drain and pass the oracle, and does store SHA-256 really use the service checksum? | 1 small arm64 instance, ~2 min per attempt, one small file input | **passed 2026-11-20** (attempt 3; two earlier attempts failed in our bootstrap and oracle, see LOG) |
| E-RQ5-COST | H3.5: the RQ3 cost matrix (SSH, BGL, HDFS × size-only / store SHA-256 / BLAKE3 read-back, 10 reps) on EC2 + S3 | one engine host, t4g.medium (D7; file sources local, S3 separate) | **done** 2026-11-21: 90/90 + HDFS re-check 30/30 pass (H3.5 partly supported) |
| E-RQ5-WAN | Long distance and low bandwidth on real networks: engine in a far region (e.g. ap-southeast-2 or eu-west-1) writing to the us-east-1 bucket (real cross-region round trip), plus bandwidth limits applied on the engine host (Linux `tc`) and VTOP's own `upload.max_egress_bytes_per_second`; replaces the emulated +20/+50 ms and the engine's emulated 100 ms / 10 Mbit/s results with measured ones | engine host in a far region | **done** 2026-11-21 in eu-west-1, sa-east-1, ap-northeast-1, ap-southeast-2 (D8, D9): 272/272 pass, 0 unsafe (H3.6 supported) |
| E-RQ5-TAMPER | H2.5–H2.8: signed manifests (`manifest.mac`) against forgery and manifest swap with and without the ledger as anchor; Object Lock and versioning against overwrite, delete and rollback; audit-chain tampering; SSE-S3/SSE-KMS once released. Reuses the RQ2 tamper tool and the no-recheck build | us-east-1; a second bucket with Object Lock + versioning (needs new S3 permissions, author approval) | planned (v0.10.2; encryption later) |
| E-RQ5-PERF | H4.2: no-fault throughput, CPU and upload latency for VTOP vs Confluent S3 Sink 12.1.15, Vector 0.61.0, Fluent Bit 5.2.0 on the same topic and bucket; recovery time after a kill | Kafka host + sink host | planned |

## Pinned environment

| Item | Value |
|---|---|
| Cloud, region | AWS, us-east-1 (compute and storage in the same region: no transfer charge) |
| Engine | vtop-engine **v0.10.1 (44f6619)** (decision D4, 2026-11-20), official aarch64/x86_64 Linux release tarballs, verified against SHA256SUMS and sigstore bundles |
| Bucket | `vtop-eval-XXXXXXXXXXXX` (private, public access blocked) |
| Instance identity | instance profile `vtop-eval-ec2`: S3 on that bucket only; no keys on hosts |
| Operator identity | IAM user `vtop-eval-operator` (local profile `vtop-op`): EC2 in us-east-1 only, t3/t4g micro–large only, no reservations/hosts/fleets, S3 on the bucket only |
| Cost guard | AWS Budget `vtop-eval-monthly`, USD 10/month, e-mail alerts at 50% and 85% actual and 100% forecast |
| Instance types, AMI | recorded per run in `run.json` (pending) |

## Recording rules (same as the rest of `research/`)

- Every run writes `run.json` (VTOP commit, binary SHA-256, instance type, AMI, region, start/end UTC,
  config) and its raw records under `results/<date>-<id>/`; nothing is summarized by hand.
- Costs are recorded from AWS Cost Explorer after each stage, in [LOG.md](LOG.md).
- Negative outcomes (a mode that does not work on S3, a run that does not drain) are results and are kept.
- No number enters the paper before its hypothesis is answered in `hypotheses.md`.
- Clean-up of every created resource is logged; the resource table is in [LESSONS.md](LESSONS.md).
