# Data and inputs the study needs

## 1. Real dataset (decided 2026-11-10): Loghub

Details, citation, license, and fetch script:
[datasets/loghub/README.md](datasets/loghub/README.md).

| Dataset | Lines | Bytes (raw) | Feeds VTOP source |
|---|---|---|---|
| HDFS_v1 | 11,175,629 | 1,577,982,906 | Kafka topic |
| BGL | 4,747,963 | 743,185,031 | File (with rotation/truncation) |
| SSH | 655,146 | 73,417,506 | Syslog spool |
| Linux | 25,567 | 2,349,686 | Syslog spool (fast fault runs) |
| Thunderbird | (optional, not downloaded) | ~30 GiB | scaling only |

Counts are from [datasets/loghub/inventory.csv](datasets/loghub/inventory.csv),
measured locally after MD5 verification.

## 2. Synthetic workloads (still needed)

Real logs fix the content; synthetic workloads control the variables the
real logs cannot.

| ID | Description | Used by |
|---|---|---|
| W-size | Fixed record sizes 256 B / 1 KiB / 16 KiB, seeded generator | RQ3 (isolates size effects) |
| W-skew | Injected per-batch delays (earliest range slow, then failing) | RQ1 fault F9 / OQ1 |
| W-rot | File rename rotation and `copytruncate` driven on BGL lines | RQ1 (file adapter) |
| W-mix | HDFS→Kafka + BGL→file + SSH→spool at once | RQ1 H1.3, RQ4 H4.3 |

Generator code goes in `datasets/generator/` (to write); generated files
under `datasets/generated/` (git-ignored).

## 3. Environment (decided 2026-11-10): the author's Mac

| Item | Value |
|---|---|
| Machine | Apple M5 Max, 18 CPU cores (6 performance + 12 efficiency) |
| Memory | 128 GiB |
| Storage | 4 TB Apple SSD (APFS), ~1.3 TB free |
| OS | macOS 27.0.1 (build 26A434) |
| Container runtime | Docker 29.9.3 |
| Rust | 1.99.1 (VTOP MSRV is 1.94.1) |
| TLA+ tools | not installed (needs a JDK + `tla2tools.jar`), or use a Python checker |
| Kafka, MinIO, Toxiproxy | not installed natively → run as pinned Docker images |

Consequences the paper must state (threats to validity):

- **Single host.** Engine, Kafka, object store, and baselines share one
  machine, so network latency is loopback unless Toxiproxy adds delay;
  absolute throughput is not representative of a cluster. Pin CPU and
  memory per container so systems get equal resources.
- **Docker on macOS runs in a Linux VM.** `fsync` and disk behavior
  inside the VM differ from bare-metal Linux; power-loss tests (F8) must
  be emulated (ledger-tail truncation), not done by cutting power.
- **Heterogeneous cores.** Record whether containers land on
  performance or efficiency cores; repeat runs and report variance.
- **One cloud confirmation series** (real S3) remains recommended for
  any claim about service-computed checksums; optional, budget TODO.

## 4. Author inputs still missing

- CAOM and FTLADS bibliographic details and which methodology elements
  to mirror.
- H3.2 overhead threshold (fix before running RQ3).
- Whether the PostgreSQL state store is in scope (OQ4).
- Budget for an optional real-S3 confirmation series.
