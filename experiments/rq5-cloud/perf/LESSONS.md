
## 2026-11-21 — Engine maintainer's bench on the throughput gap (indicative, NOT our measurement)
Source: vtop-engine maintenance session, message of 2026-11-21. 200 ms netem bench shaped like our Sydney leg (Kafka
3.9.1 local, 1 GiB JSONL, 16 MiB batches, zstd 3, store SHA-256, s3_native to MinIO, ~1.4 MiB single-PUT objects, no
bandwidth cap). The host was heavily loaded, so the maintainer calls the numbers indicative; our AWS run is the measurement.

| MiB/s end to end (to 90% drained) | 1 partition | 4 partitions |
|---|---|---|
| v0.10.3 as shipped | 7.8 (8.5) | 8.8 (9.8) |
| + `net.ipv4.tcp_slow_start_after_idle=0` | 9.9 (11.1) | 11.6 (13.7) |
| pipelined engine (prototype) | 33.2 (51.1) | 32.5 (53.1) |
| pipelined + slow start off | 34.6 | 33.5 |

- Slow start after idle costs ~27–32% on the shipped engine (a 1.4 MiB PUT drops from ~680–900 ms to ~240–290 ms with it off).
- Stop-and-wait scheduling is the dominant cause: pipelining alone gives ~4× end to end, ~5× steady state; with
  pipelining the TCP setting adds only ~3%. CPU per GiB unchanged (11–13 CPU-s/GiB).
- 4 partitions ≈ 1 partition today because a read pass is budgeted at `max_bytes` across all partitions; 64 MiB
  batches hit a Kafka read-path stall. Both are addressed by the pipelining (planned for v0.11.0).
- Commit rule held through 22 SIGKILLs on the 200 ms link; 571 engine tests pass on the prototype. Verification stays
  an independent read of the stored object.
- `tcp_slow_start_after_idle=0` is a legitimate v0.10.x deployment tuning (maintainer will document it).
- Paper use: none yet. The paper keeps its own measured statement (verification is not the main cause of the gap). The
  bench may be cited only as "the maintainers attribute the gap to …" once the v0.11.0 PRs are public and linkable.
- RESOLVED (see research/state-machine/v0101-changes.md §18, fix PR #891): the maintainer reported a data-loss bug for v0.10.4 ("two windows of one file that fail in the same pass rewind
  to the higher start"). Asked whether v0.10.2 is affected and under which conditions; this touches §V's safety claim.

## 2026-11-22: a Vector run failed on the oracle, not on Vector (v0.10.4 re-run, leg use1)

- On-VM verdict of `106-tput-vector-r4` (session `20261122T041404Z-use1`, a throughput run with no fault): incomplete,
  1,149,698 records missing in partition 2, 1 unsafe commit, 1 lost committed record, 1 duplicate. The broker exited 1
  and kept `perf/data/20261122T041404Z-use1/` (92 objects) for diagnosis.
- Cause: RQ4's `locate()` places Vector/Fluent Bit lines by content. HDFS input lines 6,441,210 and 6,441,211
  (1-based) are byte-identical, so p1 and p2 hold the same record at offset 1,610,302. Vector interleaves partitions
  line by line; the old tie-break compared only the very next line (p0's) and fell back to the first candidate, p1.
  That read as a duplicate at p1 and a hole at p2 that cut p2's intact prefix. Every other record was placed.
  2,085 of the first 11.04 M HDFS lines repeat the previous line (0.019 %).
- Fix (`rq4-baselines/code/exp_baseline.py`): two-pass `locate()` (unique lines first; an ambiguous line goes to a
  candidate the object does not hold yet, preferring the one that continues its partition there) plus
  `reconcile()` across objects (a line on a doubly covered record moves to an identical record no interval covers).
  For lines with unique content the placement is unchanged. `test_locate.py`: both observed patterns, 2,000 random
  archives with repeated input lines at 50× the HDFS rate (old rule wrong in 713, new in 0), and 2,000 + 2,000 fault
  cases (a dropped line is always a hole, a replayed line always exactly one duplicate).
- Offline re-judgement (`perf/rejudge.py`, from the 92 downloaded objects and `HDFS_v1.zip`): the old rule reproduces
  the on-VM verdict exactly; the new one gives complete, 0 missing, 0 unsafe, 0 lost, 0 duplicates, 367 intervals,
  0 reconcile moves. `analyze.py` applies `rejudge-*.json` files and lists them under "rejudged" in the summary.
- Two passing runs of the same session (`013-kill-vector-r0`, `074-kill-vector-r2`) also had one ambiguous line each;
  their objects were deleted on pass, so they cannot be re-judged. The new oracle accepts every archive the old one
  accepted as complete (a hole remains only if no identical spare copy exists), so their pass verdicts stand; their
  duplicate counts could differ by at most one record each.
- perf.py and the RQ4 sinks now call `reconcile()`; runs before 2026-11-22 used the old rule.
