# Loghub: real system logs for the VTOP experiments

**Source:** Loghub, Zenodo record 8196385
(<https://zenodo.org/records/8196385>), repository
<https://github.com/logpai/loghub>.

**Citation (verified against Crossref, 2026-11-10):**

```bibtex
@inproceedings{zhu2023loghub,
  author    = {Jieming Zhu and Shilin He and Pinjia He and Jinyang Liu and Michael R. Lyu},
  title     = {Loghub: A Large Collection of System Log Datasets for {AI}-driven Log Analytics},
  booktitle = {Proceedings of the 2023 IEEE 34th International Symposium on Software Reliability Engineering (ISSRE)},
  pages     = {355--366},
  year      = {2023},
  month     = oct,
  publisher = {IEEE},
  doi       = {10.1109/ISSRE59848.2023.00071}
}
```

Add it to `VTOP-IEEE-TCC/references.bib` when Section VII first cites it.

**License (repository LICENSE, verbatim terms):** free for research or
academic work; any use or distribution must refer to the loghub
repository URL and cite the paper above; the license notice must be
included in all copies. Zenodo lists the record as CC-BY-4.0. The raw
files stay local (git-ignored) and are not redistributed from this repo.

## Datasets and their role

| Dataset | What it is | Archive | VTOP source it feeds | Experiments |
|---|---|---|---|---|
| **HDFS_v1** | Hadoop HDFS DataNode/NameNode logs | 178 MiB zip | **Kafka**: lines produced to a topic (partitions fixed per run) | RQ1, RQ3, RQ4 |
| **BGL** | Blue Gene/L supercomputer RAS log | 55 MiB zip | **File**: appended to a log file at a controlled rate, with rename rotation and `copytruncate` cases | RQ1, RQ3, RQ4 |
| **SSH** | OpenSSH server syslog | 4 MiB tar.gz | **Syslog spool**: written to an rsyslog-style spool | RQ1, RQ2 |
| **Linux** | Linux system syslog | 0.2 MiB tar.gz | **Syslog spool** (small, fast fault-matrix runs) | RQ1, RQ2 |
| Thunderbird (optional) | Supercomputer syslog, ~30 GiB raw | 1.9 GiB tar.gz | spool / file at scale | RQ3 scaling only |

Line counts, byte sizes, and SHA-256 per file are recorded in
[inventory.csv](inventory.csv) by the fetch script; quote those, not
the paper's numbers.

Why these: they are real, public, widely used telemetry; they cover
application logs (HDFS), hardware/RAS events (BGL), and syslog (SSH,
Linux), which matches VTOP's heterogeneous-source claim; and their sizes
fit this Mac.

## How records are used

- Lines are replayed **unmodified**. The oracle identifies a record by
  `(source unit, position)` (Kafka offset, file byte offset, spool byte
  offset) and checks archived bytes against the source bytes, so no tag
  needs to be injected into the payload.
- Replay rate, batch thresholds, and partition counts are experiment
  factors, recorded in each run's `run.json`.
- Log lines contain host names and IP addresses from the original
  (public) collections; they are not personal data of this project, but
  no excerpts beyond a few illustrative lines go into the paper.

## Fetch

```bash
research/datasets/loghub/fetch.sh               # HDFS_v1, BGL, SSH, Linux (~240 MiB download)
research/datasets/loghub/fetch.sh Thunderbird   # adds ~1.9 GiB download, ~30 GiB on disk
```

Downloads are MD5-checked against the Zenodo record and extracted under
`raw/` (git-ignored).
