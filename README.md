# Research artifact: A Verification-Bound Commit and Recovery Protocol for Heterogeneous Telemetry Archival

This artifact accompanies the paper by Tamir Suliman (submitted to *IEEE Transactions on Cloud Computing*). It contains everything behind the paper's reported numbers:
- the explicit-state model and its counterexamples;
- the fault-injection harnesses and the independent byte-exact oracle;
- the raw run records;
- the analysis scripts;
- the generated tables and figures.

**System under test:** VTOP engine **v0.7.0** (tag `v0.7.0`, commit `6c8f386`), <https://github.com/allamiro/vtop-engine>. Every result here is pinned to that tag.

## Layout

| Path | Contents | Paper |
|---|---|---|
| `model/vtop_model.py` | Bounded explicit-state model of one source unit. It covers reads, uploads, verification pass and fail, adapter commit, crash, ledger-tail loss, storage mutation, rewind and recovery holdback, and checks the commit rule, source-progress safety, revalidation and at-least-once | §V-D, Fig. 3, Table (RQ1 model) |
| `results/rq1-model-sweep.tar.gz` | Model sweep: eight safe v0.7.0 configurations (largest 793,353 states) and the v0.6.0 and two-batch counterexample traces | §V-D |
| `experiments/rq1-source-safety/` and `results/rq1-v070-{crash,kafka}.tar.gz` | 49 crash and outage trials on file, spool and Kafka sources (process kills, broker outages, WAL deletion), with per-trial engine logs, ledgers (SQLite and WAL, kept byte for byte), configurations and oracle verdicts | §VII RQ1 |
| `experiments/rq2-corruption-recovery/` and `results/rq2-v070-tamper.tar.gz` | Evidence tampering (64 mutations) under each verification mode | §VII RQ2 |
| `experiments/rq3-cost-ablation/` and `results/rq3-v070-cost*.tar.gz` | Verification-cost runs, stage timings and the batch-size sweep | §VII RQ3 |
| `experiments/rq4-baselines/` | Baseline comparison (in progress at time of packaging; see that README) | §VII RQ4 |
| `experiments/lib/vtoplab.py` | Shared harness and the independent byte-exact oracle | §VI, §VII |
| `analysis/` | `analyze.py` (tables, figures, `summary.json`), diagram sources, and `verify_manuscript_data.py`, which checks every number in the manuscript against `summary.json` | all |
| `datasets/loghub/` | `fetch.sh` and `inventory.csv` (SHA-256 of every input file) | §VII setup |
| `RESULTS.md`, `hypotheses.md` | Results summary and the hypothesis register each result answers | — |
| `SHA256SUMS` | Checksums of every file in this artifact | — |

## Data not included

The experiments use real logs from **Loghub**:
- Zhu et al., ISSRE 2023, doi:10.1109/ISSRE59848.2023.00071;
- Zenodo record 8196385.

Loghub is free for research and academic use, with attribution. This artifact does not redistribute it, or any copy of it:
- **Omitted:** trial inputs and the ground-truth topic dumps `raw/*.txt` are left out of the archives.
- **To rebuild:** run `datasets/loghub/fetch.sh`, then confirm the files match `inventory.csv`.

## Reproducing

1. **Build the engine at the tag:**
   ```
   git clone https://github.com/allamiro/vtop-engine && cd vtop-engine && git checkout v0.7.0
   cargo build --release
   ```
   The harness expects the binary at `.tmp/vtop-engine-v0.7.0/target/release/vtopctl`, relative to the directory that contains `research/`. Adjust `VTOPCTL` in `experiments/lib/vtoplab.py` if yours is elsewhere.
2. **Install Python dependencies:** Python 3.12 or later with `zstandard` and `matplotlib`.
3. **Fetch the data:** `datasets/loghub/fetch.sh`.
4. **Re-run experiments:**
   - Each experiment directory's `README.md` gives its exact command and configuration.
   - `experiments/run_v070.sh` runs the v0.7.0 set.
   - The Kafka experiments need Docker; the image `apache/kafka:3.9.1` is pinned.
5. **Rebuild tables and figures:** `python analysis/scripts/analyze.py`, then `python analysis/scripts/verify_manuscript_data.py`.

To re-analyse without re-running, unpack the archives under `results/` into the matching `experiments/*/results/` directories.

## Scope and limits (as stated in the paper)

- **Platform:** one host and a local object-store backend. Service-side checksum cost on networked S3 was not measured.
- **Model:** the model is an abstraction of the implementation, not a proof of the Rust code.
- **Delivery semantics:** VTOP is at-least-once, and replay can create duplicate objects.

## License

- **Code** (`model/`, `experiments/`, `analysis/scripts/`): MIT, see `LICENSE`.
- **Run records, tables, figures and documentation:** CC BY 4.0, see `LICENSE-DATA`.

## Citation

See `CITATION.cff`. Please also cite Loghub when using the data.
