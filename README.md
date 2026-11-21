# Shopping Sessions: Rust pipeline

One Rust binary (`src/main.rs`) that implements `rules.md`: CSV cleaning, linked IDs,
sessions, last-click attribution with 7-day assists, and the two daily Parquet tables.

## Design

- **Input**: every CSV is memory-mapped and split into ~4 MB line-aligned chunks processed in
  parallel (rayon, `NCPU` threads). A hand-written AVX2 tokenizer follows the rules' dialect
  exactly: only `\n` ends a line, every comma separates fields, quotes must wrap a whole field.
- **Phase A**: the links union-find runs on one thread while the other threads parse and
  validate events and clicks. Events are scattered into 256 buckets by `event_id` hash.
- **Phase B**: per bucket, dedup on `event_id` with an open-addressing table (smallest
  `ingest_ts` wins, before validation), then each surviving event's person is resolved through
  the links table (prefetched) and scattered into 256 buckets by person hash.
- **Phase C**: per person bucket, sort by `(person, ts)`, sessionize (gap > 30 min), and credit
  each session with a sliding 7-day window over the person's clicks sorted by `(ts, click_id)`.
  The window keeps per-campaign counts, so assists are the active campaign set.
- **Output**: uncompressed Parquet via the `parquet` crate, written in parallel.

IDs are carried as 16-byte inline keys after parsing, so later phases never touch the mapped
files at random. Hash equality is always confirmed on the bytes, so collisions cannot merge
two events or two people.

## Test

```sh
cargo test --release
cargo build --release
python3 scripts/run_fixtures.py fixtures -- target/release/shopping-sessions
DATA_DIR=dev OUTPUT_DIR=out target/release/shopping-sessions && python3 scripts/compare.py out dev-answer
```
