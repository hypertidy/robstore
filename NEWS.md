# robstore 0.1.0

First working release. R bindings to the Rust
[`object_store`](https://crates.io/crates/object_store) crate via
[extendr](https://extendr.rs/), with a uniform API across local
filesystems and S3-compatible object stores.

## Backends

* `memory_store()` — in-memory store.
* `local_store(path)` — local filesystem rooted at `path`.
* `s3_store(bucket, region, endpoint, allow_http)` — AWS S3 or any
  S3-compatible service (Pawsey, MinIO, etc.). Credentials resolved
  from the standard `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
  environment variables.
* `s3_store_anonymous(bucket, region, endpoint, allow_http)` — public
  S3 buckets with unsigned requests (e.g. `sentinel-cogs`).

## Core operations

* `store_put()`, `store_get()`, `store_get_range()` — write, read,
  byte-range read.
* `store_exists()`, `store_delete()`, `store_copy()` — metadata and
  single-object operations.
* `store_list(store, prefix)` — list keys under a prefix.

## Concurrent operations

All run on a shared multi-threaded tokio runtime:

* `store_get_many(store, keys, concurrency)` — concurrent full-object
  reads.
* `store_get_ranges_many(store, keys, offsets, lengths, concurrency)` —
  concurrent byte-range reads. The primitive for COG IFD walks, Zarr
  chunk scatter-reads, and Parquet row-group reads.
* `store_list_many(store, prefixes, concurrency)` — concurrent listings
  across many prefixes, for hierarchical layouts (years, MGRS tiles).
* `store_head_bytes(store, keys, length, offset, concurrency)` — R-side
  convenience wrapper for the common case of reading a fixed-length
  header from many files.

## Performance

Verified against `sentinel-cogs` (AWS us-west-2) and a Pawsey S3
bucket:

* 63-key × 64KB byte-range scatter: **19.3s sequential → 0.77s at
  concurrency=32** (25× speedup).
* 552,860-key bucket listing fanned out across 12 year-prefixes:
  **43s serial → 8.5s parallel** (5× speedup).
* Listed 543,889 keys from a single `sentinel-cogs` prefix in one call
  without issue.

## Build

* extendr-api 0.8, object_store 0.13 (aws feature), tokio
  multi-threaded runtime via `once_cell::sync::Lazy`.
* `futures::stream::buffer_unordered` for bounded concurrency on
  fan-out operations.

## Known limitations

* Only S3 (and S3-compatible) cloud backends are wired up so far.
  Google Cloud Storage, Azure Blob Storage, and generic HTTP backends
  are planned.
* Rust dependencies are not yet vendored; CRAN submission will need
  `rextendr::vendor_pkgs()`.
* `store_list_many()` does not preserve input order.