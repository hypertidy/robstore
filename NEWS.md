# robstore 0.1.0

First release. R bindings to the Rust
[`object_store`](https://crates.io/crates/object_store) crate via
[extendr](https://extendr.rs/), with a uniform API across local
filesystems, AWS S3, S3-compatible endpoints, and Google Cloud Storage.

## Backends

* `memory_store()` — in-memory store.
* `local_store(path)` — local filesystem rooted at `path`.
* `s3_store(bucket, region, endpoint, allow_http)` — AWS S3 or any
  S3-compatible service (Pawsey, MinIO, etc.). Credentials resolved
  from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment
  variables.
* `s3_store_anonymous(bucket, region, endpoint, allow_http)` — public
  S3 buckets with unsigned requests (e.g. `sentinel-cogs`).
* `gcs_store(bucket)` — Google Cloud Storage. Credentials resolved
  from `GOOGLE_APPLICATION_CREDENTIALS` / Application Default
  Credentials.
* `gcs_store_anonymous(bucket)` — public GCS buckets with unsigned
  requests (e.g. `gcp-public-data-arco-era5`, `gcp-public-data-landsat`).
* `azure_store(account, container)` — Azure Blob Storage with
  credentials from env vars (AZURE_STORAGE_ACCOUNT_NAME /
  AZURE_STORAGE_ACCOUNT_KEY / AZURE_STORAGE_SAS_KEY).
* `azure_store_sas(account, container, sas_token)` — SAS-token
  authenticated access; the typical pattern for Microsoft
  Planetary Computer data (free SAS endpoint at
  https://planetarycomputer.microsoft.com/api/sas/v1/token/...).
* `azure_store_anonymous(account, container)` — unsigned access.
  Note that Azure generally does not permit anonymous listing
  even on public containers.
  
## Core operations

* `store_put()`, `store_get()`, `store_get_range()` — write, read,
  byte-range read.
* `store_exists()`, `store_delete()`, `store_copy()` — metadata and
  single-object operations.
* `store_list(store, prefix)` — recursive list of keys under a prefix.
* `store_list_delimited(store, prefix)` — efficient one-level
  hierarchical listing, returning immediate keys and common prefixes.
  Suitable for exploring bucket structure without enumerating trees.

## Concurrent operations

All run on a shared multi-threaded tokio runtime; concurrency is
bounded by the user via a `concurrency` argument.

* `store_get_many(store, keys, concurrency)` — concurrent full-object
  reads.
* `store_get_ranges_many(store, keys, offsets, lengths, concurrency)` —
  concurrent byte-range reads. The primitive for COG IFD walks, Zarr
  chunk scatter-reads, and Parquet row-group reads.
* `store_list_many(store, prefixes, concurrency)` — concurrent listings
  across many prefixes, for hierarchical layouts (years, MGRS tiles,
  product types).
* `store_head_bytes(store, keys, length, offset, concurrency)` — R-side
  convenience wrapper for the common case of reading a fixed-length
  header from many files.

## Performance

Verified against `sentinel-cogs` (AWS us-west-2), Pawsey `estinel`, and
`gcp-public-data-arco-era5` (GCS):

* 63-key × 64 KB byte-range scatter on AWS: **19.3 s sequential →
  0.77 s at concurrency = 32** (25× speedup).
* 552,860-key bucket listing on Pawsey fanned out across 12
  year-prefixes: **43 s serial → 8.5 s parallel** (5× speedup).
* End-to-end catalog build from a production Pawsey bucket
  (552,860 objects → 65,734 catalog entries across 135 locations,
  10-year date range): **11 s**, including all R-side parsing and
  aggregation.
* Listed 543,889 keys from a single `sentinel-cogs` prefix in one call
  without issue.

## Build

* `extendr-api` 0.8, `object_store` 0.13 with `aws` and `gcp`
  features.
* Shared multi-threaded tokio runtime via `once_cell::sync::Lazy`.
* `futures::stream::buffer_unordered` for bounded concurrency on
  fan-out operations.

## Known limitations

* Azure Blob Storage and generic HTTP backends are not yet wired up.
* Rust dependencies are not yet vendored; CRAN submission will need
  `rextendr::vendor_pkgs()`.
* `store_list_many()` does not preserve input order.
* `store_list_delimited()` always uses `/` as the delimiter.