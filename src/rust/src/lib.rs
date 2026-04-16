// robstore — R bindings to the Rust object_store crate.
//
// Phase 2: tokio runtime + S3 (credentialed + anonymous).
//
// A single process-wide multi-threaded tokio runtime drives all async
// object_store calls. This works for LocalFileSystem and InMemory too
// (they don't actually do I/O on the reactor), so we use the same path
// for every store type.

use extendr_api::prelude::*;
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::runtime::Runtime;

// ---------------------------------------------------------------------------
// Shared tokio runtime
// ---------------------------------------------------------------------------

static RT: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("robstore-tokio")
        .build()
        .expect("failed to build tokio runtime")
});

// ---------------------------------------------------------------------------
// Store struct — thin wrapper around Arc<dyn ObjectStore>
// ---------------------------------------------------------------------------

/// @export
#[extendr]
#[derive(Clone)]
struct Store {
    inner: Arc<dyn ObjectStore>,
    label: String,
}

#[extendr]
impl Store {
    /// Describe the store (used by print.Store in R).
    fn describe(&self) -> String {
        self.label.clone()
    }
}

fn to_path(key: &str) -> Result<ObjectPath> {
    ObjectPath::parse(key)
        .map_err(|e| Error::Other(format!("invalid object path {key:?}: {e}")))
}

fn os_err(e: object_store::Error) -> Error {
    Error::Other(format!("object_store error: {e}"))
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

/// Create an in-memory store (useful for testing).
/// @return A `Store` object.
/// @export
#[extendr]
fn memory_store() -> Store {
    Store {
        inner: Arc::new(InMemory::new()),
        label: "MemoryStore".into(),
    }
}

/// Create a local filesystem store rooted at `path`.
///
/// All keys passed to `put`/`get`/`list` are interpreted relative to `path`.
/// The directory must already exist.
///
/// @param path Absolute or relative path to a directory.
/// @return A `Store` object.
/// @export
#[extendr]
fn local_store(path: &str) -> Result<Store> {
    let fs = LocalFileSystem::new_with_prefix(path)
        .map_err(|e| Error::Other(format!("failed to open local store at {path:?}: {e}")))?;
    Ok(Store {
        inner: Arc::new(fs),
        label: format!("LocalStore({path})"),
    })
}

/// Create an S3 store for a credentialed bucket.
///
/// Credentials are resolved from environment variables (AWS_ACCESS_KEY_ID,
/// AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_REGION) and other standard
/// sources via the object_store crate's AmazonS3Builder.
///
/// @param bucket Bucket name.
/// @param region AWS region (required for real AWS; use the Pawsey region
///   string for Pawsey buckets).
/// @param endpoint Optional custom endpoint URL (for S3-compatible services
///   like Pawsey, MinIO, Backblaze). Pass `NULL` for AWS S3.
/// @param allow_http Allow plain-HTTP endpoints (for local MinIO etc).
///   Defaults to `FALSE`.
/// @return A `Store` object.
/// @export
#[extendr]
fn s3_store(
    bucket: &str,
    region: &str,
    endpoint: Nullable<&str>,
    allow_http: bool,
) -> Result<Store> {
    let mut b = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_allow_http(allow_http);
    if let Nullable::NotNull(ep) = endpoint {
        b = b.with_endpoint(ep);
    }
    let s3 = b
        .build()
        .map_err(|e| Error::Other(format!("failed to build S3 store: {e}")))?;
    let label = match endpoint {
        Nullable::NotNull(ep) => format!("S3Store({bucket} @ {ep})"),
        Nullable::Null => format!("S3Store({bucket} @ {region})"),
    };
    Ok(Store {
        inner: Arc::new(s3),
        label,
    })
}

/// Create an S3 store with unsigned (anonymous) requests for public buckets.
///
/// This uses `with_skip_signature(true)` on AmazonS3Builder — equivalent to
/// `AWS_NO_SIGN_REQUEST=YES` in GDAL's /vsis3/, or `anonymous=True` in
/// Python obstore / fsspec.
///
/// @param bucket Bucket name (e.g. `"sentinel-cogs"`).
/// @param region AWS region (e.g. `"us-west-2"`).
/// @param endpoint Optional custom endpoint URL. Pass `NULL` for AWS S3.
/// @param allow_http Allow plain-HTTP endpoints. Defaults to `FALSE`.
/// @return A `Store` object.
/// @export
#[extendr]
fn s3_store_anonymous(
    bucket: &str,
    region: &str,
    endpoint: Nullable<&str>,
    allow_http: bool,
) -> Result<Store> {
    let mut b = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_skip_signature(true)
        .with_allow_http(allow_http);
    if let Nullable::NotNull(ep) = endpoint {
        b = b.with_endpoint(ep);
    }
    let s3 = b
        .build()
        .map_err(|e| Error::Other(format!("failed to build anonymous S3 store: {e}")))?;
    let label = match endpoint {
        Nullable::NotNull(ep) => format!("S3Store[anon]({bucket} @ {ep})"),
        Nullable::Null => format!("S3Store[anon]({bucket} @ {region})"),
    };
    Ok(Store {
        inner: Arc::new(s3),
        label,
    })
}

// ---------------------------------------------------------------------------
// Core operations — all driven by the shared tokio runtime
// ---------------------------------------------------------------------------

/// Write raw bytes to `key` in `store`.
///
/// @param store A `Store` object.
/// @param key Object key (path) as a string.
/// @param data A raw vector.
/// @return `NULL` invisibly on success.
/// @export
#[extendr]
fn store_put(store: &Store, key: &str, data: Raw) -> Result<()> {
    let path = to_path(key)?;
    let payload = PutPayload::from_bytes(bytes::Bytes::copy_from_slice(data.as_slice()));
    RT.block_on(store.inner.put(&path, payload)).map_err(os_err)?;
    Ok(())
}

/// Read an entire object as a raw vector.
///
/// @param store A `Store` object.
/// @param key Object key (path).
/// @return A raw vector containing the object's bytes.
/// @export
#[extendr]
fn store_get(store: &Store, key: &str) -> Result<Raw> {
    let path = to_path(key)?;
    let bytes = RT
        .block_on(async {
            let got = store.inner.get(&path).await?;
            got.bytes().await
        })
        .map_err(|e: object_store::Error| os_err(e))?;
    Ok(Raw::from_bytes(&bytes))
}

/// Read a byte range from an object.
///
/// @param store A `Store` object.
/// @param key Object key (path).
/// @param offset 0-based start offset (double, coerced to u64).
/// @param length Number of bytes to read.
/// @return A raw vector of length `length`.
/// @export
#[extendr]
fn store_get_range(store: &Store, key: &str, offset: f64, length: f64) -> Result<Raw> {
    let path = to_path(key)?;
    if offset < 0.0 || length < 0.0 {
        return Err(Error::Other("offset and length must be non-negative".into()));
    }
    let start = offset as u64;
    let end = start + length as u64;
    let bytes = RT
        .block_on(store.inner.get_range(&path, start..end))
        .map_err(os_err)?;
    Ok(Raw::from_bytes(&bytes))
}

/// Delete an object.
///
/// @param store A `Store` object.
/// @param key Object key (path).
/// @return `NULL` invisibly on success.
/// @export
#[extendr]
fn store_delete(store: &Store, key: &str) -> Result<()> {
    let path = to_path(key)?;
    RT.block_on(store.inner.delete(&path)).map_err(os_err)?;
    Ok(())
}

/// Check whether an object exists.
///
/// @param store A `Store` object.
/// @param key Object key (path).
/// @return `TRUE` or `FALSE`.
/// @export
#[extendr]
fn store_exists(store: &Store, key: &str) -> Result<bool> {
    let path = to_path(key)?;
    match RT.block_on(store.inner.head(&path)) {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(e) => Err(os_err(e)),
    }
}

/// List object keys under an optional prefix.
///
/// Pass a `prefix` when listing cloud buckets — most public buckets contain
/// millions of objects and an unbounded list will take a long time and use
/// a lot of memory.
///
/// @param store A `Store` object.
/// @param prefix Optional prefix (string or `NULL`).
/// @return A character vector of object keys.
/// @export
#[extendr]
fn store_list(store: &Store, prefix: Nullable<&str>) -> Result<Vec<String>> {
    let prefix_path = match prefix {
        Nullable::NotNull(p) => Some(to_path(p)?),
        Nullable::Null => None,
    };
    let keys: Vec<String> = RT
        .block_on(async {
            store
                .inner
                .list(prefix_path.as_ref())
                .map_ok(|meta| meta.location.to_string())
                .try_collect::<Vec<String>>()
                .await
        })
        .map_err(os_err)?;
    Ok(keys)
}

/// Copy an object from `from` to `to` within the same store.
///
/// @param store A `Store` object.
/// @param from Source key.
/// @param to Destination key.
/// @return `NULL` invisibly on success.
/// @export
#[extendr]
fn store_copy(store: &Store, from: &str, to: &str) -> Result<()> {
    let from_path = to_path(from)?;
    let to_path_ = to_path(to)?;
    RT.block_on(store.inner.copy(&from_path, &to_path_))
        .map_err(os_err)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Concurrent fan-out operations
// ---------------------------------------------------------------------------

/// Read many objects concurrently.
///
/// Issues up to `concurrency` GET requests at once via the shared tokio
/// runtime. Returns a list of raw vectors, one per input key, in input order.
/// Any failure aborts the whole operation and returns an error.
///
/// @param store A `Store` object.
/// @param keys Character vector of object keys.
/// @param concurrency Maximum number of concurrent requests.
/// @return A list of raw vectors, length `length(keys)`.
/// @export
#[extendr]
fn store_get_many(
    store: &Store,
    keys: Vec<String>,
    concurrency: i32,
) -> Result<List> {
    if concurrency < 1 {
        return Err(Error::Other("concurrency must be >= 1".into()));
    }
    let conc = concurrency as usize;
    let inner = store.inner.clone();

    // Pre-validate paths so we fail fast without issuing any requests.
    let paths: Vec<ObjectPath> = keys
        .iter()
        .map(|k| to_path(k))
        .collect::<Result<Vec<_>>>()?;

    // Do all the async I/O first, collecting raw Bytes (no R allocations).
    let bytes_vec: Vec<bytes::Bytes> = RT
        .block_on(async {
            stream::iter(paths.into_iter().enumerate())
                .map(|(i, path)| {
                    let inner = inner.clone();
                    async move {
                        let got = inner.get(&path).await?;
                        let b = got.bytes().await?;
                        Ok::<(usize, bytes::Bytes), object_store::Error>((i, b))
                    }
                })
                .buffer_unordered(conc)
                .try_collect::<Vec<(usize, bytes::Bytes)>>()
                .await
                .map(|mut v| {
                    v.sort_by_key(|(i, _)| *i);
                    v.into_iter().map(|(_, b)| b).collect::<Vec<_>>()
                })
        })
        .map_err(os_err)?;

    // Now convert to R. Build Vec<Robj> first so each allocation is wrapped
    // in an Robj (which extendr protects), then feed to List::from_values.
    let robjs: Vec<Robj> = bytes_vec
        .iter()
        .map(|b| Raw::from_bytes(b).into_robj())
        .collect();
    Ok(List::from_values(robjs))
}

/// Read many byte ranges concurrently.
///
/// Each of the three input vectors `keys`, `offsets`, `lengths` must have
/// the same length. Issues up to `concurrency` range-GET requests at once.
/// Returns a list of raw vectors in input order. Ranges can target the
/// same key or different keys — this is the primitive for concurrent
/// COG IFD walks, Zarr chunk reads, Parquet row-group reads.
///
/// @param store A `Store` object.
/// @param keys Character vector of object keys.
/// @param offsets Numeric vector of 0-based byte offsets (same length).
/// @param lengths Numeric vector of byte lengths (same length).
/// @param concurrency Maximum number of concurrent requests.
/// @return A list of raw vectors.
/// @export
#[extendr]
fn store_get_ranges_many(
    store: &Store,
    keys: Vec<String>,
    offsets: Vec<f64>,
    lengths: Vec<f64>,
    concurrency: i32,
) -> Result<List> {
    if keys.len() != offsets.len() || keys.len() != lengths.len() {
        return Err(Error::Other(
            "keys, offsets, lengths must all have the same length".into(),
        ));
    }
    if concurrency < 1 {
        return Err(Error::Other("concurrency must be >= 1".into()));
    }
    let conc = concurrency as usize;
    let inner = store.inner.clone();

    let mut jobs: Vec<(usize, ObjectPath, u64, u64)> = Vec::with_capacity(keys.len());
    for (i, ((k, off), len)) in keys
        .iter()
        .zip(offsets.iter())
        .zip(lengths.iter())
        .enumerate()
    {
        if *off < 0.0 || *len < 0.0 {
            return Err(Error::Other(format!(
                "element {i}: offset and length must be non-negative"
            )));
        }
        let p = to_path(k)?;
        let start = *off as u64;
        let end = start + (*len as u64);
        jobs.push((i, p, start, end));
    }

    let bytes_vec: Vec<bytes::Bytes> = RT
        .block_on(async {
            stream::iter(jobs)
                .map(|(i, path, start, end)| {
                    let inner = inner.clone();
                    async move {
                        let b = inner.get_range(&path, start..end).await?;
                        Ok::<(usize, bytes::Bytes), object_store::Error>((i, b))
                    }
                })
                .buffer_unordered(conc)
                .try_collect::<Vec<(usize, bytes::Bytes)>>()
                .await
                .map(|mut v| {
                    v.sort_by_key(|(i, _)| *i);
                    v.into_iter().map(|(_, b)| b).collect::<Vec<_>>()
                })
        })
        .map_err(os_err)?;

    let robjs: Vec<Robj> = bytes_vec
        .iter()
        .map(|b| Raw::from_bytes(b).into_robj())
        .collect();
    Ok(List::from_values(robjs))
}

/// List object keys under many prefixes concurrently.
///
/// Fires up to `concurrency` independent `list()` calls in parallel — one
/// per prefix — and flattens the results into a single character vector.
/// Useful for hierarchical layouts where you know the top-level directory
/// names a priori (years, MGRS tiles, etc.) and want to parallelise
/// across them instead of walking a single giant paginated listing.
///
/// @param store A `Store` object.
/// @param prefixes Character vector of prefixes.
/// @param concurrency Maximum number of concurrent list calls.
/// @return A character vector of all keys across all prefixes, in no
///   guaranteed order.
/// @export
#[extendr]
fn store_list_many(
    store: &Store,
    prefixes: Vec<String>,
    concurrency: i32,
) -> Result<Vec<String>> {
    if concurrency < 1 {
        return Err(Error::Other("concurrency must be >= 1".into()));
    }
    let conc = concurrency as usize;
    let inner = store.inner.clone();

    // Validate all prefixes up front
    let paths: Vec<ObjectPath> = prefixes
        .iter()
        .map(|p| to_path(p))
        .collect::<Result<Vec<_>>>()?;

    let all_keys: Vec<String> = RT
        .block_on(async {
            stream::iter(paths)
                .map(|path| {
                    let inner = inner.clone();
                    async move {
                        let keys: Vec<String> = inner
                            .list(Some(&path))
                            .map_ok(|meta| meta.location.to_string())
                            .try_collect::<Vec<String>>()
                            .await?;
                        Ok::<Vec<String>, object_store::Error>(keys)
                    }
                })
                .buffer_unordered(conc)
                .try_collect::<Vec<Vec<String>>>()
                .await
                .map(|vs| vs.into_iter().flatten().collect())
        })
        .map_err(os_err)?;

    Ok(all_keys)
}

// ---------------------------------------------------------------------------
// Module registration
// ---------------------------------------------------------------------------

extendr_module! {
    mod robstore;
    impl Store;
    fn memory_store;
    fn local_store;
    fn s3_store;
    fn s3_store_anonymous;
    fn store_put;
    fn store_get;
    fn store_get_range;
    fn store_delete;
    fn store_exists;
    fn store_list;
    fn store_list_many;
    fn store_copy;
    fn store_get_many;
    fn store_get_ranges_many;
}