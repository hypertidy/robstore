
#' @export
#' @method format Store
format.Store <- function(x, ...) {
  paste0("<", x$describe(), ">")
}

#' @export
#' @method print Store
print.Store <- function(x, ...) {
  cat(format(x), "\n", sep = "")
  invisible(x)
}

#' Read the same byte range from many objects concurrently
#'
#' Convenience wrapper around [store_get_ranges_many()] for the common case
#' of fetching the first N bytes of each file in a batch — e.g. COG headers,
#' the start of a Parquet file, or a fixed Zarr chunk slot.
#'
#' @param store A `Store` object.
#' @param keys Character vector of object keys.
#' @param offset Single numeric offset applied to every key (default `0`).
#' @param length Single numeric length applied to every key.
#' @param concurrency Maximum number of concurrent requests.
#' @return A list of raw vectors in input order.
#' @export
store_head_bytes <- function(store, keys, length, offset = 0, concurrency = 16) {
  stopifnot(inherits(store, "Store"))
  n <- length(keys)
  store_get_ranges_many(
    store,
    keys    = keys,
    offsets = rep(offset, n),
    lengths = rep(length, n),
    concurrency = concurrency
  )
}