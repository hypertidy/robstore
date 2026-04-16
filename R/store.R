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