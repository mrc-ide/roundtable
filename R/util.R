sys_getenv <- function(name) {
  value <- Sys.getenv(name, NA_character_)
  if (is.na(value)) {
    stop(sprintf("Environment variable '%s' is not set", name))
  }
  value
}

drop_null <- function(x) {
  x[!vapply(x, is.null, TRUE)]
}

vlapply <- function(x, fun, ...) {
  vapply(x, fun, logical(1), ...)
}


write_lines_to_sharepoint <- function(text, path, folder) {
  tmp <- tempfile()
  on.exit(unlink(tmp))
  writeLines(text, tmp)
  folder$upload(tmp, path)
}
