usage <- "Usage:
  roundtable list
  roundtable prepare [options] <name>
  roundtable run [--test-job] <key>
  roundtable status <key>
  roundtable import <key>

Options:
--short-run  Only do a short run for the main chains (debug only!)
--test-run   Run a test run on the server before exporting for the cluster
--date=DATE  Date to run; if not given, defaults to today
--chains=N   Number of chains to run"

main <- function(args = commandArgs(TRUE)) {
  dat <- main_parse(args)
  do.call(dat$target, dat$args)
}


main_parse <- function(args) {
  dat <- docopt::docopt(usage, args)
  names(dat) <- gsub("-", "_", names(dat)) # compatibility

  if (dat$list) {
    mode <- "list"
    target <- roundtable_rtm_list
    args <- list()
  } else if (dat$prepare) {
    mode <- "prepare"
    target <- roundtable_rtm_prepare
    args <- dat[c("short_run", "test_run", "date", "chains", "name")]
  } else if (dat$run) {
    mode <- "status"
    target <- roundtable_rtm_run
    args <- dat[c("test_job", "key")]
  } else if (dat$status) {
    mode <- "status"
    target <- roundtable_rtm_status
    args <- dat["key"]
  } else if (dat$import) {
    mode <- "import"
    target <- roundtable_rtm_import
    args <- dat["key"]
  }

  list(mode = mode, target = target, args = args)
}


write_script <- function(path) {
  dir.create(path, FALSE, TRUE)
  rscript <- "#!/usr/bin/env Rscript"
  code <- c(rscript, "roundtable:::main()")
  path_bin <- file.path(path, "roundtable")
  writeLines(code, path_bin)
  Sys.chmod(path_bin, "755")
  invisible(path_bin)
}
