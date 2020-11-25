##' Prepare a rtm run for use on the cluster. This should be run on
##' the main server, from master. Once run, note the key, which you
##' will want for use with [roundtable_rtm_run()] on the cluster.
##'
##' @title Prepare a rtm run for use on the cluster.
##'
##' @param name The name of the task (see [orderly::orderly_list()])
##'
##' @param test_run Run a short test run on the server before export,
##'   to validate that the data and code play well together.
##'
##' @param short_run On the cluster, should this be a short run? Set
##'   this to `TRUE` only for debugging.
##'
##' @param date The date to run
##'
##' @param chains The number of chains to run. Note that this
##'   duplicates the default in the underlying task so be aware of
##'   updating either of these numbers.
##'
##' @return The key
##' @export
roundtable_rtm_prepare <- function(name, test_run = FALSE, short_run = FALSE,
                                   date = NULL, chains = 16L) {
  folder <- sharepoint_folder()
  key <- roundtable_key()
  res <- roundtable_rtm_pack(name, test_run, short_run, date, chains)
  roundtable_incoming_upload(res$tasks, key, folder)
  roundtable_metadata_upload(res$metadata, key, folder)
}








##' Debug a cluster run. Must be run on the cluster.
##'
##' @title Debug a cluster run
##'
##' @param key Key used when starting the tasks with [roundtable_rtm_run()]
##'
##' @return A "task_bundle" object. Most likely you'll want to extract
##'   individual tasks (`t <- grp$tasks[[1]]`) and look at their
##'   errors (`t$result()` and `t$result()$trace`) and logs
##'   (`t$log()`).
##'
##' @export
roundtable_rtm_debug <- function(key) {
  folder <- sharepoint_folder()
  incoming <- roundtable_incoming_download(key, folder)
  if (!incoming$is_running) {
    stop("This key has not been started")
  }
  obj <- prepare_cluster(incoming$metadata, initialise = FALSE)
  obj$task_bundle_get(incoming$id)
}


rtm_cluster_info <- function(key) {
  folder <- sharepoint_folder()
  incoming <- roundtable_incoming_download(key, folder)
  id <- readLines(incoming$folder$download("running"))
  obj <- prepare_cluster(FALSE)
  obj$task_bundle_get(id)
}



roundtable_upload_results <- function(files, key, metadata, workdir, folder) {
  dest <- folder$create(file.path("results", key))
  for (f in files) {
    message(sprintf("Uploading '%s'", basename(f)))
    dest$upload(f, progress = TRUE)
  }
  message("Uploading metadata")
  tmp <- tempfile()
  saveRDS(metadata, tmp)
  dest$upload(tmp, "metadata.rds", progress = TRUE)

  # Write a file called 'finished' to indicate we're all done.
  tmp <- tempfile()
  on.exit(unlink(tmp))
  writeLines("", tmp)
  dest$upload(tmp, dest = "finished")
}


rtm_regions <- function() {
  c("east_of_england",
    "midlands",
    "london",
    "north_east_and_yorkshire",
    "north_west",
    "south_east",
    "south_west",
    "scotland",
    "wales",
    "northern_ireland")
}


roundtable_rtm_pack <- function(name, test_run, short_run, date, chains) {
  regions <- rtm_regions()
  if (is.null(date)) {
    date <- as.character(Sys.Date())
  }
  parameters <- lapply(regions, function(x)
    list(date = date,
         chains = chains,
         region = x,
         short_run = short_run,
         kernel_scaling = 0.2)) # should this be tuneable?

  if (test_run) {
    message("Trying a short run, as requested")
    ## Do some sort of test run here - if this fails, nothing proceeds
    orderly::orderly_run(name,
                         parameters = list(date = date,
                                           chains = 2,
                                           short_run = TRUE))
  }

  path <- tempfile()
  tasks <- lapply(parameters, function(p)
    orderly::orderly_bundle_pack(path, name, p))

  p <- file.path(orderly::orderly_config()$root, "src", name, "orderly.yml")
  packages <- yaml::read_yaml(p)$packages
  versions <- lapply(packages, packageVersion)
  names(versions) <- packages
  names(parameters) <- vapply(tasks, "[[", "", "id")
  metadata <- list(name = name,
                   parameters = parameters,
                   packages = versions)

  list(tasks = tasks, metadata = metadata)
}
