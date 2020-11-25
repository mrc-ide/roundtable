##' Run a prepared roundtable job on the cluster. Requires a key as
##' created by [roundtable_rtm_prepare()]. Run this on the cluster,
##' from a directory that is *not* the checked out copy of the orderly
##' sources, as this will not interact with it.
##'
##' @title Run roundtable job on cluster
##'
##' @param key The job key
##'
##' @param test_job Send a test job to the cluster to ensure that
##'   everything works.
##'
##' @return
##' @author Richard Fitzjohn
roundtable_run <- function(key, test_job = FALSE, upgrade = FALSE,
                           rerun = FALSE) {
  folder <- sharepoint_folder()

  incoming <- roundtable_incoming_download(key, folder)

  if (incoming$status$finished) {
    stop(sprintf("Key '%s' has already been run", key))
  }

  obj <- prepare_cluster(incoming$metadata, initialise = TRUE)
  if (test_job) {
    t <- obj$enqueue(packageVersion("sircovid"))
    t$wait(timeout = 100)
  }

  if (incoming$status$running && !rerun) {
    message(sprintf("Already running '%s' as '%s'", key, id))
    id <- incoming$id
    grp <- obj$task_bundle_get(id)
    workdir <- file.path("working", id)
  } else {
    id <- ids::adjective_animal(style = "kebab")
    message(sprintf("Running '%s' as '%s'", key, id))

    ## Then the ids that need running
    workdir <- file.path("working", id)
    grp <- obj$lapply(incoming$bundles, function(p, workdir)
      orderly::orderly_bundle_run(p, workdir), workdir = workdir,
      timeout = 0, name = id)

    tmp <- tempfile()
    writeLines(id, tmp)
    incoming$folder$upload(tmp, file.path("metadata", key, running))
  }

  results <- grp$wait(timeout = Inf)
  status <- grp$status()
  message("Results:")
  print(table(status))

  if (any(status == "ERROR")) {
    message("Some tasks failed! The log for the first failed task is below")
    print(grp$tasks[[which(status == "ERROR")[[1]]]]$log())
    message("To investigate further, run")
    message(sprintf('grp <- roundtable::roundtable_debug("%s")', key))
    message("And work with the 'grp' task bundle object")
    stop("Stopping as task failed")
  }

  files <- sprintf("%s/%s.zip", workdir, vapply(results, "[[", "", "id"))
  roundtable_upload_results(files, key, incoming$metadata, workdir, folder)

  message("Import this job on the server by running")
  message(sprintf("  ./roundtable import %s", key))
}


##' Import a roundtable job back into orderly.
##' @title Import a roundtable job into orderly
##'
##' @param key The key created by [roundtable_rtm_prepare()] and run
##'   with [roundtable_rtm_run()]
##'
##' @export
roundtable_import <- function(key) {
  folder <- sharepoint_folder()

  browser()

  results <- folder$folder(file.path("results", key), verify = TRUE)
  contents <- results$list()
  if (!("finished" %in% contents$name)) {
    stop("Results are not finished")
  }

  re <- "^[0-9]{8}-[0-9]{6}-[[:xdigit:]]{8}\\.zip$"
  ids <- grep(re, contents$name, value = TRUE)

  tmp <- tempfile()
  dir.create(tmp, FALSE, TRUE)
  for (f in contents$name) {
    message(sprintf("Downloading '%s'", f))
    results$download(f, file.path(tmp, f), progress = TRUE)
  }

  metadata <- readRDS(file.path(tmp, "metadata.rds"))

  for (p in ids) {
    orderly::orderly_bundle_import(file.path(tmp, p))
  }

  unlink(tmp, recursive = TRUE)

  message("Running the combined task")
  ## We need to run the combined task here too. Getting this "Right"
  ## would be much easier after we implement the transactional
  ## workflow approach so that we might see the state of the orderly
  ## archive as a log. For now, we assume that that combined task will
  ## be ok to run.
  name_combined <- paste0(metadata$name, "_combined")
  ## This section might need tweaking for different tasks:
  keep <- c("date", "short_run", "kernel_scaling")
  parameters <- metadata$parameters[[1]][keep]
  orderly:::orderly_run_internal(name_combined, parameters = parameters,
                                 commit = TRUE)

  tmp <- tempfile()
  writeLines("", tmp)
  results$upload(tmp, "imported")

  message(sprintf("Finished processing '%s' (%s)", key, metadata$name))
  ## TODO: This will be fixed once I update spud.
  message("Please delete the incoming and results directories on sharepoint")
}


##' Get list of roundtable job keys
##'
##' @title List roundtable jobs
##' @export
roundtable_list <- function() {
  message("Querying sharepoint; this may take a second...")
  folder <- sharepoint_folder()
  metadata <- folder$folder("metadata", verify = TRUE)

  dat <- metadata$folders()
  dat <- dat[order(dat$created), ]
  contents <- lapply(dat$name, metadata$files)

  has_file <- function(contents, nm) {
    nm %in% contents$name
  }

  tibble::tibble(
    name = dat$name,
    running = vlapply(contents, has_file, "running"),
    finished = vlapply(contents, has_file, "finished"),
    imported = vlapply(contents, has_file, "imported"),
    created = dat$created)
}


##' Print summary status for a single key
##'
##' @title Summary status
##'
##' @param key A key, as created by [roundtable_rtm_prepare()]
##'
##' @export
##'
##' @return Primarily called for its side effect, though some data is
##'   returned in a list.
roundtable_status <- function(key) {
  meta <- roundtable_metadata(key, sharepoint_folder())

  res <- list(name = meta$key, status = meta$status)

  cli::cli_alert(key)
  for (i in names(res$status)) {
    if (res$status[[i]]) {
      cli::cli_alert_success(i)
    } else {
      cli::cli_alert_danger(i)
    }
  }

  invisible(res)
}


roundtable_key <- function() {
  sprintf("%s-%s", format(Sys.Date(), "%Y%m%d"),
          ids::adjective_animal(1, style = "kebab"))
}


roundtable_incoming_upload <- function(tasks, key, folder) {
  ## This fraction will end up going via OrderlyWeb
  message(sprintf("Uploading %d items to sharepoint with key '%s'",
                  length(tasks), key))
  dest <- folder$create(file.path("incoming", key))
  for (x in tasks) {
    message(sprintf("Uploading '%s'", basename(x$path)))
    dest$upload(x$path, progress = TRUE)
  }
}


roundtable_metadata_upload <- function(metadata, key, folder) {
  tmp <- tempfile()
  saveRDS(metadata, tmp)
  message("Uploading metadata")
  dest <- folder$create(file.path("metadata", key))
  dest$upload(tmp, "metadata.rds", progress = TRUE)

  message("Start this job on the cluster by running")
  message(sprintf("  ./roundtable run %s", key))

  invisible(key)
}


roundtable_incoming_download <- function(key, folder) {
  metadata <- roundtable_metadata(key, folder)

  src <- folder$folder(file.path("incoming", key), verify = TRUE)
  contents <- src$files()

  dest <- file.path("incoming", key)
  dir.create(dest, FALSE, TRUE)

  message(sprintf("Fetching bundles for '%s'", key))
  for (p in contents$name) {
    dest_p <- file.path(dest, p)
    if (!file.exists(dest_p)) {
      message(sprintf("Downloading '%s'", dest_p))
      src$download(p, dest_p)
    }
  }

  bundles <- dir(dest, pattern = "^[0-9]{8}-[0-9]{6}-[[:xdigit:]]{8}\\.zip$",
                 full.names = TRUE)

  metadata$bundles <- bundles
  metadata
}


roundtable_metadata <- function(key, folder) {
  metadata <- folder$folder("metadata", verify = TRUE)
  contents <- metadata$folders()
  if (!(key %in% contents$name)) {
    stop(sprintf("Unknown key '%s'; valid options:\n%s",
                 key, paste("  -", contents$name, collapse = "\n")))
  }

  files <- metadata$folder(key)$files()
  path_metadata <- file.path("metadata", key)
  if (!file.exists(path_metadata)) {
    message(sprintf("Fetching metadata for '%s'", key))
    dir.create(dirname(path_metadata), FALSE, TRUE)
    metadata$download(file.path(key, "metadata.rds"),
                      path_metadata)
  }

  ret <- readRDS(path_metadata)
  ret$status <- list(
    running = "running" %in% files$name,
    finished = "finished" %in% files$name,
    imported = "imported" %in% files$name)

  ## We will always want this
  ret$key <- key

  ## Save on connection time later:
  ret$folder <- folder

  if (ret$status$running) {
    ## Read the id here
    browser()
    tmp <- tempfile()
    metadata$download("running", tmp, progress = FALSE)
    ret$status$id <- readLines(tmp)
  }

  ret
}
