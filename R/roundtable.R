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
##' @param rerun Re-queue/rerun jobs even if they have been
##'   completed. Rewrites the `id` on sharepoint.
##'
##' @author Richard Fitzjohn
roundtable_run <- function(key, test_job = FALSE, rerun = FALSE) {
  folder <- sharepoint_folder()
  incoming <- roundtable_incoming_download(key, folder)

  if (incoming$status$finished) {
    stop(sprintf("Key '%s' has already been run", key))
  }

  obj <- prepare_cluster(names(incoming$packages), initialise = TRUE)
  if (test_job) {
    t <- obj$enqueue(utils::packageVersion("sircovid"))
    res <- t$wait(timeout = 100)
    if (t$status() == "ERROR") {
      stop("Error running test job: ", res$message)
    }
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

    write_lines_to_sharepoint(id, file.path("metadata", key, "running"),
                              incoming$folder)
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


##' Debug a cluster run. Must be run on the cluster.
##'
##' @title Debug a cluster run
##'
##' @param key Key used when starting the tasks with [roundtable_run()]
##'
##' @return A "task_bundle" object. Most likely you'll want to extract
##'   individual tasks (`t <- grp$tasks[[1]]`) and look at their
##'   errors (`t$result()` and `t$result()$trace`) and logs
##'   (`t$log()`).
##'
##' @export
roundtable_debug <- function(key) {
  folder <- sharepoint_folder()
  incoming <- roundtable_incoming_download(key, folder)
  if (!incoming$status$finished) {
    stop("This key has not been started")
  }
  obj <- prepare_cluster(names(incoming$packages), initialise = FALSE)
  obj$task_bundle_get(incoming$id)
}


##' Import a roundtable job back into orderly.
##' @title Import a roundtable job into orderly
##'
##' @param key The key created by [roundtable_rtm_prepare()] and run
##'   with [roundtable_run()]
##'
##' @export
roundtable_import <- function(key) {
  folder <- sharepoint_folder()
  meta <- roundtable_metadata(key, folder)

  if (!meta$status$finished) {
    stop("Results are not finished")
  }
  if (meta$status$imported) {
    stop("Results have already been imported")
  }

  results <- folder$folder(file.path("results", key), verify = TRUE)
  ids <- names(meta$parameters)

  tmp <- tempfile()
  on.exit(unlink(tmp, recursive = TRUE))
  dir.create(tmp, FALSE, TRUE)
  for (i in ids) {
    message(sprintf("Downloading '%s'", i))
    results$download(paste0(i, ".zip"), file.path(tmp, i), progress = TRUE)
  }

  for (i in ids) {
    orderly::orderly_bundle_import(file.path(tmp, i))
  }

  message("Running the combined task")
  ## We need to run the combined task here too. Getting this "Right"
  ## would be much easier after we implement the transactional
  ## workflow approach so that we might see the state of the orderly
  ## archive as a log. For now, we assume that that combined task will
  ## be ok to run.
  name_combined <- paste0(meta$name, "_combined")
  ## This section might need tweaking for different tasks:
  keep <- c("date", "short_run", "kernel_scaling")
  parameters <- meta$parameters[[1]][keep]
  orderly:::orderly_run_internal(name_combined, parameters = parameters,
                                 commit = TRUE)

  write_lines_to_sharepoint("", file.path("metadata", key, "imported"), folder)

  message(sprintf("Finished processing '%s' (%s)", key, meta$name))
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
    tmp <- tempfile()
    metadata$download(file.path(key, "running"), tmp, progress = FALSE)
    ret$id <- readLines(tmp)
  }

  ret
}


roundtable_upload_results <- function(files, key, metadata, workdir, folder) {
  dest <- folder$create(file.path("results", key))
  for (f in files) {
    message(sprintf("Uploading '%s'", basename(f)))
    dest$upload(f, progress = TRUE)
  }

  # Write a file called 'finished' to indicate we're all done.
  write_lines_to_sharepoint("", file.path("metadata", key, "finished"), folder)
}
