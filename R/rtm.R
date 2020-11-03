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
  ## TODO: check git here, or have that passed through to the bundle_pack
  regions <- c("east_of_england",
               "midlands",
               "london",
               "north_east_and_yorkshire",
               "north_west",
               "south_east",
               "south_west",
               "scotland",
               "wales",
               "northern_ireland")
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
  res <- lapply(parameters, function(p)
    orderly::orderly_bundle_pack(path, name, p))

  folder <- sharepoint_folder()

  key <- rtm_key()
  message(sprintf("Uploading %d items to sharepoint with key '%s'",
                  length(res), key))
  dest <- folder$create(file.path("incoming", key))
  for (x in res) {
    message(sprintf("Uploading '%s'", basename(x$path)))
    dest$upload(x$path, progress = TRUE)
  }

  p <- file.path(orderly::orderly_config()$root, "src", name, "orderly.yml")
  packages <- yaml::read_yaml(p)$packages
  versions <- lapply(packages, packageVersion)
  names(versions) <- packages
  names(parameters) <- vapply(res, "[[", "", "id")
  metadata <- list(name = name,
                   parameters = parameters,
                   packages = versions)
  tmp <- tempfile()
  saveRDS(metadata, tmp)
  message("Uploading metadata")
  dest$upload(tmp, "metadata.rds", progress = TRUE)

  key
}


##' Run a prepared RTM job on the cluster. Requires a key as created
##' by [roundtable_rtm_prepare()]. Run this on the cluster, from a
##' directory that is *not* the checked out copy of the orderly
##' sources, as this will not interact with it.
##'
##' @title Run RTM job on cluster
##'
##' @param key The job key
##'
##' @param test_job Send a test job to the cluster to ensure that
##'   everything works.
##'
##' @return
##' @author Richard Fitzjohn
roundtable_rtm_run <- function(key, test_job = FALSE, upgrade = FALSE) {
  browser()
  folder <- sharepoint_folder()
  incoming <- roundtable_download_incoming(key, folder)

  obj <- prepare_cluster(incoming$metadata, upgrade)
  if (upgrade) {
    obj$provision()
  }
  if (test_job) {
    t <- obj$enqueue(packageVersion("sircovid"))
    t$wait(timeout = 100)
  }

  if (incoming$is_running) {
    message(sprintf("Already running '%s' as '%s'", key, id))
    id <- incoming$id
    grp <- obj$task_bundle_get(id)
    workdir <- file.path("working", id)
  } else {
    id <- ids::adjective_animal(style = "kebab")
    message(sprintf("Running '%s' as '%s'", key, id))
    ## TODO: We should prevent multiple things starting these.
    ## TODO: I don't think we generally want to wait here?
    tmp <- tempfile()
    writeLines(id, tmp)
    incoming$folder$upload(tmp, "running")

    ## Then the ids that need running
    workdir <- file.path("working", id)
    grp <- obj$lapply(incoming$paths, function(p, workdir)
      orderly::orderly_bundle_run(p, workdir), workdir = workdir,
      timeout = 0, name = id)
  }

  results <- grp$wait(timeout = Inf)
  files <- sprintf("%s/%s.zip", workdir, vapply(results, "[[", "", "id"))

  roundtable_upload_results(results, key, workdir, folder)
}


rtm_cluster_import <- function(output_path, key) {
  folder <- sharepoint_folder()

  path_results <- file.path("orderly/results", key)
  results <- folder$folder(path_results, verify = TRUE)
  contents <- results$list()
  if (!("finished" %in% contents$name)) {
    stop("Results are not finished")
  }

  re <- "^[0-9]{8}-[0-9]{6}-[[:xdigit:]]{8}\\.zip$"
  ids <- grep(re, contents$name, value = TRUE)
  tmp <- tempfile()
  dir.create(tmp, FALSE, TRUE)

  for (p in ids) {
    message(sprintf("Downloading '%s'", p))
    results$download(p, file.path(tmp, p), progress = TRUE)
  }

  for (p in ids) {
    orderly::orderly_bundle_import(file.path(tmp, p))
  }

  unlink(tmp, recursive = TRUE)

  ## We need to run the combined task here too.

  message("Finished processing '%s' key")
  message("Please delete the incoming and results directories on sharepoint")
}


rtm_cluster_info <- function(key) {
  folder <- sharepoint_folder()
  incoming <- download_incoming(key, folder)
  id <- readLines(incoming$folder$download("running"))
  obj <- prepare_cluster(FALSE)
  obj$task_bundle_get(id)
}


roundtable_download_incoming <- function(key, folder) {
  incoming <- folder$folder("incoming", verify = TRUE)

  contents <- incoming$list()
  if (nrow(contents) == 0) {
    stop("No incoming tasks")
  }
  if (!(key %in% contents$name)) {
    stop(sprintf("Unknown key '%s'; valid options:\n%s",
                 key, paste("  -", contents$name, collapse = "\n")))
  }
  src <- incoming$folder(key)

  dest <- file.path("incoming", key)
  dir.create(dest, FALSE, TRUE)
  contents <- src$list()

  for (p in contents$name) {
    dest_p <- file.path(dest, p)
    if (file.exists(dest_p)) {
      message(sprintf("Not downloading '%s'", dest_p))
    } else {
      message(sprintf("Downloading '%s'", dest_p))
      src$download(p, dest_p)
    }
  }

  bundles <- dir(dest, pattern = "^[0-9]{8}-[0-9]{6}-[[:xdigit:]]{8}\\.zip$",
                 full.names = TRUE)
  metadata <- readRDS(file.path(dest, "metadata.rds"))
  is_running <- "running" %in% contents

  if (is_running) {
    id <- readRDS(file.path(dest, "running"))
  } else {
    id <- NULL
  }

  list(key = key,
       bundles = bundles,
       is_running = is_running,
       id = id,
       folder = src,
       metadata = metadata)
}

roundtable_upload_results <- function(files, key, workdir, folder) {
  dest <- folder$create(file.path("results", key))
  for (f in files) {
    dest$upload(f)
  }
  # Write a file called 'finished' to indicate we're all done.
  tmp <- tempfile()
  on.exit(unlink(tmp))
  writeLines("", tmp)
  dest$upload(tmp, dest = "finished")
}


prepare_cluster <- function(metadata, initialise = TRUE) {
  # Interactively on Windows, I need to do a web_login this before
  # running. Not sure how to do the auth if these scripts are run
  # directly from the command-line.
  didehpc::web_login()

  ## This might want updating to be more tuneable. We could read this
  ## from the orderly config if needed, but that's not yet supported.
  packages <- names(metadata$packages)

  src <- provisionr::package_sources(repos = "drat://ncov-ic")
  ctx <- context::context_save("contexts", packages = packages,
                               package_sources = src)

  cfg <- didehpc::didehpc_config(cluster = "big", template = "32Core",
                                 cores = 32)
  didehpc::queue_didehpc(ctx, config = cfg, initialise = initialise)
}


rtm_key <- function() {
  sprintf("%s-%s", format(Sys.Date(), "%Y%m%d"),
          ids::adjective_animal(1, style = "kebab"))
}
