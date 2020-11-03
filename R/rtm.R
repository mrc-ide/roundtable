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
roundtable_rtm_run <- function(key, test_job = FALSE, upgrade = FALSE,
                               rerun = FALSE) {
  folder <- sharepoint_folder()

  if ("finished" %in% folder$files(file.path("results", key))$name) {
    stop(sprintf("Key '%s' has already been run", key))
  }

  file.path("incoming", key)
  dir.create(dest, FALSE, TRUE)
  contents <- src$list()

  incoming <- roundtable_download_incoming(key, folder)

  obj <- prepare_cluster(incoming$metadata, initialise = TRUE)
  obj$provision()
  if (test_job) {
    t <- obj$enqueue(packageVersion("sircovid"))
    t$wait(timeout = 100)
  }

  if (incoming$is_running && !rerun) {
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
    incoming$folder$upload(tmp, "running")
  }

  results <- grp$wait(timeout = Inf)
  files <- sprintf("%s/%s.zip", workdir, vapply(results, "[[", "", "id"))

  roundtable_upload_results(files, key, incoming$metadata, workdir, folder)
}


##' Import an RTM run back into orderly.
##' @title Import an RTM run into orderly
##'
##' @param key The key created by [roundtable_rtm_prepare()] and run
##'   with [roundtable_rtm_run()]
##'
##' @export
roundtable_rtm_import <- function(key) {
  folder <- sharepoint_folder()

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
  orderly:::orderly_run_internal(name_combined, parameters = parameters)

  message(sprintf("Finished processing '%s' (%s)", key, metadata$name))
  ## TODO: This will be fixed once I update spud.
  message("Please delete the incoming and results directories on sharepoint")
}


roundtable_rtm_list <- function() {
  browser()
}


roundtable_rtm_status <- function(key) {
  browser()
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


prepare_cluster <- function(metadata, initialise = TRUE) {
  # Interactively on Windows, I need to do a web_login this before
  # running. Not sure how to do the auth if these scripts are run
  # directly from the command-line.
  didehpc::web_login()

  ## This might want updating to be more tuneable. We could read this
  ## from the orderly config if needed, but that's not yet supported.
  packages <- c(names(metadata$packages),
                "orderly")

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
