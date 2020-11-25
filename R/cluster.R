prepare_cluster <- function(packages, initialise = TRUE) {
  ## Ed: Interactively on Windows, I need to do a web_login this
  ## before running. Not sure how to do the auth if these scripts are
  ## run directly from the command-line.
  didehpc::web_login()

  src <- provisionr::package_sources(repos = "drat://ncov-ic")
  ctx <- context::context_save("contexts",
                               packages = union(packages, "orderly"),
                               package_sources = src)

  cfg <- didehpc::didehpc_config(cluster = "big", template = "32Core",
                                 cores = 32, parallel = FALSE)
  obj <- didehpc::queue_didehpc(ctx, config = cfg, initialise = initialise)
  if (initialise) {
    obj$provision()
  }
  obj
}
