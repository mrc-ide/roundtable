load_config <- function() {
  host <- sys_getenv("ROUNDTABLE_SHAREPOINT_HOST")
  site <- sys_getenv("ROUNDTABLE_SHAREPOINT_SITE")
  root <- sys_getenv("ROUNDTABLE_SHAREPOINT_ROOT")
  list(host = host, root = root, site = site)
}


sharepoint_folder <- function() {
  config <- load_config()
  sharepoint <- spud::sharepoint$new(config$host)
  folder <- sharepoint$folder(config$site, config$root, verify = TRUE)

  ## Ensure that folders are present:
  contents <- folder$folders()
  for (p in setdiff(c("incoming", "results"), contents$name)) {
    folder$create(p)
  }

  folder
}
