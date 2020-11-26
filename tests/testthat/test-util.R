test_that("sys_getenv errors if not found", {
  expect_error(sys_getenv("ROUNDTABLE_VAR"),
               "Environment variable 'ROUNDTABLE_VAR' is not set")
})


test_that("sys_getenv returns value if it exists", {
  Sys.setenv(ROUNDTABLE_VAR = "value")
  on.exit(Sys.setenv(ROUNDTABLE_VAR = NA_character_))
  expect_equal(sys_getenv("ROUNDTABLE_VAR"), "value")
})
