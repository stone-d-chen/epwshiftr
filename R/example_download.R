library(data.table)
library(progressr)
library(progress)
handlers(global = TRUE)
handlers("progress")

source("R/esgf_query.R")

filesets_ecearth = esgf_query(
    project = "CMIP6",
    variable = "pr",
    source = c("EC-Earth3-Veg"),
    variant = c("r10i1p1f1"),
    experiment = c("historical"),
    frequency = c("mon"),
    resolution = NULL, # null means all, probably should change that
    type = "File"
)


source("R/download_files.R")

create_fileset_destinations(filesets_ecearth) # creates a file structure
add_node_status(filesets_ecearth) # to prevent attempting to download from servers which are down
download_from_fileset(filesets_ecearth) # begin downloads
