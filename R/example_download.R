library(data.table) # uses data.table under the hood
library(progressr) # for download progress bars
library(progress) # for download progress bars
library(future.apply) #parallizes downloads

handlers(global = TRUE)
handlers("progress")

source("R/download_files.R")
source("R/esgf_query.R")

filesets_ecearth = esgf_query(
    project = c("CMIP6"),
    variable = c("pr"),
    source = NULL,
    variant = NULL,
    experiment = c("historical"),
    frequency = c("mon", "ssp585"),
    resolution = NULL, # null means all, probably should change that
    type = "File"
)

filesets_ecearth=filesets_ecearth[data_node == "esgf.ichec.ie"][c(1,2)]

filesets_2 = esgf_query(
    project = c("CMIP5"),
    variable = c("pr"),
    source = NULL,
    variant = NULL,
    experiment = c("past1000"),
    frequency = c("mon"),
    resolution = NULL, # null means all, probably should change that
    type = "File"
)

filesets_canesm5 = esgf_query(
    project = c("CMIP6"),
    variable = c("pr", "tas"),
    source = NULL,
    variant = c("r1i1p1f1"),
    experiment = c("historical", "ssp585"),
    frequency = c("mon"),
    resolution = c("500 km"), # null means all, probably should change that
    type = "File"
)

sum(filesets_canesm5$file_size)/1024/1024/1024 #filesize in gb

create_fileset_destinations(filesets_ecearth) # creates a file structure
add_node_status(filesets_ecearth) # to prevent attempting to download from servers which are down
download_from_fileset(filesets_ecearth) # begin downloads; no parallel call

#sometimes downloads will randomly fail (maybe too many connection to the server at that time)
sum(filesets_canesm5$downloaded == "FALSE")
# if so, re-attempt to download by re-running
# it should just re-attempt to download the missing files
download_from_fileset(filesets_canesm5)


# create_fileset_destinations(filesets_ecearth) # creates a file structure
# add_node_status(filesets_ecearth) # to prevent attempting to download from servers which are down
# download_from_fileset(filesets_ecearth) # begin downloads; no parallel call
#
