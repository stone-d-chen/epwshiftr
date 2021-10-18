create_dataset_destinations = function(fileset_db) {
    name_parts = strsplit(basename(fileset_db$file_url),split = "_")
    dataset_name = sapply(name_parts, function(vec)  {
        name = paste(vec[1:(length(vec)-1)], collapse = "_")
        paste0(name, ".nc")
    })
    paths = paste("data",
                  fileset_db$variable_id,
                  fileset_db$source_id,
                  fileset_db$experiment_id,
                  fileset_db$member_id,
                  dataset_name,
                  sep = "/")
    fileset_db[, dataset_path := paths]
}

merge_cdo = function(fileset_db) {
    files = split(fileset_db$file_paths, fileset_db$dataset_path)
    res = sapply(seq_along(files), function(i) {
        inputs = files[[i]]
        output = names(files)[i]
        system2("cdo", args = c("mergetime", inputs, output))
        # merge time will ask to over write, which may be annoying
    })

    fileset_db[, merged := res]

    #subset and merge result
}




fileset = data.table::fread("fileset_db.csv")
create_fileset_destinations(fileset)
create_dataset_destinations(fileset)
paths = unique(fileset$dataset_path)
merge_cdo(fileset[dataset_path %in% paths[1:2]])

fileset_db = fileset[dataset_path %in% paths[1:2], ]
dataset_db = fileset_db[!duplicated(dataset_path)][, file_paths := NULL]

remapbil_cdo = function(dataset_db, grid = "r360x180", other_nc = NULL) {
    if (!is.null(other_nc)) grid = other_nc
    inputs = dataset_db$dataset_path
    outname = paste("regrid", grid, basename(inputs), sep = "_")
    out_dest = paste(dirname(inputs), outname, sep = "/")
    remapbil = paste0("-remapbil,", grid)

    res = sapply(seq_along(out_dest), function(i) {
        system2("cdo", args = c(remapbil, inputs[i], out_dest[i]))
    })

    dataset_db[, (grid) := ifelse(res == 0, out_dest, "FAIL")]
}


