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

merge_cdo = function(fileset_db, cores = 1) {
    call_mergetime = function(i) {
        inputs = files[[i]]
        output = names(files)[i]
        system2("cdo", args = c("mergetime", inputs, output))
    }

    files = split(fileset_db$file_paths, fileset_db$dataset_path)
    if(cores > 1) {
        cl = parallel::makeForkCluster(cores)
        res = parallel::parSapply(cl, seq_along(files), call_mergetime)
        # parallel::stopCluster(cl)
    } else {
        res = sapply(seq_along(files), call_mergetime)
    }

    fileset_db[, merged := res]

    #subset and merge result
}


fileset = data.table::fread("fileset_db.csv")
# create_fileset_destinations(fileset)
create_dataset_destinations(fileset)
merge_cdo(fileset, cores = 26)
# file.remove(fileset$dataset_path)

fileset_db = fileset[dataset_path %in% paths[1:2], ]w
dataset_db = fileset_db[!duplicated(dataset_path)][, file_paths := NULL]


remapbil_cdo = function(dataset_db, grid = "r360x180", other_nc = NULL, cores = 1) {
    if (!is.null(other_nc)) grid = other_nc
    inputs = dataset_db$dataset_path
    outname = paste("regrid", grid, basename(inputs), sep = "_")
    out_dest = paste(dirname(inputs), outname, sep = "/")
    remapbil = paste0("-remapbil,", grid)

    cl = parallel::makeForkCluster(cores)

    res = parallel::parSapply(cl, seq_along(out_dest), function(i) {
        system2("cdo", args = c(remapbil, inputs[i], out_dest[i]))
    })

    dataset_db[, (grid) := ifelse(res == 0, out_dest, "FAIL")]
}


