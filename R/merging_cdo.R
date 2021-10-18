
create_dataset_destination = function(fileset_db) {
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
                  fileset_db$dataset_name,
                  dataset_name,
                  sep = "/")
    fileset_db[, dataset_path := paths]
}

merge_cdo = function(fileset_db) {
    files = split(fileset_db$file_paths, fileset_db$dataset_path)
    res = lapply(seq_along(files), function(i) {
        inputs = files[[i]]
        output = names(files)[i]
        system2("cdo", args = c("mergetime", inputs, output))
    })
}

#example

create_fileset_destinations(filesets_canesm5)
create_dataset_destination(filesets_canesm5)
merge_cdo(filesets_canesm5)
