merge_files_in_fileset_db = function(fileset_db) {

  file_name_from_dataset_id = function(dataset_id) {
    splits_names = strsplit(dataset_id, "\\.")
    file_names = sapply(splits_names, function(vec) {
      paste(vec[1:8], collapse = "_")
    })
    file_names = gsub("-", ".", file_names)
    return(file_names)
  }

  merged_stars_list = merge_files_in_dataset(fileset_db$dataset_id,
                                        fileset_db$file_paths)

  file_names = file_name_from_dataset_id(names(merged_stars_list))
  dir_paths = unique(dirname(fileset_db$file_paths)) # this is a problem
  file_paths = paste(dir_paths, file_names, sep = "/")
  mapply(qs::qsave, x = merged_stars_list, file = file_paths)
  return(data.table::data.table(dataset_id = names(merged_stars_list),
                                file_name = file_names,
                                file_path = file_paths))
}

merge_files_in_dataset = function(dataset_name, file_paths) {
  dataset_list = split(file_paths, dataset_name)

  read_merge_ncdf = function(file_paths) {
    list_stars = lapply(file_paths,  stars::read_ncdf)
    merged_star = do.call(c, list_stars)
  }
  merged_stars = lapply(dataset_list, read_merge_ncdf)

  names(merged_stars) = names(dataset_list)
  return(merged_stars)
}


