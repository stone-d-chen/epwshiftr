# from epwshifter
get_node_status = function (speed_test = FALSE, timeout = 3)
{
  f <- tempfile()
  utils::download.file("https://esgf-node.llnl.gov/status/",
                       f, "libcurl", quiet = TRUE)
  l <- readLines(f)
  l_s <- grep("<!--load block main-->", l, fixed = TRUE)
  if (!length(l_s))
    stop("Internal Error: Failed to read data node table")
  l <- l[l_s:length(l)]
  l_s <- grep("<table>", l, fixed = TRUE)[1L]
  l_e <- grep("</table>", l, fixed = TRUE)[1L]
  if (!length(l_s) || !length(l_e))
    stop("Internal Error: Failed to read data node table")
  l <- l[l_s:l_e]
  loc <- regexec("\\t<td>(.+)</td>", l)
  nodes <- vapply(seq_along(l), function(i) {
    if (all(loc[[i]][1] == -1L))
      return(NA_character_)
    substr(l[i], loc[[i]][2], loc[[i]][2] + attr(loc[[i]],
                                                 "match.length")[2] - 1L)
  }, NA_character_)
  nodes <- nodes[!is.na(nodes)]
  loc <- regexec("\\t\\t<font color=\"#\\S{6}\"><b>(UP|DOWN)</b>",
                 l)
  status <- vapply(seq_along(l), function(i) {
    if (all(loc[[i]][1] == -1L))
      return(NA_character_)
    substr(l[i], loc[[i]][2], loc[[i]][2] + attr(loc[[i]],
                                                 "match.length")[2] - 1L)
  }, NA_character_)
  status <- status[!is.na(status)]
  if (length(nodes) != length(status))
    stop("Internal Error: Failed to read data node table")
  res <- data.table::data.table(data_node = nodes, status = status)
  data.table::setorderv(res, "status", -1)
  if (!speed_test)
    return(res)
  if (!requireNamespace("pingr", quietly = TRUE)) {
    stop("'epwshiftr' relies on the package 'pingr' to perform speed test",
         "please add this to your library with install.packages('pingr') and try again.")
  }
  if (!length(nodes_up <- res[status == "UP", data_node])) {
    message("No working data nodes available now. Skip speed test")
    return(res)
  }
  speed <- vapply(nodes_up, function(node) {
    message(sprintf("Testing data node '%s'...", node))
    pingr::ping(node, count = 1, timeout = timeout)
  }, numeric(1))
  res[status == "UP", `:=`(ping, speed)][order(ping)]
}


create_fileset_destinations = function(fileset_db) {
  paths = paste("data",
                fileset_db$variable_id,
                fileset_db$source_id,
                fileset_db$experiment_id,
                fileset_db$member_id,
                basename(fileset_db$file_url),
                sep = "/")
  downloaded = "FALSE"
  fileset_db[, file_paths := paths]
  fileset_db[, downloaded := "FALSE"]
}

add_node_status = function(fileset_db) {
  node_status = data.table::setDT(get_node_status())
  fileset_db[node_status, on = c("data_node"), node_status := i.status]
}

download_files = function(urls_to_dl, destination_paths, method = "libcurl") {
  p = progressor(along = urls_to_dl)
  options(timeout = max(600, getOption("timeout")))

  trycatch_download = function(url, path, method = method) {
    tryCatch(
      {
        p(message = paste("starting", basename(path)), amount = 0)
        Sys.sleep(1)
        download.file(url, path, method = method, mode = "wb",
                      quiet = TRUE)
        p(message = basename(path), class = "sticky", amount = 1)
        return("TRUE")
      },
      error = function(e) p(message = e, class = "sticky"),
      warning =  function(w) {
        p(message = sprintf("%s\n", w$message), class = "sticky")
        return(w$message)
      }
    )
  }
  status <- mapply(trycatch_download, urls_to_dl, destination_paths)
}


download_from_fileset = function(fileset_db, cores = 0, method = "libcurl") {
  files_to_dl = fileset_db
  if("node_status" %in% names(fileset_db))  {
    files_to_dl = files_to_dl[node_status == "UP" & downloaded == "FALSE", ]
  }

  message(sprintf("Files to download: %d\nTotal Size (GB): %.2f", nrow(files_to_dl)[1], sum(files_to_dl$file_size)/1024/1024/1024))

  for (path in unique(dirname(files_to_dl$file_paths))) {
    dir.create(path, recursive = TRUE, showWarnings = FALSE)
  }

  if (cores > 0) plan(multisession, workers = cores)
  download_status <- download_files(files_to_dl$file_url, files_to_dl$file_paths)
  files_to_dl$downloaded = download_status
  fileset_db[files_to_dl[,c("dataset_id", "downloaded")], on = ("dataset_id"), downloaded := i.downloaded]
}




download_from_fileset_ = function(fileset_db, cores = 0, method = "libcurl", further_check = FALSE) {
  files_to_dl = fileset_db

  if("node_status" %in% names(fileset_db))  {
    files_to_dl = files_to_dl[node_status == "UP" & downloaded == "FALSE", ]
  }

  if(further_check == TRUE) {
      nodes_to_check = files_to_dl[!duplicated(data_node), .(data_node, file_url, file_paths)]

      for (path in unique(dirname(nodes_to_check$file_paths))) {
        dir.create(path, recursive = TRUE, showWarnings = FALSE)
      }

      try_download = function(file_url, file_paths, method) {
        tryCatch(
          {
            download.file(file_url, file_paths, method)
            return("downloaded")
          },
          error = function(error) message(error),
          warning = function(warning) return(list(warning))
        )
      }

      options(timeout = max(600, getOption("timeout")))
      status = future_mapply(try_download, nodes_to_check$file_url, nodes_to_check$file_paths)

      nodes_to_check$status = sapply(status, function(node_status) {
          if (node_status[1] == "downloaded") return("success")
          else {"error/warn"}
         }
      )
      files_to_dl[nodes_to_check[, .(data_node, status)], on = c("data_node"), dl_check := i.status]
      files_to_dl = files_to_dl[dl_check == "success"]
  }

  for (path in unique(dirname(files_to_dl$file_paths))) {
    dir.create(path, recursive = TRUE, showWarnings = FALSE)
  }

  if (cores > 0) plan(multisession, workers = cores)

  download_status <- download_files(files_to_dl$file_url, files_to_dl$file_paths)
  files_to_dl$downloaded = download_status
  fileset_db[files_to_dl[,c("dataset_id", "downloaded")], on = ("dataset_id"), downloaded := i.downloaded]
}
