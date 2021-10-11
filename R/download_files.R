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
  fileset_db$file_paths = paste("data",
                                fileset_db$variable_id,
                                fileset_db$source_id,
                                fileset_db$experiment_id,
                                fileset_db$member_id,
                                basename(fileset_db$file_url),
                                sep = "/")
  fileset_db$downloaded = NA

  return(fileset_db)
}

add_node_status = function(fileset_db) {
  node_status = data.table::setDT(get_node_status())
  fileset_db[node_status, on = c("data_node"), node_status := i.status]
}


download_from_fileset = function(fileset_db, parallel = TRUE) {


  if("node_status" %in% names(fileset_db))  {
    fileset_db = fileset_db[node_status == "UP" & is.na(downloaded)]
  }
  urls_to_dl = fileset_db$file_url
  destination_paths = fileset_db$file_paths


  for (path in unique(dirname(destination_paths))) {
    dir.create(path, recursive = TRUE)
  }

  if (parallel) plan(multicore)

  progressr::with_progress(download_status <-
                             download_files(urls_to_dl, destination_paths))
  download_status

}

download_files = function(urls_to_dl, destination_paths) {
  seqs = 1:length(urls_to_dl)
  p = progressr::progressor(along = seqs)
  options(timeout = max(600, getOption("timeout")))

  ret = sapply(seqs, function(i) {
    res = try(download.file(
      urls_to_dl[i],
      destination_paths[i],
      method = "libcurl",
      mode = "wb"
    ),
    silent = TRUE)
    if (inherits(res, "try-error")) {
      p(message = paste("Error", urls_to_dl[i]),
        class = "sticky")
      return(1)
    } else {
      p(message = basename(urls_to_dl[i]), class = "sticky")
      return(0)
    }
  })
}
