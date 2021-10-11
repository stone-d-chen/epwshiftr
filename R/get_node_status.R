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
