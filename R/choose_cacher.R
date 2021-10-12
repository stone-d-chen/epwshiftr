
choose_cacher = function() {
  cache = new.env()
  function(n, k) {
    key = paste(as.character(n), as.character(k), sep = ",", collapse = ";")
    if(!is.null(cache[[key]])) {
      return(cache[[key]])
    }
    result = base::choose(n, k)
    cache[[key]] = result
    return(result)
  }
}

choose = choose_cacher()
