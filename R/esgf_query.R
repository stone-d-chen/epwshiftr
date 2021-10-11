  # modified from epwshiftr package
  # https://github.com/ideas-lab-nus/epwshiftr

  esgf_query = function (project = "CMIP6",
                         activity = NULL,
                         variable = c("tas", "tasmax", "tasmin", "hurs",
                                      "hursmax", "hursmin", "pr", "rsds",
                                      "rlds", "psl", "sfcWind", "clt"),
                         frequency = "day",
                         experiment = NULL,
                         source = c(
                           "AWI-CM-1-1-MR",
                           "BCC-CSM2-MR",
                           "CESM2",
                           "CESM2-WACCM",
                           "EC-Earth3",
                           "EC-Earth3-Veg",
                           "GFDL-ESM4",
                           "INM-CM4-8",
                           "INM-CM5-0",
                           "MPI-ESM1-2-HR",
                           "MRI-ESM2-0"
                         ),
                         variant = "r1i1p1f1",
                         replica = FALSE,
                         latest = TRUE,
                         resolution = c("100 km", "50 km"),
                         type = "Dataset",
                         limit = 10000L,
                         data_node = NULL)
  {
    pair <- function(x, first = FALSE) {
      var <- deparse(substitute(x))
      if (is.null(x) || length(x) == 0)
        return()
      key <- dict[names(dict) == var]
      if (!length(key))
        key <- var
      if (is.logical(x))
        x <- tolower(x)
      s <- paste0(key, "=", paste0(x, collapse = "%2C"))
      if (first)
        s
      else
        paste0("&", s)
    }

    `%and%` <- function(lhs, rhs) {
      if (is.null(rhs))
        lhs
      else
        paste0(lhs, rhs)
    }

    database_from_query = function(query_orig) {
      res = tryCatch(jsonlite::read_json(query_orig),
                     warning = function(w) w, error = function(e) e)
      if (inherits(res, "warning") || inherits(res, "error")) {
        message(
          "No matched data. Please check network connection and the availability of LLNL ESGF node."
        )
        dt <- rbind(data.table::data.table())
        return(dt)
      }
      else if (res$response$numFound == 0L) {
        message(

          "No matched data. Please examine the actual response using 'attr(x, \"response\")'."
        )
        dt <- data.table::data.table()
      }
      else if (type == "Dataset") {
        dt <- extract_query_dataset(res, project, variable)
      }
      else if (type == "File") {
        dt <- extract_query_file(res, project)
      }
      if("response" %in% attributes(dt)) return(dt)
      else {
        data.table::setattr(dt, "response", res)
        data.table::setattr(dt, "numFound", res$response$numFound)
        return(dt)
      }
    }


    url_base <- "http://esgf-node.llnl.gov/esg-search/search/?"
      if (project == "CMIP6") {
      dict <- c(
        activity = "activity_id",
        experiment = "experiment_id",
        source = "source_id",
        variable = "variable_id",
        resolution = "nominal_resolution",
        variant = "variant_label"
      )
      } else if (project == "CMIP5") {
        dict <- c(
          activity = "activity",
          experiment = "experiment",
          source = "model",
          variable = "variable",
          resolution = NULL,
          frequency = "time_frequency",
          variant = "ensemble"
        )
    }


    format <- "application%2Fsolr%2Bjson"
    resolution <- c(gsub(" ", "", resolution, fixed = TRUE),
                    gsub(" ", "+", resolution, fixed = TRUE))

    query_orig <-
      url_base %and% pair(project, TRUE) %and% pair(activity) %and%
      pair(experiment) %and% pair(source) %and% pair(variable) %and%
      pair(resolution) %and% pair(variant) %and% pair(data_node) %and%
      pair(frequency) %and% pair(replica) %and% pair(latest) %and%
      pair(type) %and% pair(limit) %and% pair(format)

    print(query_orig)

    dt = database_from_query(query_orig)
    loops_to_do = ceiling(attr(dt, "numFound")/10000) - 1
    offset = 10000
    if (loops_to_do > 0) {
      for (i in seq_len(loops_to_do)) {
        query_w_offset = paste0(query_orig, "&offset=" , as.character(offset))
        dt = rbind(dt, database_from_query(query_w_offset))
        offset = offset + 10000
      }
    }

    return(dt)
  }


  extract_query_dataset <- function (q, project, variable_name) {
    if(project == "CMIP6") {

      dt <- data.table::rbindlist(
        lapply(q$response$docs, function (l) {
          l = l[c("id", "mip_era", "activity_drs", "institution_id", "source_id",
            "experiment_id", "member_id", "table_id", "frequency", "grid_label",
            "version", "nominal_resolution", "variable_id", "variable_long_name",
            "variable_units", "data_node", "pid")]
        lapply(l, unlist)
      }))
      data.table::setnames(dt, c("id", "pid"), c("dataset_id", "dataset_pid"), skip_absent = TRUE)
    }
    if(project == "CMIP5") {

      dt <- data.table::rbindlist(
        lapply(q$response$docs, function (l) {
          l = l[c("id", "institute", "model", "ensemble",
            "experiment", "time_frequency", "variable",
            "variable_units", "data_node")]
        lapply(l, unlist)
      }))
      data.table::setnames(dt, c("id", "pid"), c("dataset_id", "dataset_pid"), skip_absent = TRUE)
      dt = dt[dt[["variable"]] == variable_name]
    }
    dt
  }



  extract_query_file <- function (q, project) {
    # to avoid No visible binding for global variable check NOTE
    id <- NULL
    if(project == "CMIP6") {
      dt_file <-
        data.table::rbindlist(lapply(q$response$docs, function (l) {
          l <-
            l[c("id", "dataset_id", "mip_era", "activity_drs",
              "institution_id", "source_id", "experiment_id",
              "member_id", "table_id", "frequency", "grid_label",
              "version", "nominal_resolution", "variable_id", "variable_long_name",
              "variable_units", "data_node", "size", "url", "tracking_id")]
          l$url <- grep("HTTPServer", unlist(l$url), fixed = TRUE, value = TRUE)

          if (!length(l$url)) {
            warning("Dataset with id '", l$id, "' does not have a HTTPServer download method.")
            l$url <- NA_character_
          }
          lapply(l, unlist)
        }))

      dt_file[, c("datetime_start", "datetime_end") := parse_file_date(id, frequency)]
      dt_file[, url := gsub("\\|.+$", "", url)]
      data.table::setnames(dt_file, c("id", "size", "url"),
                           c("file_id", "file_size", "file_url"))
      data.table::setcolorder(dt_file, c( "file_id",
                                          "dataset_id",
                                          "mip_era",
                                          "activity_drs",
                                          "institution_id",
                                          "source_id",
                                          "experiment_id",
                                          "member_id",
                                          "table_id",
                                          "frequency",
                                          "grid_label",
                                          "version",
                                          "nominal_resolution",
                                          "variable_id",
                                          "variable_long_name",
                                          "variable_units",
                                          "datetime_start",
                                          "datetime_end",
                                          "file_size",
                                          "data_node",
                                          "file_url",
                                          "tracking_id")
      )
    }

    if(project == "CMIP5") {
      dt_file <-
        data.table::rbindlist(lapply(q$response$docs, function (l) {
          l <-
            l[c("id", "dataset_id", "project",
              "institute", "experiment",
              "ensemble", "time_frequency",
              "version", "variable", "variable_units", "data_node",
              "size", "url", "tracking_id")]
          l$url <- grep("HTTPServer", unlist(l$url), fixed = TRUE, value = TRUE)

          if (!length(l$url)) {
            warning("Dataset with id '", l$id, "' does not have a HTTPServer download method.")
            l$url <- NA_character_
          }
          lapply(l, unlist)
        }))

      dt_file[, c("datetime_start", "datetime_end") := parse_file_date(id, time_frequency)]
      dt_file[, url := gsub("\\|.+$", "", url)]
      data.table::setnames(dt_file, c("id", "size", "url"),
                           c("file_id", "file_size", "file_url"))
    }

    return(dt_file[])
  }


  parse_file_date <- function (id, frequency) {
    dig <-
      data.table::fifelse(grepl("hr", frequency, fixed = TRUE), 12L,
        data.table::fifelse(frequency == "day", 8L,
          data.table::fifelse(frequency == "dec", 4L,
            data.table::fifelse(grepl("mon", frequency, fixed = TRUE), 6L,
              data.table::fifelse(grepl("yr", frequency, fixed = TRUE), 4L, 0L)
            )
          )
        )
      )

    reg <- sprintf("([0-9]{%i})-([0-9]{%i})", dig, dig)
    reg[dig == 0L] <- NA_character_

    suf <- data.table::fifelse(dig == 0L, "",
                               data.table::fifelse(dig == 4L, "0101",
                                                   data.table::fifelse(dig == 6L, "01", "")))

    fmt <- data.table::fifelse(dig == 0L, NA_character_,
      data.table::fifelse(dig == 4L, "%Y%m%d",
        data.table::fifelse(dig == 6L, "%Y%m%d",
          data.table::fifelse(dig == 8L, "%Y%m%d",
            data.table::fifelse(dig == 12L, "%Y%m%d%H%M%s", NA_character_)
          )
        )
      )
    )

    data.table::data.table(id, reg, suf, fmt)[!J(NA_character_), on = "reg", by = "reg",
                                              c("datetime_start", "datetime_end") := {
                                                m <- regexpr(.BY$reg, id)
                                                s <-
                                                  data.table::tstrsplit(regmatches(id, m), "-", fixed = TRUE)
                                                lapply(s, function (x)
                                                  as.POSIXct(paste0(x, suf), format = fmt[1L], tz = "UTC"))
                                              }][, .SD, .SDcols = c("datetime_start", "datetime_end")]
  }
