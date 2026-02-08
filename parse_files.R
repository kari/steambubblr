# Parses Steam player statistics html files to an RDS file

library(tidyverse)
library(rvest)
library(janitor)
library(mirai)

mirai::daemons(parallel::detectCores() - 1)
# mirai::daemons(0)

filenames <- list.files(
  "steamstats",
  "\\d{8}-stats\\.html$",
  full.names = TRUE
) |>
  str_subset("20190912", negate = TRUE) # 20190912 is broken

# Test cases:
# 20150218 is empty
# 20160502 is in Swedish
# 20200504 has just few random characters

# NOTE: function has package prefixes for mirai
read_stats <- function(fn) {
  print(fn)
  doc <- rvest::read_html(fn)

  # doc is empty
  if (length(rvest::html_children(doc)) == 0) {
    return(tibble::tibble())
  }

  stat_table <- doc |> # FIXME: set correct encoding based on date!
    rvest::html_element("div#detailStats table")

  # archive.is modifies the html, dropping all classes
  if (is.na(stat_table)) {
    return(tibble::tibble())
  }

  stat_table |>
    rvest::html_table(header = TRUE) |>
    dplyr::select(c(1, 2, 4)) |>
    setNames(c("current_players", "peak_today", "game")) |>
    dplyr::filter(!dplyr::row_number() == 1) |>
    dplyr::mutate(
      current_players = as.numeric(gsub(",", "", current_players)),
      peak_today = as.numeric(gsub(",", "", peak_today)),
      # older documents are iso-8859-1 and later utf-8
      game = stringr::str_replace_all(game, "[^\\p{ASCII}]", ""),
      game_id = stat_table |>
        rvest::html_elements("tr.player_count_row td:nth-of-type(4) a") |>
        rvest::html_attr("href") |>
        # NOTE: some app IDs might not be numeric
        stringr::str_extract("(?<=/app/)\\d+"),
      date = lubridate::ymd(stringr::str_extract(fn, "\\d{8}"))
    )
}

start_time <- Sys.time()

steam_stats <- map(
  filenames,
  in_parallel(\(fn) read_stats(fn), read_stats = read_stats)
) |>
  bind_rows()

end_time <- Sys.time()
execution_time <- end_time - start_time
print(execution_time)

# gaps in dates
# dates <- unique(steam_stats$date)
# expected <- seq(min(dates), max(dates), by="day")
# expected[!expected %in% dates]

saveRDS(steam_stats, "steam_stats.rds")
