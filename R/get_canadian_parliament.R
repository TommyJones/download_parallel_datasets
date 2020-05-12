# Aligned Hansards of the 36th Parliament of Canada: 1.3 million pairs of aligned 
# text segments in English and French. The text is taken from the official records
# of the 36th Canadian Parliament.

# Source: https://www.isi.edu/natural-language/download/hansard/

library(tidyverse)

in_folder <- "data_raw/canadian_parliament"

# check to see if a folder exists for this data source
# if not, create one
if (! dir.exists(in_folder)) {
  dir.create(in_folder)
}

# create a subfolder with a date/timestamp 
time <-
  Sys.time() %>%
  str_replace_all("[^0-9-]", ".")

in_folder <- 
  in_folder %>%
  paste0(
    "/",
    time
  )

dir.create(in_folder)

# download the files
download.file(
  "http://www.isi.edu/natural-language/download/hansard/hansard.36.r2001-1a.house.debates.training.tar",
  destfile = paste0(in_folder, "/", "house_debates_training.tar"),
  mode = "wb"
)

download.file(
  "http://www.isi.edu/natural-language/download/hansard/hansard.36.r2001-1a.house.debates.testing.tar",
  destfile = paste0(in_folder, "/", "house_debates_testing.tar"),
  mode = "wb"
)

download.file(
  "http://www.isi.edu/natural-language/download/hansard/hansard.36.r2001-1a.senate.debates.training.tar",
  destfile = paste0(in_folder, "/", "senate_debates_training.tar"),
  mode = "wb"
)

download.file(
  "http://www.isi.edu/natural-language/download/hansard/hansard.36.r2001-1a.senate.debates.testing.tar",
  destfile = paste0(in_folder, "/", "senate_debates_testing.tar"),
  mode = "wb"
)

# extract the files

in_file_list <- list.files(in_folder, recursive = TRUE, full.names = TRUE)

for (f in in_file_list) {
  untar(
    tarfile = f,
    exdir = str_replace_all(f, "\\.tar", "")
      )
}


### extract and pull into data frames
# list files
file_list <- list.files(
  in_folder,
  recursive = TRUE,
  full.names = TRUE
)

file_list <- file_list[str_detect(file_list, "\\.gz$")]

# load those files into one big ol' character vector
docs <- parallel::mclapply(
  file_list,
  function(x) {
    # read the doc in, no need to unzip
    doc <- read_lines(x, skip_empty_rows = FALSE)
    
    doc[is.na(doc)] <- "" # because empty rows get entered as NAs
    
    # parse the file path as it has metadata we want
    path_parsed <- str_split(x, "/")[[1]]
    
    # collect metadata
    out <-
      tibble(
        doc_id = path_parsed[length(path_parsed)],
        doc_id_normalized = str_replace(
          path_parsed[length(path_parsed)],
          "\\.(e|f)\\.gz", ""
        ),
        chamber = case_when(
          "house" %in% path_parsed ~ "house",
          "senate" %in% path_parsed ~ "senate"
        ),
        train_test = case_when(
          "training" %in% path_parsed ~ "training",
          "testing" %in% path_parsed ~ "testing"
        )
      ) %>%
      mutate(
        language = case_when(
          str_detect(doc_id, "e\\.gz$") ~ "English",
          str_detect(doc_id, "f\\.gz$") ~ "French"
        )
      )
    
    # get sentences into a tibble
    out2 <- 
      tibble(
        sentence_id = seq_along(doc),
        text = doc
      ) %>%
      mutate(doc_id = out$doc_id)
    
    # combine
    out <- full_join(
      out,
      out2
    )

    out
    
  },
  mc.cores = parallel::detectCores() - 1
)

docs <- do.call(rbind, docs)

# get english and french in the same rows
english <- 
  docs %>%
  filter(language == "English") %>%
  select(
    doc_id_normalized,
    sentence_id,
    train_test,
    chamber,
    text_en = text
  )

french <- 
  docs %>%
  filter(language == "French") %>%
  select(
    doc_id_normalized, 
    sentence_id,
    train_test,
    chamber,
    text_fr = text
  )

canadian_parliament <- 
  full_join(
    english,
    french
  )

# save and exit

canadian_parliament$downloaded <- 
  lubridate::ymd_hms(time)

save(
  canadian_parliament,
  file = "data_derived/canadian_parliament.RData"
)


