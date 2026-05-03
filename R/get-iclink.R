# fuzzy matching helper 
token_set_ratio <- function(s1, s2) {
  
  tokenize <- function(s) {
    
    sort(unique(tolower(unlist(strsplit(trimws(s), "[^[:alnum:]]+")))))
    
  }
  
  ratio <- function(a, b) {
    
    # simple char-level ratio: 2 * LCS_length / (len_a + len_b)
    if (nchar(a) == 0 && nchar(b) == 0) return(100)
    
    lv <- utils::adist(a, b)           # edit distance
    sim <- 1 - lv / max(nchar(a), nchar(b))
    round(sim * 100)
    
  }

  t1 <- tokenize(s1)
  t2 <- tokenize(s2)

  inter  <- intersect(t1, t2)
  diff1  <- setdiff(t1, inter)
  diff2  <- setdiff(t2, inter)

  t0_str <- paste(inter,  collapse = " ")
  t1_str <- paste(c(inter, diff1), collapse = " ")
  t2_str <- paste(c(inter, diff2), collapse = " ")

  max(ratio(t0_str, t1_str), ratio(t0_str, t2_str), ratio(t1_str, t2_str))
  
}

token_set_ratio_v <- Vectorize(token_set_ratio)

# Build CRSP-IBES link table (iclink)

get_iclink <- function(conn) {

  # Step 1: Link by CUSIP
  # 1.1 IBES: US firms with a CUSIP
  ibes1 <- DBI::dbGetQuery(conn, "
    SELECT ticker, cusip, cname, sdates
    FROM   ibes.id
    WHERE  usfirm = 1
      AND  cusip  <> ''
  ")
  ibes1$sdates <- as.Date(ibes1$sdates)

  # Date range per (ticker, cusip)
  ibes1_date <- ibes1 |>
    group_by(ticker, cusip) |>
    summarise(fdate = min(sdates), ldate = max(sdates), .groups = "drop")

  ibes2 <- ibes1 |>
    left_join(ibes1_date, by = c("ticker", "cusip")) |>
    arrange(ticker, cusip, sdates) |>
    filter(sdates == ldate) |>         # keep most-recent company name
    select(-sdates)

  # 1.2 CRSP: permno-ncusip combinations
  crsp1 <- DBI::dbGetQuery(conn, "
    SELECT permno, ncusip, comnam, namedt, nameenddt
    FROM   crsp.stocknames
    WHERE  ncusip <> ''
  ")
  crsp1$namedt     <- as.Date(crsp1$namedt)
  crsp1$nameenddt  <- as.Date(crsp1$nameenddt)

  crsp1_dtrange <- crsp1 |>
    group_by(permno, ncusip) |>
    summarise(namedt    = min(namedt),
              nameenddt = max(nameenddt),
              .groups   = "drop")

  crsp2 <- crsp1 |>
    select(-namedt) |>
    rename(enddt = nameenddt) |>
    left_join(crsp1_dtrange, by = c("permno", "ncusip")) |>
    filter(enddt == nameenddt) |>
    select(-enddt)

  # 1.3 Merge on full CUSIP
  link1_1 <- inner_join(ibes2, crsp2,
                        by = c("cusip" = "ncusip")) |>
    arrange(ticker, permno, ldate)

  # Keep most-recent ldate per (ticker, permno)
  link1_1_tmp <- link1_1 |>
    group_by(ticker, permno) |>
    summarise(ldate = max(ldate), .groups = "drop")

  link1_2 <- inner_join(
    link1_1, link1_1_tmp, by = c("ticker", "permno", "ldate")
  )

  # Fuzzy name matching
  link1_2$name_ratio <- token_set_ratio_v(link1_2$comnam, link1_2$cname)
  name_ratio_p10     <- quantile(link1_2$name_ratio, 0.10, na.rm = TRUE)

  # Score 0-3
  score1 <- function(fdate, ldate, namedt, nameenddt, name_ratio) {
    
    dplyr::case_when(
      (fdate <= nameenddt) & (ldate >= namedt) & (name_ratio >= name_ratio_p10) ~ 0L,
      (fdate <= nameenddt) & (ldate >= namedt)                                  ~ 1L,
      name_ratio >= name_ratio_p10                                              ~ 2L,
      TRUE                                                                      ~ 3L
    )
    
  }

  link1_2 <- link1_2 |>
    mutate(score = score1(fdate, ldate, namedt, nameenddt, name_ratio)) |>
    select(ticker, permno, cname, comnam, name_ratio, score) |>
    distinct()

  # Step 2: Link by Exchange Ticker
  # Unmatched IBES tickers
  nomatch1 <- ibes2 |>
    select(ticker) |>
    left_join(link1_2 |> select(ticker, permno), by = "ticker", relationship = "many-to-many") |>
    filter(is.na(permno)) |>
    select(ticker) |>
    distinct()

  # IBES exchange ticker info
  ibesid <- DBI::dbGetQuery(conn, "
    SELECT ticker, cname, oftic, sdates, cusip
    FROM   ibes.id
    WHERE  oftic IS NOT NULL
  ")
  ibesid$sdates <- as.Date(ibesid$sdates)

  nomatch2 <- inner_join(nomatch1, ibesid, by = "ticker")

  # Date range per (ticker, oftic)
  nomatch3_dates <- nomatch2 |>
    group_by(ticker, oftic) |>
    summarise(fdate = min(sdates), ldate = max(sdates), .groups = "drop")

  nomatch3 <- nomatch2 |>
    left_join(nomatch3_dates, by = c("ticker", "oftic")) |>
    filter(sdates == ldate)

  # CRSP with exchange ticker
  crsp_n1 <- DBI::dbGetQuery(conn, "
    SELECT ticker, comnam, permno, ncusip, namedt, nameenddt
    FROM   crsp.stocknames
    WHERE  ticker IS NOT NULL
  ")
  crsp_n1$namedt    <- as.Date(crsp_n1$namedt)
  crsp_n1$nameenddt <- as.Date(crsp_n1$nameenddt)
  crsp_n1           <- arrange(crsp_n1, permno, ticker, namedt)

  crsp_n1_dt <- crsp_n1 |>
    group_by(permno, ticker) |>
    summarise(namedt    = min(namedt),
              nameenddt = max(nameenddt),
              .groups   = "drop")

  crsp_n2 <- crsp_n1 |>
    rename(namedt_ind = namedt, nameenddt_ind = nameenddt) |>
    left_join(crsp_n1_dt, by = c("permno", "ticker")) |>
    filter(nameenddt_ind == nameenddt) |>
    select(-namedt_ind, -nameenddt_ind) |>
    rename(crsp_ticker = ticker)

  # Merge on exchange ticker respecting date ranges
  link2_1 <- inner_join(nomatch3, crsp_n2,
                        by = c("oftic" = "crsp_ticker"), relationship = "many-to-many") |>
    filter(ldate >= namedt, fdate <= nameenddt)

  # Fuzzy name matching
  link2_1$name_ratio <- token_set_ratio_v(link2_1$comnam, link2_1$cname)

  link2_2 <- link2_1 |>
    mutate(cusip6  = substr(cusip,  1, 6),
           ncusip6 = substr(ncusip, 1, 6))

  # Score 0 / 4-6
  link2_2 <- link2_2 |>
    mutate(score = dplyr::case_when(
      cusip6 == ncusip6 & name_ratio >= name_ratio_p10 ~ 0L,
      cusip6 == ncusip6                                ~ 4L,
      name_ratio >= name_ratio_p10                     ~ 5L,
      TRUE                                             ~ 6L
    ))

  # Keep best score per IBES ticker
  link2_3 <- link2_2 |>
    select(ticker, permno, cname, comnam, name_ratio, score) |>
    group_by(ticker) |>
    filter(score == min(score)) |>
    ungroup() |>
    distinct()

  # Step 3: Combine
  iclink <- bind_rows(link1_2, link2_3)

  iclink
  
}
