#!/usr/bin/env Rscript
# ======================================================== #
#
#             Get Standardized Earnings Surprises
#
#                 Gabriel E. Cabrera-Guzmán
#                The University of Manchester
#
#                       Spring, 2026
#
#                https://gcabrerag.rbind.io
#
# ------------------------------ #
# email: gabriel.cabreraguzman@postgrad.manchester.ac.uk
# ======================================================== #

# Load packages
library(tidyverse)
library(lubridate)
library(scales)
library(RSQLite)
library(dbplyr)
library(RPostgres)

# Load auxiliary functions
source("R/get_iclink.R")

# Connection
wrds <- dbConnect(
    RPostgres::Postgres(),
    host     = "wrds-pgdata.wharton.upenn.edu",
    dbname   = "wrds",
    port     = 9737,
    sslmode  = "require",
    user     = Sys.getenv("WRDS_USER"),
    password = Sys.getenv("WRDS_PASSWORD")
)

# Set sample date range
begdate <- '01/01/2013'
enddate <- '12/31/2025'

# Set CRSP date range a bit wider to guarantee collecting all information
crsp_begdate <- '01/01/2013'
crsp_enddate <- '12/31/2025'

all_sp500 <- 0

# ==========================================
#            S&P 500 Index Universe
# ------------------------------------------

# All companies that were ever included in S&P 500 index as an example
# Old version of the code uses comp.idxcst_his

# New code uses crsp.msp500list
# Linking Compustat GVKEY and IBES Tickers using ICLINK               
# For unmatched GVKEYs, use header IBTIC link in Compustat Security file 
if (all_sp500 == 1) {
    
    sp500_db <- dbSendQuery(wrds,
                            "select a.* from crsp.msp500list as a")
    
    sp500 <- dbFetch(sp500_db, n = -1) |>
        as_tibble()
    
    dbClearResult(sp500_db)
    
}

# CCM data
ccm_db <- dbSendQuery(wrds,
                      "select gvkey, lpermco as permco, lpermno as permno, 
                       linkdt, linkenddt 
                       from crsp.ccmxpf_linktable 
                       where usedflag=1 
                       and linkprim in ('P', 'C')")

# Fill linkenddt missing value (.E in SAS dataset) with today's date
ccm <- dbFetch(ccm_db, n = -1) |>
    as_tibble() |>
    mutate(
        permco = as.integer(permco),
        permno = as.integer(permno),
        linkenddt = if_else(is.na(linkenddt), lubridate::today(), linkenddt)
    )

dbClearResult(ccm_db)

sec_db <- dbSendQuery(wrds,
                      "select ibtic, gvkey from comp.security")

sec <- dbFetch(sec_db, n = -1)
dbClearResult(sec_db)

# Start the sequence of left join
if (all_sp500 == 1) {
    
    gvkey <- sp500 |>
        left_join(
            ccm, by = "permno", relationship = "many-to-many"
        ) |>
        left_join(
            sec |>
                filter(!is.na(ibtic)), by = "gvkey", relationship = "many-to-many"
        )
    
} else {
    
    gvkey <- ccm |>
        left_join(
            sec |>
                filter(!is.na(ibtic)), by = "gvkey", relationship = "many-to-many"
        )
    
}

# Build CRSP-IBES link table using get_iclink()
# High quality links: score = 0 or 1
iclink <- get_iclink(wrds)

iclink_hq <- iclink |>
    filter(score <= 1)

gvkey <- gvkey |>
    left_join(iclink_hq, by = "permno", relationship = "many-to-many")

# Fill missing ticker with ibtic
gvkey <- gvkey |>
    mutate(ticker = ifelse(!is.na(ticker), ticker, ibtic)) |>
    # Keep relevant columns and drop duplicates
    select('gvkey', 'permco', 'permno', 'linkdt', 'linkenddt', 'ticker') |>
    distinct_all()

# Calculate date ranges from gvkey
# Min linkdt for ticker and permno combination
gvkey_mindt <- gvkey |> 
    group_by(permno, ticker) |> 
    drop_na(ticker, permno) |> 
    filter(linkdt == min(linkdt) | is.na(linkdt)) |>
    select(ticker, permno, linkdt) |>
    ungroup()

# Max linkenddt for ticker and permno combination
gvkey_maxdt <- gvkey |> 
    group_by(permno, ticker) |> 
    drop_na(ticker, permno) |> 
    filter(linkenddt == max(linkenddt) | is.na(linkenddt)) |>
    select(ticker, permno, linkenddt) |>
    ungroup()

# Merge min and max date ranges
gvkey_dt <- gvkey_mindt |>
    inner_join(
        gvkey_maxdt, by = c('ticker', 'permno')
    ) |>
    arrange(ticker)

# ==========================================
#       Extract Estimates from IBES
# ------------------------------------------

# Extract estimates from IBES Unadjusted file and select    
# the latest estimate for a firm within broker-analyst group
# "fpi in (6,7)" selects quarterly forecast for the current 
# and the next fiscal quarter  
ibes_temp_db <- dbSendQuery(wrds,
                            paste0("select ticker, estimator, analys, pdf, fpi, value,", 
                                   " fpedats, revdats, revtims, anndats, anntims",
                                   " from ibes.detu_epsus", 
                                   " where fpedats between '", begdate, "' and '", enddate,
                                   "' and (fpi='6' or fpi='7')"))

# Merge to get date range linkdt and linkenddt to fulfill date requirement
ibes_temp <- dbFetch(ibes_temp_db, n = -1) |>
    as_tibble() |>
    left_join(
        gvkey_dt, by = 'ticker', relationship = "many-to-many"
    ) |>
    filter((linkdt <= anndats) & (anndats <= linkenddt))

dbClearResult(ibes_temp_db)

# Subset data for primary and diluted pdf
p_sub <- ibes_temp |> 
    filter(pdf == 'P') |> 
    select(ticker, fpedats, pdf)

d_sub <- ibes_temp |> 
    filter(pdf == 'D') |> 
    select(ticker, fpedats, pdf)

# Count number of estimates reported on primary/diluted basis
p_count <- p_sub |> 
    group_by(ticker, fpedats) |> 
    summarise(p_count = n()) |>
    ungroup()

d_count <- d_sub |> 
    group_by(ticker, fpedats) |> 
    summarise(d_count = n()) |>
    ungroup()

# Merge the counts with ibes_temp
ibes <- ibes_temp |> 
    left_join(
        d_count, by = c('ticker', 'fpedats')
    ) |> 
    left_join(
        p_count, by = c('ticker', 'fpedats')
    ) |>
    mutate(
        d_count = ifelse(is.na(d_count), 0, d_count),
        p_count = ifelse(is.na(p_count), 0, p_count),
        # Determine whether most analysts report estimates on primary/diluted basis
        # following Livnat and Mendenhall (2006)     
        basis = ifelse(p_count > d_count, 'P', 'D')
    ) |>
    arrange(ticker, fpedats, estimator, analys, anndats, anntims, revdats, revtims) |>
    select(-c('linkdt', 'linkenddt', 'p_count', 'd_count', 'pdf', 'fpi')) |>
    group_by(ticker, fpedats, estimator, analys) |>
    filter(row_number() == n()) |>
    ungroup()

# ==========================================
#         Link Estimates with Actuals
# ------------------------------------------

# Getting actual piece of data
ibes_act_db <- dbSendQuery(wrds,
                           paste0("select ticker, anndats as repdats, value as act, pends as fpedats, pdicity",
                                  " from ibes.actu_epsus",
                                  " where pends between '", begdate, "' and '", enddate, 
                                  "' and pdicity='QTR'"))

ibes_act <- dbFetch(ibes_act_db, n = -1) |>
    as_tibble() |>
    group_by(ticker, fpedats) |>
    # Remove duplicates with missing values in act
    slice(which.max(!is.na(act))) |>
    ungroup()

dbClearResult(ibes_act_db)

# Join with the estimate piece of the data
ibes1 <- ibes |>
    left_join(
        ibes_act, by = c('ticker','fpedats')
    ) |>
    as_tibble() |>
    mutate(
        dgap = repdats - anndats,
        flag = ifelse(dgap >= 0 & dgap <= 90 & !is.na(repdats) & !is.na(anndats), 1, 0)
    ) |>
    filter(flag == 1) |>
    select(-c(flag, dgap, pdicity))

# Select all relevant combinations of Permnos and Date
ibes1_dt1 <- unique(ibes1[c('permno', 'anndats')])
ibes1_dt2 <- unique(ibes1[c('permno', 'repdats')])
colnames(ibes1_dt2) <- c('permno', 'anndats')

ibes_anndats <- unique(rbind(ibes1_dt1, ibes1_dt2))

# unique anndats from ibes
uniq_anndats <- ibes_anndats |>
    as_tibble() |>
    select(anndats) |>
    distinct_all()

# unique trade dates from crsp.inddlyseriesdata (crsp.IndSeriesInfoHdr)
crsp_dats_db <- dbSendQuery(wrds, paste0("
                                         select indno, dlycaldt
                                         from crsp.inddlyseriesdata"))

crsp_dats <- dbFetch(crsp_dats_db, n = -1) |>
    as_tibble() |>
    mutate(prior_date = dlycaldt) |>
    rename(date = dlycaldt)

dbClearResult(crsp_dats_db)

# Create up to 5 days prior dates relative to anndats
for (i in 0:4) {
    
    uniq_anndats[[paste0("prior_", i)]] <- uniq_anndats$anndats - i
    
}

# Reshape (transpose) the dataframe for later join with CRSP trading dates
expand_anndats <- pivot_longer(uniq_anndats, -anndats, names_to = "prior", values_to = "prior_date")

# Merge with CRSP trading dates
tradedates <- expand_anndats |>
    left_join(
        crsp_dats, by = "prior_date"
    ) |>
    as_tibble() |>
    # Create the dgap (days gap) variable for min selection
    mutate(dgap = anndats - date) |>
    # Choosing the row with the smallest dgap for a given anndats
    group_by(anndats) |>
    filter(!is.na(dgap)) |>
    filter(dgap == min(dgap, na.rm = TRUE)) |>
    select(anndats, date) |> 
    arrange(anndats) |>
    ungroup()

# New dates formats
crsp_begdate_sql <- 20130101
crsp_enddate_sql <- 20251231

# extract CRSP adjustment factors
cfacshr_db <- dbSendQuery(wrds, paste0("select permno, yyyymmdd, dlycumfacshr",
                                       " from crsp.dsf_v2",
                                       " where yyyymmdd between '", 
                                       crsp_begdate_sql, "' and '", crsp_enddate_sql, "'"))

cfacshr <- dbFetch(cfacshr_db, n = -1) |>
    as_tibble()

dbClearResult(cfacshr_db)

ibes_anndats <- ibes_anndats |>
    as_tibble() |>
    left_join(
        tradedates, by = "anndats"
    ) |>
    left_join(
        cfacshr |>
            rename(date = yyyymmdd, cfacshr = dlycumfacshr) |>
            mutate(date = as.Date(as.character(date), format = "%Y%m%d")), by = c("permno", "date")
    )

# ==========================================
#       Adjust Estimates with CFACSHR
# ------------------------------------------

# Put the estimate on the same per share basis as
# company reported EPS using CRSP Adjustment factors. 
# New_value is the estimate adjusted to be on the 
# same basis with reported earnings.
ibes1 <- ibes1 |>
    inner_join(ibes_anndats, by = c("permno", "anndats")) |>
    as_tibble() |>
    select(-c("anndats", "date")) |>
    rename(cfacshr_ann = cfacshr) |> 
    inner_join(
        ibes_anndats |>
            mutate(repdats = anndats), by = c("permno", "repdats")
    ) |>
    as_tibble() |>
    select(-c("anndats", "date")) |>
    rename(cfacshr_rep = cfacshr) |>
    mutate(new_value = (cfacshr_rep / cfacshr_ann) * value) |>
    # Sanity check: there should be one most recent estimate for 
    # a given firm-fiscal period end combination 
    arrange(ticker, fpedats, estimator, analys) |>
    distinct_all()

# Compute the median forecast based on estimates in the 90 days prior to the EAD
grp_permno <- ibes1 |>
    drop_na(act) |> # Python group by remove missing values
    group_by(ticker, fpedats, basis, repdats, act) |>
    summarise(permno = max(permno)) |>
    select(ticker, fpedats, basis, repdats, act, permno) |>
    ungroup()

medest <- ibes1 |>
    drop_na(act) |>
    group_by(ticker, fpedats, basis, repdats, act) |>
    summarize(
        median = median(new_value, na.rm = TRUE),
        count = sum(!is.na(new_value))
    ) |>
    inner_join(
        grp_permno, by = c("ticker", "fpedats", "basis", "repdats", "act")
    ) |>
    rename(medest = median, numest = count) |> 
    select(ticker, fpedats, basis, repdats, act, medest, numest, permno) |>
    ungroup() 

# ==========================================
#          Merge with Compustat Data
# ------------------------------------------

# get items from fundq
fundq_db <- dbSendQuery(wrds, paste0("select gvkey, fyearq, fqtr, conm, datadate, rdq, epsfxq, epspxq, cshoq, prccq, 
                                      ajexq, spiq, cshprq, cshfdq, saleq, atq, fyr, datafqtr, cshoq*prccq as mcap  
                                      from comp.fundq 
                                      where consol='C' and popsrc='D' and indfmt='INDL' and datafmt='STD'
                                      and datadate between '", crsp_begdate, "' and '", crsp_enddate, 
                                     "'"))

fundq <- dbFetch(fundq_db, n = -1) |>
    as_tibble() |>
    filter((atq > 0 | !is.na(saleq)) & !is.na(datafqtr))

dbClearResult(fundq_db)

# Calculate link date ranges for given GVKEY and ticker combination
gvkey_mindt1 <- gvkey |>
    drop_na(gvkey, ticker) |>
    group_by(gvkey, ticker) |>
    filter(linkdt == min(linkdt)) |>
    rename(mindate = linkdt) |>
    select(gvkey, ticker, mindate) |>
    ungroup()

gvkey_maxdt1 <- gvkey |>
    drop_na(gvkey, ticker) |>
    group_by(gvkey, ticker) |>
    filter(linkenddt == max(linkenddt)) |>
    rename(maxdate = linkenddt) |>
    select(gvkey, ticker, maxdate) |>
    ungroup()

gvkey_dt1 <- gvkey_mindt1 |>
    inner_join(
        gvkey_maxdt1, 
        by = c("gvkey", "ticker")
    ) 

# Use the date range to merge
comp <- fundq |> 
    left_join(
        gvkey_dt1, by = 'gvkey', relationship = "many-to-many"
    ) |>
    filter(!is.na(ticker) & (datadate <= maxdate) & (datadate >= mindate)) |>
    # Merge with the median estimates
    left_join(
        medest |>
            mutate(datadate = fpedats), by = c("ticker", "datadate")
    ) |>
    # Sort data and drop duplicates
    arrange(gvkey, fqtr, fyearq, ticker) |>
    distinct_all()

# ==========================================
#                Calculate SUEs
# ------------------------------------------

# Block handling lag eps
sue <- comp |>
    arrange(gvkey, fqtr, fyearq) |>
    group_by(gvkey, fqtr) |>
    mutate(
        # Handling same qtr previous year
        diff_fyearq = fyearq - lag(fyearq, 1),
        laggvkey = lag(gvkey, 1),
        lagadj = ifelse(diff_fyearq == 1, lag(ajexq, 1), NA),
        lageps_p = ifelse(diff_fyearq == 1, lag(epspxq, 1), NA),
        lageps_d = ifelse(diff_fyearq == 1, lag(epsfxq, 1), NA),
        lagshr_p = ifelse(diff_fyearq == 1, lag(cshprq, 1), NA),
        lagshr_d = ifelse(diff_fyearq == 1, lag(cshfdq, 1), NA),
        lagspiq = ifelse(diff_fyearq == 1, lag(spiq, 1), NA),
        # Handling first gvkey
        lagadj = ifelse(gvkey != laggvkey, NA, lagadj),
        lageps_p = ifelse(gvkey != laggvkey, NA, lageps_p),
        lageps_d = ifelse(gvkey != laggvkey, NA, lageps_d),
        lagshr_p = ifelse(gvkey != laggvkey, NA, lagshr_p),
        lagshr_d = ifelse(gvkey != laggvkey, NA, lagshr_d),
        lagspiq = ifelse(gvkey != laggvkey, NA, lagspiq),
        # Handling reporting basis 
        # Basis = P and missing are treated the same
        basis = ifelse(is.na(basis), "missing", basis),
        actual1 = ifelse(basis == "D", epsfxq / ajexq, epspxq / ajexq),
        actual2 = ifelse(basis == 'D', (ifelse(is.na(epsfxq), 0, epsfxq) - ifelse(is.na(0.65 * spiq / cshfdq), 0, 0.65 * spiq / cshfdq)) / ajexq, (ifelse(is.na(epspxq), 0, epspxq) - ifelse(is.na(0.65 * spiq / cshprq), 0, 0.65 * spiq / cshprq)) / ajexq),
        expected1 = ifelse(basis == "D", lageps_d / lagadj, lageps_p / lagadj),
        expected2 = ifelse(basis == "D", (ifelse(is.na(lageps_d), 0, lageps_d) - ifelse(is.na(0.65 * lagspiq / lagshr_d), 0, 0.65 * lagspiq / lagshr_d)) / lagadj, (ifelse(is.na(lageps_p), 0, lageps_p) - ifelse(is.na(0.65 * lagspiq / lagshr_p), 0, 0.65 * lagspiq / lagshr_p)) / lagadj),
        # SUE calculations
        sue1 = (actual1 - expected1) / (prccq / ajexq),
        sue2 = (actual2 - expected2) / (prccq / ajexq),
        sue3 = (act - medest) / prccq
    ) |>
    select(
        'ticker', 'permno', 'gvkey', 'conm', 'fyearq', 'fqtr', 'fyr', 'datadate', 'repdats', 'rdq', 'sue1', 'sue2', 'sue3', 'basis', 'act', 'medest', 'numest', 'prccq', 'mcap'
    ) |>
    mutate(basis = ifelse(basis == "missing", NA, basis)) |>
    ungroup()

# Shifting the announcement date to be the next trading day
# Defining the day after the following quarterly EA as leadrdq1

# unique rdq 
uniq_rdq <- comp |> 
    select(rdq) |>
    drop_na(rdq) |>
    distinct_all()

# Create up to 5 days post rdq relative to rdq
for (i in 0:4) {
    
    uniq_rdq[[paste0("post_", i)]] <- uniq_rdq$rdq + i
    
}

# Reshape (transpose) for later join with crsp trading dates
expand_rdq <- pivot_longer(uniq_rdq, -rdq, names_to = "post", values_to = "post_date")

# Merge with CRSP trading dates
eads1 <- expand_rdq |>
    left_join(
        crsp_dats |>
            mutate(post_date = date),
        by = "post_date"
    ) |>
    mutate(
        # create the dgap (days gap) variable for min selection
        dgap = date - rdq
    ) |>
    drop_na(rdq) |>
    group_by(rdq) |>
    filter(!is.na(dgap)) |>
    slice(1) |>
    select(rdq, post, post_date, date, dgap) |>
    rename(rdq1 = date) |>
    arrange(rdq) |>
    ungroup() 

# Create sue_final
sue_final <- sue |>
    left_join(
        eads1 |>
            select(rdq, rdq1),
        by = "rdq"
    ) |>
    arrange(gvkey, desc(fyearq), desc(fqtr)) |>
    distinct_all()

#  Filter from Livnat & Mendenhall (2006):
#
# - Earnings announcement date is reported in Compustat                   
# - The price per share is available from Compustat at fiscal quarter end  
# - Price is greater than $1                                              
# - The market (book) equity at fiscal quarter end is available and is  
#   EADs in Compustat and in IBES (if available) should not differ by more  
#   than one calendar day larger than $5 mil.                              
esurprises <- sue_final |>
    mutate(
        leadrdq1 = lag(rdq1, 1),
        leadgvkey = lag(gvkey, 1),
        leadgvkey = ifelse(is.na(leadgvkey), NaN, leadgvkey),
        leadrdq1 = if_else(gvkey == leadgvkey, lag(rdq1, 1), floor_date(rdq1 + 1, "month") %m+% months(3) - 1),
        leadgvkey = if_else(leadgvkey == "NaN", NA, leadgvkey),
        dgap = ifelse(is.na(repdats - rdq), 0, repdats - rdq)
    ) |>
    filter(
        # Keep NAs created by the filter condition
        (rdq1 != leadrdq1) |> replace_na(TRUE),
        # Various conditioning for filtering
        (!is.na(sue1) & !is.na(sue2) & is.na(repdats)) | (!is.na(repdats) & (dgap <= 1) & (dgap >= -1)),
        (!is.na(rdq) & (prccq > 1) & (mcap > 5))
    ) |>
    select(
        'gvkey', 'ticker', 'permno', 'conm', 'dgap', 'fyearq', 'fqtr', 'datadate', 
        'fyr', 'rdq', 'rdq1', 'leadrdq1', 'repdats', 'mcap', 'medest', 'act', 'numest', 'sue1', 'sue2', 'sue3'
    )

# Export earnings surprises
write_rds(esurprises, "data/esurprises.rds")
