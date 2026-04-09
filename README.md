# Post-Earnings Announcement Drift (PEAD)

[![R](https://img.shields.io/badge/R-%3E%3D4.1-276DC3?logo=r&logoColor=white)](https://www.r-project.org/)
[![WRDS](https://img.shields.io/badge/Data-WRDS-003366)](https://wrds-www.wharton.upenn.edu/)

An R pipeline to construct standardized earnings surprises (SUEs) for U.S. equity
research via WRDS.

## Overview

This repository constructs standardized earnings surprises (SUEs) for U.S.
publicly traded firms, following the methodology of Livnat and Mendenhall
(2006, JAR). The pipeline links IBES analyst forecasts with Compustat fundamentals
and CRSP prices to produce three SUE measures per firm-quarter, which can be
used to study the post-earnings announcement drift anomaly.

The code is an R port of the original Python notebook by Qingyi (Freda) Song
Drechsler (WRDS, 2019/2021), updated to use the new CRSP v2 data tables.

## Data Sources

All data are retrieved from WRDS (Wharton Research Data Services):

| Source | Tables used |
|---|---|
| CRSP v2 | `crsp.dsf_v2`, `crsp.inddlyseriesdata`, `crsp.stocknames`, `crsp.msp500list` |
| Compustat | `comp.fundq`, `comp.security`, `crsp.ccmxpf_linktable` |
| IBES | `ibes.detu_epsus`, `ibes.actu_epsus`, `ibes.id` |

Sample period: **January 2013 – December 2025**.

## Pipeline

```
CRSP–IBES link (get_iclink)
        ↓
CCM link table  →  GVKEY–Ticker–PERMNO universe
        ↓
IBES unadjusted estimates  →  latest estimate per analyst per firm-quarter
        ↓
IBES unadjusted actuals    →  merged on (ticker, fiscal period end)
        ↓
CRSP adjustment factors    →  adjust estimates to same per-share basis as actuals
        ↓
Compustat fundq            →  merge fundamentals and compute lagged EPS
        ↓
SUE calculations (sue1, sue2, sue3)
        ↓
data/esurprises.rds
```

### SUE Measures

| Variable | Description |
|---|---|
| `sue1` | `(actual_EPS − expected_EPS) / (price / adj_factor)` — no special items adjustment |
| `sue2` | Same as `sue1` but with 65% of special items per share removed from both actual and expected EPS |
| `sue3` | `(IBES actual − median analyst forecast) / price` — analyst-expectation based |

`sue1` and `sue2` are Compustat-based; `sue3` is IBES-based.

## Requirements

- R packages: `tidyverse`, `lubridate`, `scales`, `RSQLite`, `dbplyr`, `RPostgres`
- WRDS account with access to CRSP, Compustat, and IBES

### Credentials

Set WRDS credentials as environment variables before running:

```bash
export WRDS_USER=your_username
export WRDS_PASSWORD=your_password
```

Or add them to your `.Renviron` file:

```
WRDS_USER=your_username
WRDS_PASSWORD=your_password
```

## References

* Livnat, J., and Mendenhall, R. R. (2006). Comparing the post-earnings
announcement drift for surprises calculated from analyst and time series
forecasts. *Journal of Accounting Research*, 44(1), 177–205.
