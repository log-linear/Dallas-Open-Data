library(tidyverse)
library(soql)
library(RSocrata)
library(forecast)

endpoint <- 'https://www.dallasopendata.com/resource/7h2m-3um5.json'
token <- readChar('APPTOKEN.txt', nchars = 25)
query <- soql() %>%
  soql_add_endpoint(endpoint) %>%
  soql_select('date_extract_y(intake_date) AS year,
               date_extract_m(intake_date) AS month,
               count(*) AS intake_count') %>%
  soql_where("intake_date < '2019-09-01'") %>%
  soql_order('year, month') %>%
  soql_group('year, month')

intake <- read.socrata(query, token)

intake_ts <- ts(intake$intake_count, frequency = 12)
plot.ts(intake_ts)

intake_decomp <- stl(intake_ts, s.window = "period")
plot(intake_decomp)

forecasts <- forecast(intake_decomp)
plot(forecasts)
