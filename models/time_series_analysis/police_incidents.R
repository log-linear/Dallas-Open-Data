library(tidyverse)
library(soql)
library(RSocrata)
library(forecast)

endpoint <- 'https://www.dallasopendata.com/resource/qv6i-rri7.json'
token <- readChar('APPTOKEN.txt', nchars = 25)
query <- soql() %>%
  soql_add_endpoint(endpoint) %>%
  soql_select('date_extract_y(date1) AS year, 
               date_extract_m(date1) AS month, 
               count(*) AS num_incidents') %>%
  soql_where("date1 BETWEEN '2014-06-01' AND '2019-08-01'") %>%
  soql_order('year, month') %>%
  soql_group('year, month')

incidents <- read.socrata(query, token)

incidents_ts <- ts(incidents$num_incidents, frequency = 12, start = c(2014,7))
incidents_ts

plot.ts(incidents_ts)

decomp <- stl(incidents_ts, s.window = "period")

forecasts <- forecast(decomp)
plot(forecasts)
