library(dplyr)
library(soql)
library(RSocrata)

endpoint <- 'https://www.dallasopendata.com/resource/qv6i-rri7.json'
token <- readChar('APPTOKEN.txt', nchars = 25)
query <- soql() %>%
  soql_add_endpoint(endpoint) %>%
  soql_select('distinct year1, month1, count(*)') %>%
  soql_order('year1, month1') %>%
  soql_group('year1, month1')

incidents <- read.socrata(query, token)
