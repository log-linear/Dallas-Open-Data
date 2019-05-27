SELECT DISTINCT
    zip_code
    ,COUNT(*) as num_occurences

FROM https://www.dallasopendata.com/resource/qv6i-rri7.json

WHERE servyr < 2019

GROUP BY zip_code

ORDER BY zip_code ASC

LIMIT 1000
