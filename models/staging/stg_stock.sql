{{ config(
    materialized='table'  
) }}

with cte as(
SELECT 
    timestamp as trade_date,
    open,
    high,
    low,
    close,
    volume,
    company as company_symbol,
    load_date
FROM {{ source('stocks_data', 'stock') }}
where 
    timestamp is not null and 
    open is not null and 
    high is not null and 
    low is not null and
    close is not null and 
    volume is not null and 
    company is not null and 
    load_date is not null and 
    load_date BETWEEN DATE('2025-04-20') AND DATE('2025-04-26')  --subject to change the end date will become the start date for next run
)
select * from cte

-- We can also do group by for monthly and yearly data depedning on need