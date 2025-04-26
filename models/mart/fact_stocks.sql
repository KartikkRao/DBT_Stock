{{
    config(
        materialized = 'incremental',
        unique_key = ['company_symbol', 'trade_date'],
        incremental_strategy = 'merge'
    )
}}

with cte as(
    select
        s.trade_date,
        s.company_symbol,
        c.Marketcap,
        c.country,
        s.open,
        s.close,
        s.high,
        s.low,
        s.volume
    from {{ ref('stg_stock')}} as s 
    left join {{ref('dim_company')}} as c 
    on s.company_symbol = c.company_symbol
)
select * from cte 