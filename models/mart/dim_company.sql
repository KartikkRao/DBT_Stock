{{ config(
    materialized='incremental',
    unique_key = 'company_symbol',
    incremental_strategy='merge'  
) }}

with cte as (
select 
    Symbol as company_symbol,
    AssetType,
    Name,
    Exchange,
    Currency,
    Country,
    Sector,
    Industry,
    MarketCapitalization as Marketcap,
    load_date
from {{ source('stocks_data', 'company') }}

{% if is_incremental() %}
where load_date > (select max(load_date) from {{this}})
{% endif %}

)

select * from cte

