with cte as(
SELECT *
FROM {{ source('stocks_data', 'stock') }}
)

select * from cte
