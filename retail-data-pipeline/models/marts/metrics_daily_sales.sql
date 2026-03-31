{{ config(materialized='table') }}

select
  DATE(order_timestamp) as order_date,  
  Country,
  count(distinct InvoiceNo) as orders,
  count(*) as order_lines,
  sum(revenue) as daily_revenue,
  avg(revenue) as avg_order_value
from {{ ref('stg_orders') }}
group by 1, 2
order by 1 desc, 2