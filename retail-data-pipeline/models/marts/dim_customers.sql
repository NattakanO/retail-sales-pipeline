{{ config(materialized='table') }}

select 
  CustomerID,
  Country,
  count(distinct InvoiceNo) as lifetime_orders,
  sum(revenue) as lifetime_revenue,
  min(order_timestamp) as first_order,
  max(order_timestamp) as last_order
from {{ ref('stg_orders') }}
where CustomerID is not null
group by 1, 2