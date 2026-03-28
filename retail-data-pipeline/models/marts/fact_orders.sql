{{ config(materialized='table') }}

select
  InvoiceNo,
  order_timestamp,
  CustomerID,
  StockCode,
  Description,
  quantity,
  unit_price,
  revenue
from {{ ref('stg_orders') }}