{{ config(materialized='view') }}

select
  InvoiceNo,
  StockCode,
  Description,
  safe_cast(Quantity as int64) as quantity,
  safe_cast(UnitPrice as float64) as unit_price,
  parse_datetime('%m/%d/%Y %H:%M', InvoiceDate) as order_timestamp, 
  CustomerID,
  Country,
  safe_cast(Quantity as int64) * safe_cast(UnitPrice as float64) as revenue
from {{ source('retail', 'raw_orders') }}
where InvoiceNo not like 'C%' 
  and safe_cast(Quantity as int64) > 0 
  and safe_cast(UnitPrice as float64) > 0