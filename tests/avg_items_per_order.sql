{{
  config(
    severity='error'
  )
}}

SELECT
  customer_id,
  avg_items_per_order
FROM {{ ref('mrt_local_bike__customers_analysis') }}
WHERE avg_items_per_order < 0
