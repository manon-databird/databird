WITH customers AS (
  SELECT * FROM {{ ref('int_local_bike__customers_aggregated') }}
)

SELECT
  -- client
  customer_id,
  customer_name,
  customer_city,
  customer_state,
  
  -- metrics
  total_orders,
  shipped_orders,
  total_revenue AS customer_lifetime_value,
  avg_order_value,
  min_order_value,
  max_order_value,
  total_items_purchased,
  avg_items_per_order,
  first_order_date,
  last_order_date,
  customer_lifetime,
  total_revenue / NULLIF(total_orders, 0) AS revenue_per_order,
  total_items_purchased / NULLIF(total_orders, 0) AS items_per_order,
  
  -- check
  is_one_time_buyer,
  has_repeat_purchase,
  
  --purchase_frequency
  CASE
    WHEN customer_lifetime IS NOT NULL AND customer_lifetime > 0 
    THEN total_orders / (customer_lifetime / 30.0)
    ELSE 0
  END AS orders_per_month,
  
  -- customer_type
  CASE
    WHEN total_orders = 1 THEN 'One-Time Buyer'
    WHEN total_orders BETWEEN 2 AND 3 THEN 'Repeat Buyer'
    WHEN total_orders >= 4 THEN 'Loyal Customer'
  END AS customer_type,
  
  -- clv_value
  CASE
    WHEN total_revenue >= PERCENTILE_CONT(total_revenue, 0.75) OVER() THEN 'High Value'
    WHEN total_revenue >= PERCENTILE_CONT(total_revenue, 0.50) OVER() THEN 'Medium-High Value'
    WHEN total_revenue >= PERCENTILE_CONT(total_revenue, 0.25) OVER() THEN 'Medium-Low Value'
    ELSE 'Low Value'
  END AS clv_tier,
  
  -- segment
  CASE
    WHEN total_orders >= 4 AND total_revenue >= PERCENTILE_CONT(total_revenue, 0.75) OVER() 
      THEN 'VIP - Loyal & High Value'
    WHEN total_orders >= 4 
      THEN 'Loyal - Regular Value'
    WHEN total_orders BETWEEN 2 AND 3 AND total_revenue >= PERCENTILE_CONT(total_revenue, 0.50) OVER()
      THEN 'Repeat - High Potential'
    WHEN total_orders BETWEEN 2 AND 3 
      THEN 'Repeat - Standard'
    WHEN total_orders = 1 AND total_revenue >= PERCENTILE_CONT(total_revenue, 0.75) OVER()
      THEN 'One-Time - High Value'
    ELSE 'One-Time - Standard'
  END AS customer_segment,
  
  -- customer life time stage
  CASE
    WHEN customer_lifetime <= 30 THEN 'New Customer (0-1 month)'
    WHEN customer_lifetime <= 90 THEN 'Recent Customer (1-3 months)'
    WHEN customer_lifetime <= 180 THEN 'Established (3-6 months)'
    WHEN customer_lifetime <= 365 THEN 'Long-term (6-12 months)'
    ELSE 'Veteran (12+ months)'
  END AS customer_lifetime_stage

  FROM customers