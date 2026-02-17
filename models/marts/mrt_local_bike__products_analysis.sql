
SELECT
  product_id
  , product_name
  , brand_id
  , brand_name
  , category_id
  , category_name
  , model_year
  , product_price
  
  -- volume
  , total_orders
  , total_line_items
  , total_quantity_sold
  
  -- revenue
  , total_gross_revenue
  , total_net_revenue
  , total_discount_amount
  , avg_revenue_per_line
  
  -- price
  , avg_selling_price
  , min_selling_price
  , max_selling_price
  , avg_discount_rate
  , overall_dicount_rate
  , avg_price_reduction_amount
  
  -- time
  , first_sale_date
  , last_sale_date
  , sales_period_days
  , units_per_month
  , revenue_per_month
  , orders_per_month
  
  -- ranks
  , revenue_rank
  , quantity_rank
  , order_count_rank
  , category_revenue_rank
  , brand_revenue_rank
  , total_products
  , revenue_percentile
  
  -- stock velocity
  , CASE
      WHEN units_per_month >= 10 THEN 'Fast Mover'
      WHEN units_per_month >= 5 THEN 'Medium Mover'
      WHEN units_per_month >= 1 THEN 'Slow Mover'
      WHEN units_per_month > 0 THEN 'Very Slow Mover'
      ELSE 'No Movement'
    END AS velocity_category
  
  -- rank
  , CASE
      WHEN revenue_rank <= 20 THEN 'Top 20'
      WHEN revenue_rank <= 50 THEN 'Top 50'
      WHEN revenue_rank <= 100 THEN 'Top 100'
      WHEN revenue_percentile <= 0.20 THEN 'Top 20%'
      WHEN revenue_percentile <= 0.50 THEN 'Top 50%'
      ELSE 'Long Tail'
    END AS revenue_tier
  
  -- product Age
  , CASE
      WHEN model_year >= 2018 THEN '2018'
      WHEN model_year = 2017 THEN '1 year old'
      WHEN model_year BETWEEN 2015 AND 2016 THEN '2-3 Years Old'
      ELSE '4+ Years Old'
    END AS product_age_category
  
  -- perf
  , CASE
      WHEN revenue_rank <= 20 AND units_per_month >= 5 THEN 'Star Product'
      WHEN revenue_rank <= 100 AND units_per_month >= 3 THEN 'Solid Performer'
      WHEN revenue_rank > 100 AND units_per_month < 1 THEN 'Poor Performer'
      WHEN units_per_month = 0 THEN 'No Sales'
      ELSE 'Average Performer'
    END AS performance_classification
  
  -- profit
  , CASE
      WHEN avg_discount_rate < 0.10 AND units_per_month >= 5 THEN 'High Margin High Volume'
      WHEN avg_discount_rate < 0.10 AND units_per_month < 5 THEN 'High Margin Low Volume'
      WHEN avg_discount_rate >= 0.10 AND units_per_month >= 5 THEN 'Low Margin High Volume'
      WHEN avg_discount_rate >= 0.10 AND units_per_month < 5 THEN 'Low Margin Low Volume'
      ELSE 'Uncategorized'
    END AS profitability_quadrant
  
FROM {{ ref('int_local_bike__products')}}
