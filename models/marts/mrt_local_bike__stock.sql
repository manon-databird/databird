SELECT
  store_id
  , store_name
  , store_city
  , store_state

  -- prod
  , product_id
  , product_name
  , category_name
  , brand_name
  , model_year
  , product_price
  
  -- stock
  , current_stock_qty
  , inventory_value
  , is_out_of_stock
  , has_never_sold
  
  -- stock_status
  , CASE 
      WHEN current_stock_qty = 0 THEN 'Out of Stock'
      WHEN current_stock_qty <= 5 THEN 'Critical Low'
      WHEN current_stock_qty <= 10 THEN 'Low Stock'
      WHEN current_stock_qty <= 50 THEN 'Adequate'
      WHEN current_stock_qty <= 100 THEN 'High Stock'
      ELSE 'Overstock'
    END AS stock_status
  
 
  -- priority
  , CASE 
      WHEN current_stock_qty = 0 THEN 1
      WHEN current_stock_qty <= 5 THEN 2
      WHEN current_stock_qty <= 10 THEN 3
      WHEN has_never_sold AND current_stock_qty > 0 THEN 4
      WHEN current_stock_qty > 100 THEN 5
      ELSE NULL
    END AS alert_priority
  

FROM {{ ref('int_local_bike__stocks')}}