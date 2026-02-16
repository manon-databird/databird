
WITH order_total AS (
    SELECT 
    o.order_id
    , o.customer_id
    , o.order_date
    , o.is_shipped
    , o.is_completed 
    , SUM(oi.net_revenue) AS order_revenue
    , COUNT(oi.order_item_id) AS order_item_count
FROM {{ref('int_local_bike__orders_enriched')}} o
LEFT JOIN {{ref('int_local_bike__order_items_enriched')}} oi USING (order_id)
GROUP BY 1,2,3,4,5
), 

customer_metrics AS (
    SELECT 
    customer_id

    -- orders
    , COUNT(DISTINCT order_id) AS total_orders
    , COUNT(DISTINCT CASE WHEN is_shipped THEN order_id END) AS shipped_orders

    -- revenue
    , SUM(order_revenue) AS total_revenue
    , AVG(order_revenue) AS avg_order_value
    , MIN(order_revenue) AS min_order_value
    , MAX(order_revenue) AS max_order_value
    
    -- item
    , SUM(order_item_count) AS total_items_purchased
    , AVG(order_item_count) AS avg_items_per_order

    -- date
    , MIN(order_date) AS first_order_date
    , MAX(order_date) AS last_order_date
    , DATE_DIFF(MAX(order_date), MIN(order_date), DAY) AS customer_lifetime
    
    FROM order_total
    GROUP BY customer_id
    ), 

customers_info AS (
    SELECT DISTINCT 
        customer_id
        , customer_name
        , customer_city
        , customer_state
    FROM {{ref('int_local_bike__orders_enriched')}} 
) 

SELECT 
    -- customer info
    ci.customer_id
    , ci.customer_name
    , ci.customer_city
    , ci.customer_state

    -- orders metrics
    , cm.total_orders
    , cm.shipped_orders
    , cm.total_revenue
    , cm.avg_order_value
    , cm.min_order_value
    , cm.max_order_value

    -- item metrics
    , cm.total_items_purchased
    , cm.avg_items_per_order

    -- date metrics
    , cm.first_order_date
    , cm.last_order_date
    , cm.customer_lifetime

    -- check
    , CASE WHEN cm.total_orders = 1 THEN TRUE ELSE FALSE END AS is_one_time_buyer
    , CASE WHEN cm.total_orders >= 2 THEN TRUE ELSE FALSE END AS has_repeat_purchase
    
FROM customer_metrics cm 
LEFT JOIN customers_info ci USING (customer_id)
ORDER BY cm.total_revenue DESC
