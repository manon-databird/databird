WITH customer_orders AS (
    SELECT
        customer_id
        , order_id
        , order_date
        , is_shipped
        , is_completed
        , order_net_revenue
        , order_gross_revenue
        , order_item_count
        , order_total_quantity
        , order_has_discount
    FROM {{ ref('int_local_bike__customer_orders') }}
),

customers AS (
    SELECT
        customer_id
        , customer_name
        , customer_city
        , customer_state
    FROM {{ ref('int_local_bike__dim_customers') }}
),

customer_metrics AS (
    SELECT
        customer_id

        -- orders
        , COUNT(DISTINCT order_id)                                              AS total_orders
        , COUNT(DISTINCT CASE WHEN is_shipped THEN order_id END)                AS shipped_orders
        , COUNT(DISTINCT CASE WHEN is_completed THEN order_id END)              AS completed_orders

        -- revenue
        , SUM(order_net_revenue)                                                AS total_revenue
        , AVG(order_net_revenue)                                                AS avg_order_value
        , MIN(order_net_revenue)                                                AS min_order_value
        , MAX(order_net_revenue)                                                AS max_order_value

        -- items
        , SUM(order_item_count)                                                 AS total_items_purchased
        , AVG(order_item_count)                                                 AS avg_items_per_order

        -- dates
        , MIN(order_date)                                                       AS first_order_date
        , MAX(order_date)                                                       AS last_order_date
        , DATE_DIFF(MAX(order_date), MIN(order_date), DAY)                      AS customer_lifetime

        -- flags
        , CASE WHEN COUNT(DISTINCT order_id) = 1 THEN TRUE ELSE FALSE END       AS is_one_time_buyer
        , CASE WHEN COUNT(DISTINCT order_id) >= 2 THEN TRUE ELSE FALSE END      AS has_repeat_purchase

    FROM customer_orders
    GROUP BY customer_id
)


SELECT
    -- identity
    c.customer_id
    , c.customer_name
    , c.customer_city
    , c.customer_state

    -- order metrics
    , cm.total_orders
    , cm.shipped_orders
    , cm.completed_orders
    , cm.total_revenue 
    , cm.avg_order_value
    , cm.min_order_value
    , cm.max_order_value
    , cm.total_items_purchased
    , cm.avg_items_per_order
    , cm.first_order_date
    , cm.last_order_date
    , cm.customer_lifetime

    -- calculated
    , cm.total_revenue / NULLIF(cm.total_orders, 0)         AS revenue_per_order
    , cm.total_items_purchased / NULLIF(cm.total_orders, 0) AS items_per_order

    , CASE
        WHEN cm.customer_lifetime > 0
        THEN cm.total_orders / (cm.customer_lifetime / 30.0)
        ELSE 0
    END AS orders_per_month

    -- flags
    , cm.is_one_time_buyer
    , cm.has_repeat_purchase

    -- customer type
    , CASE
        WHEN cm.total_orders = 1             THEN 'One-Time Buyer'
        WHEN cm.total_orders BETWEEN 2 AND 3 THEN 'Repeat Buyer'
        WHEN cm.total_orders >= 4            THEN 'Loyal Customer'
    END AS customer_type

    -- clv tier
    , CASE
        WHEN cm.total_revenue >= PERCENTILE_CONT(cm.total_revenue, 0.75) OVER() THEN 'High Value'
        WHEN cm.total_revenue >= PERCENTILE_CONT(cm.total_revenue, 0.50) OVER() THEN 'Medium-High Value'
        WHEN cm.total_revenue >= PERCENTILE_CONT(cm.total_revenue, 0.25) OVER() THEN 'Medium-Low Value'
        ELSE 'Low Value'
    END AS clv_tier

    -- segment
    , CASE
        WHEN cm.total_orders >= 4 AND cm.total_revenue >= PERCENTILE_CONT(cm.total_revenue, 0.75) OVER()
            THEN 'VIP - Loyal & High Value'
        WHEN cm.total_orders >= 4
            THEN 'Loyal - Regular Value'
        WHEN cm.total_orders BETWEEN 2 AND 3 AND cm.total_revenue >= PERCENTILE_CONT(cm.total_revenue, 0.50) OVER()
            THEN 'Repeat - High Potential'
        WHEN cm.total_orders BETWEEN 2 AND 3
            THEN 'Repeat - Standard'
        WHEN cm.total_orders = 1 AND cm.total_revenue >= PERCENTILE_CONT(cm.total_revenue, 0.75) OVER()
            THEN 'One-Time - High Value'
        ELSE 'One-Time - Standard'
    END AS customer_segment

    -- lifetime stage
    , CASE
        WHEN cm.customer_lifetime <= 30  THEN 'New Customer (0-1 month)'
        WHEN cm.customer_lifetime <= 90  THEN 'Recent Customer (1-3 months)'
        WHEN cm.customer_lifetime <= 180 THEN 'Established (3-6 months)'
        WHEN cm.customer_lifetime <= 365 THEN 'Long-term (6-12 months)'
        ELSE 'Veteran (12+ months)'
    END AS customer_lifetime_stage

FROM customer_metrics cm
LEFT JOIN customers c USING (customer_id)
