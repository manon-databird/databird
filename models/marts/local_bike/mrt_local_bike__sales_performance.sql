WITH sales_details AS (
    SELECT 
        o.order_id
        , o.order_date

        -- store
        , o.store_id

        -- staff
        , o.staff_id

        -- customer
        , o.customer_id

        -- orders
        , o.days_to_ship
        , o.variance_day_from_required
        , o.is_shipped
        , o.is_ship_date_missing
        , o.is_required_date_missing

        -- items
        , oi.item_quantity
        , oi.net_revenue
        , oi.gross_revenue
        , oi.discount_amount
        , oi.discount_rate

    FROM {{ ref('int_local_bike__fct_orders')}} o 
    INNER JOIN {{ ref('int_local_bike__fct_order_items')}} oi 
        USING (order_id)
    WHERE o.is_shipped = TRUE 
)

SELECT

    -- time
    order_date

    -- day cat
    , CASE EXTRACT(DAYOFWEEK FROM order_date)
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END AS day_name
    
    , CASE 
        WHEN EXTRACT(DAYOFWEEK FROM order_date) IN (1,7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type

     -- business
    , store_id
    , staff_id

    -- agreg
    , COUNT(DISTINCT order_id) AS total_orders
    , COUNT(DISTINCT customer_id) AS unique_customers
    , SUM(net_revenue) AS total_revenue
    , SUM(gross_revenue) AS gross_revenue
    , SUM(discount_amount) AS total_discounts
    , SUM(item_quantity) AS total_units_sold

    -- KPIs

    , SUM(net_revenue) / NULLIF(COUNT(DISTINCT order_id), 0 ) AS avg_order_value
    , SUM(net_revenue) / NULLIF(SUM(item_quantity), 0) AS revenue_per_unit
    , SUM(discount_amount) / NULLIF(SUM(gross_revenue), 0) AS discount_rate

    -- delivery_perf
    , AVG(CASE WHEN is_ship_date_missing = FALSE THEN days_to_ship END) AS avg_delivery_days
    , COUNT(DISTINCT CASE WHEN variance_day_from_required IS NOT NULL 
                     AND variance_day_from_required <= 0 THEN order_id END) AS on_time_orders
    , COUNT(DISTINCT CASE WHEN variance_day_from_required IS NOT NULL 
                     AND variance_day_from_required > 0  THEN order_id END) AS late_orders
    , COUNT(DISTINCT CASE WHEN variance_day_from_required IS NOT NULL 
                     AND variance_day_from_required <= 0 THEN order_id END) / NULLIF (COUNT(DISTINCT order_id), 0) AS on_time_delivery_rate

    , CASE
        WHEN COUNT(DISTINCT CASE WHEN variance_day_from_required IS NOT NULL 
                     AND variance_day_from_required <= 0 THEN order_id END) / NULLIF(COUNT(DISTINCT order_id), 0) >= 0.95 THEN 'Excellent'
        WHEN COUNT(DISTINCT CASE WHEN variance_day_from_required IS NOT NULL 
                     AND variance_day_from_required <= 0 THEN order_id END) / NULLIF(COUNT(DISTINCT order_id), 0) >= 0.85 THEN 'Good'
        WHEN COUNT(DISTINCT CASE WHEN variance_day_from_required IS NOT NULL 
                     AND variance_day_from_required <= 0 THEN order_id END) / NULLIF(COUNT(DISTINCT order_id), 0) >= 0.70 THEN 'Needs Improvement'
        ELSE 'Poor'
    END AS delivery_performance_category


    -- discount
    , CASE 
        WHEN SUM(discount_amount) / NULLIF(SUM(gross_revenue),0) >= 0.20 THEN 'Big discount'
        WHEN SUM(discount_amount) / NULLIF(SUM(gross_revenue),0) >= 0.10 THEN 'Moderate discount'
        WHEN SUM(discount_amount) / NULLIF(SUM(gross_revenue),0) > 0 THEN 'Light discount'
        ELSE 'no discount'
    END AS discount_strategy

    FROM sales_details
    GROUP BY
        order_date
        , store_id
        , staff_id

