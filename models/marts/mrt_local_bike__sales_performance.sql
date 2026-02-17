WITH sales_details AS (
    SELECT 
        o.order_id
        , o.order_date
        , o.order_year 
        , o.order_month
        , o.order_quarter
        , o.order_year_month

        -- store
        , o.store_id
        , o.store_name
        , o.store_state

        -- staff
        , o.staff_id
        , o.staff_name

        -- customer
        , o.customer_id
        , o.customer_name
        , o.customer_state

        -- orders
        , o.days_to_ship
        , o.variance_day_from_required
        , o.is_shipped
        , o.is_same_state
        , o.is_ship_date_missing
        , o.is_required_date_missing

        -- items
        , oi.category_name
        , oi.brand_name
        , oi.product_name
        , oi.item_quantity
        , oi.net_revenue
        , oi.gross_revenue
        , oi.discount_amount
        , oi.discount_rate

    FROM {{ ref('int_local_bike__orders_enriched')}} o 
    INNER JOIN {{ ref('int_local_bike__order_items_enriched')}} oi 
        USING (order_id)
    WHERE o.is_shipped = TRUE 
)

SELECT

    -- time
    order_date
    , order_year
    , order_month
    , order_quarter
    , order_year_month

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
    , store_name
    , store_state
    , staff_id
    , staff_name
    , category_name
    , brand_name
    , customer_state

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

    -- geography
    , SUM(CASE WHEN is_same_state THEN net_revenue ELSE 0 END) AS in_state_revenue
    , SUM(CASE WHEN is_same_state = FALSE THEN net_revenue ELSE 0 END) AS out_of_state_revenue
    , SUM(CASE WHEN is_same_state THEN net_revenue ELSE 0 END) / NULLIF(SUM(net_revenue),0) AS in_state_revenue_rate

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
        , order_year
        , order_month
        , order_quarter
        , order_year_month
        , store_id
        , store_name
        , store_state
        , staff_id
        , staff_name
        , category_name
        , brand_name
        , customer_state
