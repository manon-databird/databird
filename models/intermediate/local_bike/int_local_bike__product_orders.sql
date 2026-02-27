
SELECT
    product_id

    -- volume
    , COUNT(DISTINCT order_id)       AS total_orders
    , COUNT(DISTINCT order_item_id)  AS total_line_items
    , SUM(item_quantity)             AS total_quantity_sold

    -- revenue
    , SUM(gross_revenue)             AS total_gross_revenue
    , SUM(net_revenue)               AS total_net_revenue
    , SUM(discount_amount)           AS total_discount_amount
    , AVG(net_revenue)               AS avg_revenue_per_line

    -- pricing
    , AVG(item_price)                AS avg_selling_price
    , MIN(item_price)                AS min_selling_price
    , MAX(item_price)                AS max_selling_price
    , AVG(discount_rate)             AS avg_discount_rate

    -- dates
    , MIN(order_date)                AS first_sale_date
    , MAX(order_date)                AS last_sale_date
    , DATE_DIFF(MAX(order_date), MIN(order_date), DAY) AS sales_period_days

FROM {{ref('int_local_bike__fct_order_items')}}
GROUP BY product_id