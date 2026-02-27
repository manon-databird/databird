WITH order_items AS (
    SELECT
        product_id
        , order_id
        , order_item_id
        , order_date
        , store_id
        , item_quantity
        , gross_revenue
        , net_revenue
        , discount_amount
    FROM {{ ref('int_local_bike__fct_order_items') }}
),

orders AS (
    SELECT
        order_id
        , is_shipped
    FROM {{ ref('int_local_bike__fct_orders') }}
)

SELECT
    oi.product_id
    , oi.store_id

    -- volume
    , COUNT(DISTINCT oi.order_id)       AS total_orders
    , COUNT(DISTINCT oi.order_item_id)  AS total_line_items
    , SUM(oi.item_quantity)             AS total_quantity_sold

    -- revenue
    , SUM(oi.gross_revenue)             AS total_gross_revenue
    , SUM(oi.net_revenue)               AS total_net_revenue
    , SUM(oi.discount_amount)           AS total_discount_amount

    -- dates
    , MIN(oi.order_date)                AS first_sale_date
    , MAX(oi.order_date)                AS last_sale_date

FROM order_items oi
LEFT JOIN orders o USING (order_id)
GROUP BY
    oi.product_id
    , oi.store_id