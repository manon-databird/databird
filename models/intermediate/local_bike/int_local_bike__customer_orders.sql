WITH order_items AS (
    SELECT
        order_id
        , order_item_id
        , product_id
        , customer_id
        , store_id
        , item_quantity
        , gross_revenue
        , net_revenue
        , discount_amount
        , has_discount
    FROM {{ ref('int_local_bike__fct_order_items') }}
),

orders AS (
    SELECT
        order_id
        , customer_id
        , order_date
        , is_shipped
        , is_completed
    FROM {{ ref('int_local_bike__fct_orders') }}
)

SELECT
    o.order_id
    , o.customer_id
    , o.order_date
    , o.is_shipped
    , o.is_completed

    -- revenue
    , SUM(oi.gross_revenue)   AS order_gross_revenue
    , SUM(oi.net_revenue)     AS order_net_revenue
    , SUM(oi.discount_amount) AS order_discount_amount

    -- items
    , COUNT(oi.order_item_id)         AS order_item_count
    , SUM(oi.item_quantity)           AS order_total_quantity
    , COUNT(DISTINCT oi.product_id)   AS order_distinct_products
    , COUNTIF(oi.has_discount)        AS order_discounted_items

    -- flag
    , CASE WHEN SUM(oi.discount_amount) > 0 THEN TRUE ELSE FALSE END AS order_has_discount

    FROM orders o
    LEFT JOIN order_items oi USING (order_id)
    GROUP BY
        o.order_id,
        o.customer_id,
        o.order_date,
        o.is_shipped,
        o.is_completed
