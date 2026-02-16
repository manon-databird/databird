WITH products AS (
    SELECT
        product_id
        , product_name
        , brand_id
        , brand_name
        , category_id
        , category_name
        , model_year
        , product_price
    FROM {{ref('int_local_bike__order_items_enriched')}}
    GROUP BY 
    product_id
        , product_name
        , brand_id
        , brand_name
        , category_id
        , category_name
        , model_year
        , product_price
), 

stores AS (
    SELECT 
        store_id
        , store_name
        , store_city
        , store_state
    FROM {{ref('stg_local_bike__stores')}}
), 

-- sales
sales AS (
    SELECT 
        oi.product_id
        , o.store_id
        , SUM(oi.item_quantity) AS total_qty_sold
        , COUNT(DISTINCT o.order_id) AS total_orders
        , MIN(o.order_date) AS first_sale_date
        , MAX(o.order_date) AS last_sale_date
    FROM {{ref('int_local_bike__order_items_enriched')}} oi 
    INNER JOIN {{ref('int_local_bike__orders_enriched')}} o USING (order_id)
    WHERE o.is_shipped = TRUE
    GROUP BY
        oi.product_id
        , o.store_id
    )

SELECT 
    --IDs
    s.store_id
    , s.product_id

    -- store
    , st.store_name
    , st.store_city
    , st.store_state

    -- product
    , p.product_name
    , p.brand_name
    , p.category_name
    , p.model_year
    , p.product_price

    -- stock
    , s.quantity AS current_stock_qty
    , s.quantity * p.product_price AS inventory_value

    -- check
    , CASE WHEN s.quantity = 0 THEN TRUE ELSE FALSE END AS is_out_of_stock
    , CASE WHEN ats.total_qty_sold = 0 THEN TRUE ELSE FALSE END AS has_never_sold


FROM {{ref('stg_local_bike__stocks')}} s 
LEFT JOIN stores st USING (store_id)
LEFT JOIN products p USING (product_id)
LEFT JOIN sales ats ON (s.product_id = ats.product_id AND s.store_id = ats.store_id)