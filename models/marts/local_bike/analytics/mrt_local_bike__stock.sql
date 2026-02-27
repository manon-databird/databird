WITH stock_snapshot AS (
    SELECT
        stock_id
        , product_id
        , store_id
        , current_stock_qty
        , inventory_value
        , is_out_of_stock
        , has_never_sold
    FROM {{ ref('int_local_bike__fct_stock_snapshot') }}
),

products AS (
    SELECT
        product_id
        , product_name
        , category_name
        , brand_name
        , model_year
        , product_price
    FROM {{ ref('int_local_bike__dim_products') }}
),

stores AS (
    SELECT
        store_id
        , store_name
        , store_city
        , store_state
    FROM {{ ref('int_local_bike__dim_stores') }}
)

SELECT
    -- store
    st.store_id
    , st.store_name
    , st.store_city
    , st.store_state

    -- product
    , p.product_id
    , p.product_name
    , p.category_name
    , p.brand_name
    , p.model_year
    , p.product_price

    -- stock
    , s.current_stock_qty
    , s.inventory_value
    , s.is_out_of_stock
    , s.has_never_sold

    -- stock status
    , CASE
        WHEN s.current_stock_qty = 0   THEN 'Out of Stock'
        WHEN s.current_stock_qty <= 5  THEN 'Critical Low'
        WHEN s.current_stock_qty <= 10 THEN 'Low Stock'
        WHEN s.current_stock_qty <= 50 THEN 'Adequate'
        WHEN s.current_stock_qty <= 100 THEN 'High Stock'
        ELSE 'Overstock'
      END AS stock_status

    -- alert priority
    , CASE
        WHEN s.current_stock_qty = 0                          THEN 1
        WHEN s.current_stock_qty <= 5                         THEN 2
        WHEN s.current_stock_qty <= 10                        THEN 3
        WHEN s.has_never_sold AND s.current_stock_qty > 0     THEN 4
        WHEN s.current_stock_qty > 100                        THEN 5
        ELSE NULL
      END AS alert_priority

FROM stock_snapshot s
LEFT JOIN products p  USING (product_id)
LEFT JOIN stores st   USING (store_id)