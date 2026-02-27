WITH stocks AS (
    SELECT
        stock_id
        , product_id
        , store_id
        , quantity
    FROM {{ ref('stg_local_bike__stocks') }}
),

products AS (
    SELECT
        product_id
        , product_price
    FROM {{ ref('int_local_bike__dim_products') }}
),

product_store_orders AS (
    SELECT
        product_id
        , store_id
        , total_quantity_sold
    FROM {{ ref('int_local_bike__product_store_orders') }}
)

SELECT
    s.stock_id
    , s.product_id
    , s.store_id

    -- stock
    , s.quantity                                AS current_stock_qty
    , s.quantity * p.product_price              AS inventory_value

    -- flags
    , CASE WHEN s.quantity = 0 
        THEN TRUE ELSE FALSE END                AS is_out_of_stock
    , CASE WHEN pso.total_quantity_sold IS NULL 
        THEN TRUE ELSE FALSE END                AS has_never_sold

FROM stocks s
LEFT JOIN products p 
    USING (product_id)
LEFT JOIN product_store_orders pso 
    USING (product_id, store_id)