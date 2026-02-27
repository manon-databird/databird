SELECT
    -- Grain : 1 ligne = 1 produit dans 1 commande
    
    oi.order_item_id -- PK

    -- FK
    , oi.order_id
    , oi.product_id
    , o.customer_id
    , o.store_id

    -- Measures
     ,oi.item_quantity
    , oi.item_price
    , COALESCE(oi.discount, 0) AS discount_rate

    -- Date
    , o.order_date
    , CAST(FORMAT_DATE('%Y%m%d', o.order_date) AS INT64) AS date_id

    -- Revenue metrics
    , oi.item_price * oi.item_quantity AS gross_revenue
    , oi.item_price * oi.item_quantity * (1 - COALESCE(oi.discount, 0)) AS net_revenue
    , oi.item_price * oi.item_quantity * COALESCE(oi.discount, 0) AS discount_amount

    --  flags 
    , CASE WHEN COALESCE(oi.discount, 0) > 0 THEN TRUE ELSE FALSE END AS has_discount
    , CASE WHEN oi.item_quantity > 1 THEN TRUE ELSE FALSE END AS is_multi_item

FROM {{ ref('stg_local_bike__order_items') }} oi
LEFT JOIN {{ ref('stg_local_bike__orders') }} o USING (order_id)
