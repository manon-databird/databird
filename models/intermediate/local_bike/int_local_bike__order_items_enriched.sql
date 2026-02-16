SELECT
    -- granularité : une ligne = un produit de commande
    -- IDs
    oi.order_id
    , oi.item_id
    , oi.order_item_id
    , oi.product_id

    -- order item elements
    , oi.item_quantity
    , oi.item_price
    , oi.discount 

    -- product
    , p.product_name
    , p.brand_id
    , p.category_id
    , p.model_year
    , p.product_price 

    -- category
    , cat.category_name
    
    -- brand
    , b.brand_name

    -- revenue_calc
    , oi.item_price * oi.item_quantity AS gross_revenue
    , oi.item_price * oi.item_quantity * (1 - COALESCE(oi.discount, 0)) AS net_revenue
    , oi.item_price * oi.item_quantity * COALESCE(oi.discount, 0) AS discount_amount
    , COALESCE(oi.discount, 0) AS discount_rate

    -- price_calc
    , p.product_price - oi.item_price AS price_reduction_amount
    , CASE
        WHEN p.product_price > 0
        THEN (p.product_price - oi.item_price) / p.product_price
        ELSE 0 
      END AS price_reduction_rate 
    
    -- check
    , CASE WHEN COALESCE(oi.discount, 0) > 0 THEN TRUE ELSE FALSE END AS has_discount
    , CASE WHEN oi.item_price < p.product_price THEN TRUE ELSE FALSE END AS is_below_list_price
    , CASE WHEN oi.item_quantity > 1 THEN TRUE ELSE FALSE END is_multi_item

FROM {{ref('stg_local_bike__order_items')}} oi 
LEFT JOIN {{ref('stg_local_bike__products')}} p USING (product_id)
LEFT JOIN {{ref('stg_local_bike__categories')}} cat USING (category_id)
LEFT JOIN {{ref('stg_local_bike__brands')}} b USING (brand_id)