SELECT

    --IDs
    p.product_id
    , p.brand_id
    , p.category_id

    -- info
    , p.product_name
    , b.brand_name
    , cat.category_name
    , p.model_year
    , p.product_price

FROM {{ ref('stg_local_bike__products')}} p 
LEFT JOIN {{ ref('stg_local_bike__brands')}} b 
    USING (brand_id)
LEFT JOIN {{ ref('stg_local_bike__categories')}} cat
    USING (category_id)