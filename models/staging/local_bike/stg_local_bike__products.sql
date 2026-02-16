with 

source as (

    select * from {{ source('local_bike', 'products') }}

),

renamed as (

    select
        product_id,
        product_name,
        brand_id,
        category_id,
        model_year,
        list_price AS product_price

    from source

)

select * from renamed