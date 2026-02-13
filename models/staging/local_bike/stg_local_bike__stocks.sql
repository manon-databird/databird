with 

source as (

    select * from {{ source('local_bike', 'stocks') }}

),

renamed as (

    select
        CONCAT(store_id, '_', product_id) AS stock_id,
        store_id,
        product_id,
        quantity

    from source

)

select * from renamed