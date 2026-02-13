with 

source as (

    select * from {{ source('local_bike', 'stocks') }}

),

renamed as (

    select
        store_id,
        product_id,
        quantity

    from source

)

select * from renamed