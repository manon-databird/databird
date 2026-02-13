with 

source as (

    select * from {{ source('local_bike', 'brands') }}

),

renamed as (

    select
        brand_id,
        brand_name

    from source

)

select * from renamed