with 

source as (

    select * from {{ source('local_bike', 'stores') }}

),

renamed as (

    select
        store_id,
        store_name,
        phone,
        email,
        street,
        city AS store_city,
        state AS store_state,
        CAST(zip_code AS STRING) AS zip_code

    from source

)

select * from renamed