with 

source as (

    select * from {{ source('local_bike', 'customers') }}

),

renamed as (

    select
        customer_id,
        first_name,
        last_name,
        phone,
        email,
        street,
        city,
        state,
        CAST(zip_code AS string) AS zip_code

    from source

)

select * from renamed