with 

source as (

    select * from {{ source('local_bike', 'orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        order_status,
        SAFE_CAST(NULLIF(TRIM(CAST(order_date AS STRING)), 'NULL') AS DATE) AS order_date,
        SAFE_CAST(NULLIF(TRIM(CAST(required_date AS STRING)), 'NULL') AS DATE) AS required_date,
        SAFE_CAST(NULLIF(TRIM(CAST(shipped_date AS STRING)), 'NULL') AS DATE) AS shipped_date,
        store_id,
        staff_id

    from source

)

select * from renamed