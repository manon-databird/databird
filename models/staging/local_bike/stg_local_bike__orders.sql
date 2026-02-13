with 

source as (

    select * from {{ source('local_bike', 'orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        order_status,
        order_date,
        required_date,
        CAST(shipped_date AS DATE) AS shipped_date,
        store_id,
        staff_id

    from source

)

select * from renamed