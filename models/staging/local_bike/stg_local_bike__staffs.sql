with 

source as (

    select * from {{ source('local_bike', 'staffs') }}

),

renamed as (

    select
        staff_id,
        first_name,
        last_name,
        email,
        phone,
        active,
        store_id,
        CAST(manager_id AS INT) AS manager_id

    from source

)

select * from renamed