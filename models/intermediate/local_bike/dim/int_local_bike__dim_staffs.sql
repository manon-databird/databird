SELECT 
    staff_id
    , CONCAT(first_name, ' ', last_name) AS staff_name
    , active
    , store_id
    , CAST(manager_id AS INT) AS manager_id
FROM {{ ref('stg_local_bike__staffs')}}