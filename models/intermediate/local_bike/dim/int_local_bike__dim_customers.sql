SELECT
    customer_id
    , CONCAT(first_name, ' ', last_name) AS customer_name
    , city AS customer_city
    , state AS customer_state
    
FROM {{ref('stg_local_bike__customers')}}