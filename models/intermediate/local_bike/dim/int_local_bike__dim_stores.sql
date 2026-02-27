SELECT 
  store_id
  , store_name
  , store_city
  , store_state
  
FROM {{ref('stg_local_bike__stores')}}