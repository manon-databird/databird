SELECT
  o.order_id,
  o.customer_id,
  o.store_id,
  o.order_date,
  o.order_status,
  o.staff_id,
  c.city,
  c.state,
  SUM(o.item_quantity) AS total_item_quantity,
  SUM(o.CA_brut) AS total_CA_brut,
  SUM(o.CA_net) AS total_CA_net
FROM {{ ref("int_local_bike__order_items_order")}} o
LEFT JOIN `dbt_local_bike_dev.stg_local_bike__customers` c
  USING (customer_id)
GROUP BY 
o.order_id,
  o.customer_id,
  o.store_id,
  o.order_date,
  o.order_status,
  o.staff_id,
  c.city,
  c.state