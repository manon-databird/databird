SELECT
  oi.order_item_id,
  oi.order_id,
  oi.product_id,
  o.customer_id,
  o.store_id,
  o.staff_id,
  o.order_date,
  o.order_status,
  oi.item_quantity,
  oi.item_price,
  oi.discount,
  oi.item_quantity * oi.item_price as CA_brut,
  oi.item_quantity * oi.item_price * (1 - oi.discount) as CA_net,

FROM {{ref('stg_local_bike__order_items')}} AS oi
LEFT JOIN {{ ref("stg_local_bike__orders")}} AS o
  USING (order_id)