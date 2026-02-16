WITH product_sales AS (
    SELECT 
        oi.product_id
        , oi.product_name
        , oi.brand_id
        , oi.brand_name
        , oi.category_id
        , oi.category_name
        , oi.model_year
        , oi.product_price

        -- volume
        , COUNT(DISTINCT o.order_id) AS total_orders 
        , COUNT(DISTINCT oi.order_item_id) AS total_line_items
        , SUM(oi.item_quantity) AS total_quantity_sold

        -- revenue
        , SUM(oi.gross_revenue) AS total_gross_revenue
        , SUM(oi.net_revenue) AS total_net_revenue
        , SUM(oi.discount_amount) AS total_discount_amount
        , AVG(oi.net_revenue) AS avg_revenue_per_line 

        -- pricing
        , AVG(oi.item_price) AS avg_selling_price
        , MIN(oi.item_price) AS min_selling_price
        , MAX(oi.item_price) AS max_selling_price
        , AVG(oi.discount_rate) AS avg_discount_rate

        -- date
        , MIN(o.order_date) AS first_sale_date
        , MAX(o.order_date) AS last_sale_date
        , DATE_DIFF(MAX(o.order_date), MIN(o.order_date), DAY) AS sales_period_days

    FROM {{ref('int_local_bike__order_items_enriched')}} oi 
    INNER JOIN {{ref('int_local_bike__orders_enriched')}} o USING (order_id)
    WHERE o.is_shipped = TRUE
    GROUP BY oi.product_id
        , oi.product_name
        , oi.brand_id
        , oi.brand_name
        , oi.category_id
        , oi.category_name
        , oi.model_year
        , oi.product_price
),

-- units per period

sales_per_period AS(
    SELECT
        product_id
        
        -- units per month
        , CASE
            WHEN sales_period_days > 0
            THEN total_quantity_sold / (sales_period_days / 30)
            ELSE total_quantity_sold
          END AS units_per_month
        
        -- revenue per month
        , CASE
            WHEN sales_period_days > 0
            THEN total_net_revenue / (sales_period_days /30)
            ELSE total_net_revenue
          END AS revenue_per_month

        -- order per month
        , CASE 
            WHEN sales_period_days > 0
            THEN total_orders / (sales_period_days/30)
            ELSE total_orders 
          END AS orders_per_month

    FROM product_sales
), 

product_rank AS(
    SELECT
        product_id

        , ROW_NUMBER() OVER (ORDER BY total_net_revenue DESC) AS revenue_rank
        , ROW_NUMBER() OVER (ORDER BY total_quantity_sold DESC) AS quantity_rank
        , ROW_NUMBER() OVER (ORDER BY total_orders DESC) AS order_count_rank
        , ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY total_net_revenue DESC) AS category_revenue_rank
        , ROW_NUMBER() OVER (PARTITION BY brand_id ORDER BY total_net_revenue DESC) AS brand_revenue_rank
        , COUNT(*) OVER () AS total_products

    FROM product_sales
)

SELECT 
    ps.product_id
  , ps.product_name
  , ps.brand_id
  , ps.brand_name
  , ps.category_id
  , ps.category_name
  , ps.model_year
  , ps.product_price

    -- volume
  , ps.total_orders
  , ps.total_line_items
  , ps.total_quantity_sold

    -- revenue 
  , ps.total_gross_revenue
  , ps.total_net_revenue
  , ps.total_discount_amount
  , ps.avg_revenue_per_line

    -- discount rate
  , CASE
        WHEN ps.total_gross_revenue > 0
        THEN ps.total_discount_amount / ps.total_gross_revenue
        ELSE 0
    END AS overall_dicount_rate

    -- price metrics
  , ps.avg_selling_price
  , ps.min_selling_price
  , ps.max_selling_price
  , ps.avg_discount_rate
  , ps.product_price - ps.avg_selling_price AS avg_price_reduction_amount

  -- date
  , ps.first_sale_date
  , ps.last_sale_date
  , ps.sales_period_days

  -- volume
  , spp.units_per_month
  , spp.revenue_per_month
  , spp.orders_per_month

  --ranks
  , pr.revenue_rank
  , pr.quantity_rank
  , pr.order_count_rank
  , pr.category_revenue_rank
  , pr.brand_revenue_rank
  , pr.total_products

  --percentile
  , pr.revenue_rank / NULLIF(pr.total_products,0) AS revenue_percentile

FROM product_sales ps 
INNER JOIN sales_per_period spp USING (product_id)
INNER JOIN product_rank pr USING(product_id)
