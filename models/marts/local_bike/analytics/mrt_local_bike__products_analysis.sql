WITH products AS (
    SELECT
        product_id
        , product_name
        , brand_id
        , brand_name
        , category_id
        , category_name
        , model_year
        , product_price
    FROM {{ ref('int_local_bike__dim_products') }}
),

product_orders AS (
    SELECT
        product_id
        , total_orders
        , total_line_items
        , total_quantity_sold
        , total_gross_revenue
        , total_net_revenue
        , total_discount_amount
        , avg_revenue_per_line
        , avg_selling_price
        , min_selling_price
        , max_selling_price
        , avg_discount_rate
        , first_sale_date
        , last_sale_date
        , sales_period_days
    FROM {{ ref('int_local_bike__product_orders') }}
),

product_metrics AS (
    SELECT
        product_id

        -- per period
        , CASE
            WHEN sales_period_days > 0
            THEN total_quantity_sold / (sales_period_days / 30.0)
            ELSE total_quantity_sold
          END AS units_per_month

        , CASE
            WHEN sales_period_days > 0
            THEN total_net_revenue / (sales_period_days / 30.0)
            ELSE total_net_revenue
          END AS revenue_per_month

        , CASE
            WHEN sales_period_days > 0
            THEN total_orders / (sales_period_days / 30.0)
            ELSE total_orders
          END AS orders_per_month

        -- discount
        , CASE
            WHEN total_gross_revenue > 0
            THEN total_discount_amount / total_gross_revenue
            ELSE 0
          END AS overall_discount_rate

        -- price reduction
        , product_price - avg_selling_price AS avg_price_reduction_amount

    FROM product_orders po
    LEFT JOIN products p USING (product_id)
),

product_ranks AS (
    SELECT
        product_id
        , ROW_NUMBER() OVER (ORDER BY total_net_revenue DESC)                       AS revenue_rank
        , ROW_NUMBER() OVER (ORDER BY total_quantity_sold DESC)                     AS quantity_rank
        , ROW_NUMBER() OVER (ORDER BY total_orders DESC)                            AS order_count_rank
        , ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY total_net_revenue DESC) AS category_revenue_rank
        , ROW_NUMBER() OVER (PARTITION BY brand_id ORDER BY total_net_revenue DESC)    AS brand_revenue_rank
        , COUNT(*) OVER ()                                                          AS total_products
    FROM product_orders po
    LEFT JOIN products p USING (product_id)
)


SELECT
    p.product_id
    , p.product_name
    , p.brand_id
    , p.brand_name
    , p.category_id
    , p.category_name
    , p.model_year
    , p.product_price

    -- volume
    , po.total_orders
    , po.total_line_items
    , po.total_quantity_sold

    -- revenue
    , po.total_gross_revenue
    , po.total_net_revenue
    , po.total_discount_amount
    , po.avg_revenue_per_line

    -- price
    , po.avg_selling_price
    , po.min_selling_price
    , po.max_selling_price
    , po.avg_discount_rate
    , pm.overall_discount_rate
    , pm.avg_price_reduction_amount

    -- time
    , po.first_sale_date
    , po.last_sale_date
    , po.sales_period_days
    , pm.units_per_month
    , pm.revenue_per_month
    , pm.orders_per_month

    -- ranks
    , pr.revenue_rank
    , pr.quantity_rank
    , pr.order_count_rank
    , pr.category_revenue_rank
    , pr.brand_revenue_rank
    , pr.total_products
    , pr.revenue_rank / NULLIF(pr.total_products, 0) AS revenue_percentile

    -- velocity
    , CASE
        WHEN pm.units_per_month >= 10 THEN 'Fast Mover'
        WHEN pm.units_per_month >= 5  THEN 'Medium Mover'
        WHEN pm.units_per_month >= 1  THEN 'Slow Mover'
        WHEN pm.units_per_month > 0   THEN 'Very Slow Mover'
        ELSE 'No Movement'
    END AS velocity_category

    -- revenue tier
    , CASE
        WHEN pr.revenue_rank <= 20  THEN 'Top 20'
        WHEN pr.revenue_rank <= 50  THEN 'Top 50'
        WHEN pr.revenue_rank <= 100 THEN 'Top 100'
        WHEN pr.revenue_rank / NULLIF(pr.total_products, 0) <= 0.20 THEN 'Top 20%'
        WHEN pr.revenue_rank / NULLIF(pr.total_products, 0) <= 0.50 THEN 'Top 50%'
        ELSE 'Long Tail'
    END AS revenue_tier

    -- product age
    , CASE
        WHEN p.model_year >= 2018            THEN '2018+'
        WHEN p.model_year = 2017             THEN '1 Year Old'
        WHEN p.model_year BETWEEN 2015 AND 2016 THEN '2-3 Years Old'
        ELSE '4+ Years Old'
    END AS product_age_category

    -- performance
    , CASE
        WHEN pr.revenue_rank <= 20  AND pm.units_per_month >= 5 THEN 'Star Product'
        WHEN pr.revenue_rank <= 100 AND pm.units_per_month >= 3 THEN 'Solid Performer'
        WHEN pr.revenue_rank > 100  AND pm.units_per_month < 1  THEN 'Poor Performer'
        WHEN pm.units_per_month = 0                             THEN 'No Sales'
        ELSE 'Average Performer'
    END AS performance_classification

    -- profitability
    , CASE
        WHEN po.avg_discount_rate < 0.10 AND pm.units_per_month >= 5 THEN 'High Margin High Volume'
        WHEN po.avg_discount_rate < 0.10 AND pm.units_per_month < 5  THEN 'High Margin Low Volume'
        WHEN po.avg_discount_rate >= 0.10 AND pm.units_per_month >= 5 THEN 'Low Margin High Volume'
        WHEN po.avg_discount_rate >= 0.10 AND pm.units_per_month < 5  THEN 'Low Margin Low Volume'
        ELSE 'Uncategorized'
    END AS profitability_quadrant

    FROM products p
    LEFT JOIN product_orders po      USING (product_id)
    LEFT JOIN product_metrics pm     USING (product_id)
    LEFT JOIN product_ranks pr       USING (product_id)
