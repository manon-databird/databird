SELECT
    -- Grain: 1 row per order

    o.order_id, -- PK

    -- FK
    o.customer_id,
    o.store_id,
    o.staff_id,

    -- Dates
    o.order_date,
    o.required_date,
    o.shipped_date,

    CAST(FORMAT_DATE('%Y%m%d', o.order_date) AS INT64) AS order_date_id,

    -- Operational metrics
    DATE_DIFF(o.shipped_date, o.order_date, DAY) AS days_to_ship,
    DATE_DIFF(o.required_date, o.order_date, DAY) AS days_expected_delivery,
    DATE_DIFF(o.shipped_date, o.required_date, DAY) AS variance_day_from_required,

    -- Status flags
    CASE WHEN o.shipped_date IS NOT NULL THEN TRUE ELSE FALSE END AS is_shipped,
    CASE WHEN o.order_status = 4 THEN TRUE ELSE FALSE END AS is_completed,
    CASE WHEN o.required_date IS NULL THEN TRUE ELSE FALSE END AS is_required_date_missing,
    CASE WHEN o.shipped_date IS NULL THEN TRUE ELSE FALSE END AS is_ship_date_missing

FROM {{ ref('stg_local_bike__orders') }} o
