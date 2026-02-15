SELECT
    -- IDs
    o.order_id
    , o.customer_id
    , o.store_id
    , o.staff_id

    -- orders
    , o.order_status
    , o.order_date
    , o.required_date
    , o.shipped_date

    -- customers
    , CONCAT(c.first_name, ' ', c.last_name) AS customer_name
    , c.city AS customer_city
    , c.state AS customer_state

    -- store
    , st.store_name
    , st.city AS store_city
    , st.state AS store_state

    -- staff
    , CONCAT(staff.first_name, ' ', staff.last_name) AS staff_name
    , staff.active
    , staff.manager_id

    -- date calc
    , DATE_DIFF(o.shipped_date, o.order_date, DAY) AS days_to_ship
    , DATE_DIFF(o.required_date, o.order_date, DAY) AS days_expected_delivery
    , DATE_DIFF(o.shipped_date, o.required_date, DAY) AS variance_day_from_required

    -- checks
    , CASE WHEN o.shipped_date IS NOT NULL THEN TRUE ELSE FALSE END AS is_shipped
    , CASE WHEN o.order_status = 4 THEN TRUE ELSE FALSE END AS is_completed
    , CASE WHEN c.state = st.state THEN TRUE ELSE FALSE END AS is_same_state
    , CASE WHEN c.city = st.city THEN TRUE ELSE FALSE END AS is_same_city

    -- for time analysis
    , EXTRACT(YEAR FROM o.order_date) AS order_year
    , EXTRACT(MONTH FROM o.order_date) AS order_month
    , EXTRACT(QUARTER FROM o.order_date) AS order_quarter
    , FORMAT_DATE('%Y-%m', o.order_date) AS order_year_month

FROM {{ ref('stg_local_bike__orders')}} o 
LEFT JOIN {{ ref('stg_local_bike__customers')}} c USING (customer_id)
LEFT JOIN {{ ref('stg_local_bike__stores')}} st USING (store_id)
LEFT JOIN {{ ref('stg_local_bike__staffs')}} staff USING (staff_id)