USE basic_sql;

-- Task 1.
-- Total revenue from the 2nd to the 7th payment for each user.
WITH payment_events AS (
    SELECT
        user_id,
        amount,
        CAST(event_time AS DATETIME) AS event_datetime
    FROM fact_table
    WHERE event_name = 'payment'
),
ranked_payments AS (
    SELECT
        user_id,
        amount,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY event_datetime
        ) AS payment_number
    FROM payment_events
)
SELECT
    SUM(amount) AS total_revenue_2nd_to_7th_payment
FROM ranked_payments
WHERE payment_number BETWEEN 2 AND 7;

-- Task 2.
-- Average number of logins and payments per user in the first 28 days after install.
WITH installs AS (
    SELECT
        user_id,
        CAST(event_time AS DATETIME) AS install_time
    FROM fact_table
    WHERE event_name = 'install'
),
events_28d AS (
    SELECT
        i.user_id,
        f.event_name
    FROM installs AS i
    LEFT JOIN fact_table AS f
        ON f.user_id = i.user_id
       AND CAST(f.event_time AS DATETIME) >= i.install_time
       AND CAST(f.event_time AS DATETIME) <= i.install_time + INTERVAL 28 DAY
)
SELECT
    SUM(CASE WHEN e.event_name = 'login' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(DISTINCT i.user_id), 0) AS avg_logins_per_user_28d,
    SUM(CASE WHEN e.event_name = 'payment' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(DISTINCT i.user_id), 0) AS avg_payments_per_user_28d
FROM installs AS i
LEFT JOIN events_28d AS e
    ON e.user_id = i.user_id;

-- Task 3.
-- Average predicted revenue for the next 7 and 30 days for lifetime days 0-9.
WITH RECURSIVE days_series AS (
    SELECT 0 AS day_num
    UNION ALL
    SELECT day_num + 1
    FROM days_series
    WHERE day_num < 9
),
installs AS (
    SELECT
        user_id,
        CAST(event_time AS DATETIME) AS install_time
    FROM fact_table
    WHERE event_name = 'install'
),
user_days AS (
    SELECT
        i.user_id,
        d.day_num,
        i.install_time + INTERVAL d.day_num DAY AS current_day_start
    FROM installs AS i
    CROSS JOIN days_series AS d
),
metrics_per_user_day AS (
    SELECT
        ud.day_num,
        ud.user_id,
        COALESCE(SUM(CASE
            WHEN CAST(f.event_time AS DATETIME) > ud.current_day_start
             AND CAST(f.event_time AS DATETIME) <= ud.current_day_start + INTERVAL 7 DAY
            THEN f.amount
            ELSE 0
        END), 0) AS revenue_next_7d,
        COALESCE(SUM(CASE
            WHEN CAST(f.event_time AS DATETIME) > ud.current_day_start
             AND CAST(f.event_time AS DATETIME) <= ud.current_day_start + INTERVAL 30 DAY
            THEN f.amount
            ELSE 0
        END), 0) AS revenue_next_30d
    FROM user_days AS ud
    LEFT JOIN fact_table AS f
        ON f.user_id = ud.user_id
       AND f.event_name = 'payment'
       AND CAST(f.event_time AS DATETIME) > ud.current_day_start
    GROUP BY
        ud.day_num,
        ud.user_id
)
SELECT
    day_num AS lifetime_day,
    AVG(revenue_next_7d) AS avg_revenue_next_7d,
    AVG(revenue_next_30d) AS avg_revenue_next_30d
FROM metrics_per_user_day
GROUP BY day_num
ORDER BY day_num;
