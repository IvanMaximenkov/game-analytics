USE basic_sql;
WITH payment_ranks AS (
    SELECT 
        user_id,
        amount,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY CAST(event_time AS DATETIME)) as payment_number
    FROM fact_table
    WHERE event_name = 'payment'
)
SELECT 
    SUM(amount) as total_revenue_2nd_to_7th_payment
FROM payment_ranks
WHERE payment_number BETWEEN 2 AND 7;

WITH installs AS (
    SELECT user_id, CAST(event_time AS DATETIME) as install_time
    FROM fact_table
    WHERE event_name = 'install'
),
user_events_28d AS (
    SELECT 
        i.user_id,
        f.event_name
    FROM installs i
    LEFT JOIN fact_table f ON i.user_id = f.user_id 
    WHERE CAST(f.event_time AS DATETIME) >= i.install_time 
      AND CAST(f.event_time AS DATETIME) <= i.install_time + INTERVAL 28 DAY
)
SELECT 
    COUNT(CASE WHEN e.event_name = 'login' THEN 1 END) / NULLIF(COUNT(DISTINCT i.user_id), 0) as avg_logins,
    COUNT(CASE WHEN e.event_name = 'payment' THEN 1 END) / NULLIF(COUNT(DISTINCT i.user_id), 0) as avg_payments
FROM installs i
LEFT JOIN user_events_28d e ON i.user_id = e.user_id;

WITH RECURSIVE days_series (day_num) AS (
    SELECT 0
    UNION ALL
    SELECT day_num + 1 FROM days_series WHERE day_num < 9
),
installs AS (
    SELECT user_id, CAST(event_time AS DATETIME) as install_time
    FROM fact_table
    WHERE event_name = 'install'
),
user_days AS (
    SELECT 
        i.user_id,
        i.install_time,
        d.day_num,
        (i.install_time + INTERVAL d.day_num DAY) as current_day_start
    FROM installs i
    CROSS JOIN days_series d
),
metrics_per_user_day AS (
    SELECT 
        ud.day_num,
        ud.user_id,
        COALESCE(SUM(CASE 
            WHEN CAST(f.event_time AS DATETIME) > ud.current_day_start 
             AND CAST(f.event_time AS DATETIME) <= ud.current_day_start + INTERVAL 7 DAY
            THEN f.amount ELSE 0 END), 0) as revenue_next_7d,
        COALESCE(SUM(CASE 
            WHEN CAST(f.event_time AS DATETIME) > ud.current_day_start 
             AND CAST(f.event_time AS DATETIME) <= ud.current_day_start + INTERVAL 30 DAY
            THEN f.amount ELSE 0 END), 0) as revenue_next_30d
    FROM user_days ud
    LEFT JOIN fact_table f 
        ON ud.user_id = f.user_id 
        AND f.event_name = 'payment'
        AND CAST(f.event_time AS DATETIME) > ud.current_day_start 
    GROUP BY ud.day_num, ud.user_id
)
SELECT 
    day_num as lifetime_day,
    AVG(revenue_next_7d) as avg_revenue_next_7d,
    AVG(revenue_next_30d) as avg_revenue_next_30d
FROM metrics_per_user_day
GROUP BY day_num
ORDER BY day_num;