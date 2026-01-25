-- Totals

SELECT 
    COUNT(DISTINCT s.user_id) as signed_users,
    ROUND(SUM(t.purchase_amount_usd)::numeric, 2) as total_revenue,
    ROUND(COUNT(DISTINCT CASE WHEN rr.accept_ts IS NOT NULL THEN rr.ride_id END)::DECIMAL / 
        NULLIF(COUNT(DISTINCT rr.ride_id), 0), 2) as successful_trips_rate,
    ROUND(AVG(rev.rating)::numeric, 2) as avg_rating
FROM signups s
LEFT JOIN ride_requests rr ON s.user_id = rr.user_id
LEFT JOIN transactions t ON rr.ride_id = t.ride_id
LEFT JOIN reviews rev ON rr.ride_id = rev.ride_id;



-- Time to first ride

WITH first_rides AS (
  SELECT 
    user_id,
    MIN(request_ts) AS first_ride_ts
  FROM ride_requests
  GROUP BY user_id
),
time_diffs AS (
  SELECT 
    EXTRACT(EPOCH FROM (fr.first_ride_ts - s.signup_ts)) / 86400 AS days_to_first_ride
  FROM signups s
  INNER JOIN first_rides fr ON s.user_id = fr.user_id
)
SELECT 
  COUNT(*) AS users_with_rides,
  ROUND(AVG(days_to_first_ride), 2) AS avg_days,
  ROUND(CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_first_ride) AS numeric), 2) AS median_days,
  ROUND(MIN(days_to_first_ride), 2) AS min_days,
  ROUND(MAX(days_to_first_ride), 2) AS max_days
FROM time_diffs;


-- Funnel analysis

WITH funnel AS (
  SELECT 1 AS step, 'Downloads' AS stage, COUNT(DISTINCT app_download_key) AS users FROM app_downloads
  UNION ALL
  SELECT 2, 'Signups', COUNT(DISTINCT user_id) FROM signups
  UNION ALL
  SELECT 3, 'Requested', COUNT(DISTINCT user_id) FROM ride_requests WHERE request_ts IS NOT NULL
  UNION ALL
  SELECT 4, 'Accepted', COUNT(DISTINCT user_id) FROM ride_requests WHERE accept_ts IS NOT NULL
  UNION ALL
  SELECT 5, 'Picked Up', COUNT(DISTINCT user_id) FROM ride_requests WHERE pickup_ts IS NOT NULL
  UNION ALL
  SELECT 6, 'Completed', COUNT(DISTINCT user_id) FROM ride_requests WHERE dropoff_ts IS NOT NULL
  UNION ALL
  SELECT 7, 'Paid', COUNT(DISTINCT r.user_id) FROM ride_requests r JOIN transactions t ON r.ride_id = t.ride_id
  UNION ALL
  SELECT 8, 'Reviewed', COUNT(DISTINCT user_id) FROM reviews
)
SELECT 
  stage,
  users,
  LAG(users) OVER (ORDER BY step) AS prev_users,
  users - LAG(users) OVER (ORDER BY step) AS change,
  ROUND(100.0 * users / LAG(users) OVER (ORDER BY step), 2) AS conversion_pct,
  ROUND(100.0 * (LAG(users) OVER (ORDER BY step) - users) / LAG(users) OVER (ORDER BY step), 2) AS drop_off_pct
FROM funnel
ORDER BY step;


-- Drop-off points

WITH funnel AS (
  SELECT 1 AS step, 'Downloads' AS stage, COUNT(DISTINCT app_download_key) AS users FROM app_downloads
  UNION ALL
  SELECT 2, 'Signups', COUNT(DISTINCT user_id) FROM signups
  UNION ALL
  SELECT 3, 'Requested', COUNT(DISTINCT user_id) FROM ride_requests WHERE request_ts IS NOT NULL
  UNION ALL
  SELECT 4, 'Accepted', COUNT(DISTINCT user_id) FROM ride_requests WHERE accept_ts IS NOT NULL
  UNION ALL
  SELECT 5, 'Picked Up', COUNT(DISTINCT user_id) FROM ride_requests WHERE pickup_ts IS NOT NULL
  UNION ALL
  SELECT 6, 'Completed', COUNT(DISTINCT user_id) FROM ride_requests WHERE dropoff_ts IS NOT NULL
  UNION ALL
  SELECT 7, 'Paid', COUNT(DISTINCT r.user_id) FROM ride_requests r JOIN transactions t ON r.ride_id = t.ride_id
  UNION ALL
  SELECT 8, 'Reviewed', COUNT(DISTINCT user_id) FROM reviews
),
funnel_metrics AS (
  SELECT 
    stage,
    users,
    LAG(users) OVER (ORDER BY step) AS prev_users,
    LAG(stage) OVER (ORDER BY step) AS prev_stage,
    ROUND(100.0 * (LAG(users) OVER (ORDER BY step) - users) / LAG(users) OVER (ORDER BY step), 2) AS drop_off_pct
  FROM funnel
)
SELECT 
  prev_stage || ' → ' || stage AS transition,
  prev_users AS users_start,
  users AS users_end,
  prev_users - users AS users_lost,
  drop_off_pct
FROM funnel_metrics
WHERE prev_stage IS NOT NULL
ORDER BY drop_off_pct DESC;


-- Cancellation

SELECT 
  CASE 
    WHEN cancel_ts IS NOT NULL AND accept_ts IS NULL THEN 'Cancelled before accept'
    WHEN cancel_ts IS NOT NULL AND pickup_ts IS NULL THEN 'Cancelled after accept, before pickup'
    WHEN cancel_ts IS NOT NULL AND dropoff_ts IS NULL THEN 'Cancelled during ride'
    ELSE 'Not cancelled'
  END AS cancellation_stage,
  COUNT(*) AS count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage,
  ROUND(AVG(EXTRACT(EPOCH FROM (cancel_ts - request_ts))/60), 2) AS avg_time_to_cancel_minutes
FROM ride_requests
GROUP BY 
  CASE 
    WHEN cancel_ts IS NOT NULL AND accept_ts IS NULL THEN 'Cancelled before accept'
    WHEN cancel_ts IS NOT NULL AND pickup_ts IS NULL THEN 'Cancelled after accept, before pickup'
    WHEN cancel_ts IS NOT NULL AND dropoff_ts IS NULL THEN 'Cancelled during ride'
    ELSE 'Not cancelled'
  END
ORDER BY count DESC;



WITH wait_time_analysis AS (
  SELECT 
    ride_id,
    EXTRACT(EPOCH FROM (accept_ts - request_ts)) / 60 AS wait_for_accept_minutes,
    EXTRACT(EPOCH FROM (pickup_ts - accept_ts)) / 60 AS wait_for_pickup_minutes,
    CASE 
      WHEN accept_ts IS NOT NULL THEN 1
      ELSE 0
    END AS was_accepted,
    CASE 
      WHEN pickup_ts IS NOT NULL THEN 1
      ELSE 0
    END AS was_picked_up,
    CASE 
      WHEN cancel_ts IS NOT NULL THEN 1
      ELSE 0
    END AS was_cancelled
  FROM ride_requests
  WHERE request_ts IS NOT NULL
)
SELECT 
  CASE 
    WHEN wait_for_accept_minutes <= 2 THEN '0-2 min'
    WHEN wait_for_accept_minutes <= 5 THEN '2-5 min'
    WHEN wait_for_accept_minutes <= 10 THEN '5-10 min'
    WHEN wait_for_accept_minutes <= 15 THEN '10-15 min'
    ELSE '15+ min'
  END AS wait_time_bucket,
  COUNT(*) AS total_requests,
  SUM(was_accepted) AS accepted,
  SUM(was_picked_up) AS picked_up,
  SUM(was_cancelled) AS cancelled
FROM wait_time_analysis
WHERE wait_for_accept_minutes IS NOT NULL
GROUP BY 
  CASE 
    WHEN wait_for_accept_minutes <= 2 THEN '0-2 min'
    WHEN wait_for_accept_minutes <= 5 THEN '2-5 min'
    WHEN wait_for_accept_minutes <= 10 THEN '5-10 min'
    WHEN wait_for_accept_minutes <= 15 THEN '10-15 min'
    ELSE '15+ min'
  END
ORDER BY MIN(wait_for_accept_minutes);


-- Waiting time analysis by day of week and hour
-- before accept
SELECT 
    TO_CHAR(request_ts, 'Day') as day_of_week,
    EXTRACT(DOW FROM request_ts) as day_number,
    EXTRACT(HOUR FROM request_ts) as hour_of_day,
    COUNT(*) as total_requests,
    COUNT(accept_ts) as accepted_requests,
    ROUND(AVG(EXTRACT(EPOCH FROM (accept_ts - request_ts)) / 60)::numeric, 2) as avg_waiting_minutes,
    ROUND(MIN(EXTRACT(EPOCH FROM (accept_ts - request_ts)) / 60)::numeric, 2) as min_waiting_minutes,
    ROUND(MAX(EXTRACT(EPOCH FROM (accept_ts - request_ts)) / 60)::numeric, 2) as max_waiting_minutes,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (accept_ts - request_ts)) / 60)::numeric, 2) as median_waiting_minutes
FROM 
    ride_requests
WHERE 
    accept_ts IS NOT NULL
    AND cancel_ts IS NULL
GROUP BY 
    TO_CHAR(request_ts, 'Day'),
    EXTRACT(DOW FROM request_ts),
    EXTRACT(HOUR FROM request_ts)
ORDER BY 
    day_number,
    hour_of_day;
    
-- before pickup
SELECT 
    TO_CHAR(accept_ts, 'Day') as day_of_week,
    EXTRACT(DOW FROM accept_ts) as day_number,
    EXTRACT(HOUR FROM accept_ts) as hour_of_day,
    COUNT(*) as total_accept,
    COUNT(pickup_ts) as pickup,
    ROUND(AVG(EXTRACT(EPOCH FROM (pickup_ts - accept_ts)) / 60)::numeric, 2) as avg_waiting_minutes,
    ROUND(MIN(EXTRACT(EPOCH FROM (pickup_ts - accept_ts)) / 60)::numeric, 2) as min_waiting_minutes,
    ROUND(MAX(EXTRACT(EPOCH FROM (pickup_ts - accept_ts)) / 60)::numeric, 2) as max_waiting_minutes,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (pickup_ts - accept_ts)) / 60)::numeric, 2) as median_waiting_minutes
FROM 
    ride_requests
WHERE 
    pickup_ts IS NOT NULL
    AND cancel_ts IS NULL
GROUP BY 
    TO_CHAR(accept_ts, 'Day'),
    EXTRACT(DOW FROM accept_ts),
    EXTRACT(HOUR FROM accept_ts)
ORDER BY 
    day_number,
    hour_of_day;


-- Platforms analysis

SELECT 
    ad.platform,
    COUNT(DISTINCT ad.app_download_key) as total_downloads,
    COUNT(DISTINCT s.user_id) as total_signups,
    COUNT(DISTINCT CASE WHEN r.dropoff_ts IS NOT NULL THEN r.ride_id END) as completed_rides,
    COUNT(DISTINCT rev.user_id) as users_with_reviews,
    ROUND(SUM(t.purchase_amount_usd)::numeric, 2) as total_revenue,
    ROUND(AVG(t.purchase_amount_usd)::numeric, 2) as avg_transaction_amount,
    ROUND(COALESCE(AVG(rev.rating), 0), 2) as avg_review_rating,
    ROUND(
        CASE 
            WHEN COUNT(DISTINCT ad.app_download_key) > 0 
            THEN COUNT(DISTINCT CASE WHEN r.dropoff_ts IS NOT NULL THEN r.user_id END)::numeric / 
                 COUNT(DISTINCT ad.app_download_key) * 100
            ELSE 0 
        END, 
        2
    ) as conversion_rate_pct
FROM app_downloads ad
LEFT JOIN signups s ON ad.app_download_key = s.session_id
LEFT JOIN ride_requests r ON s.user_id = r.user_id AND r.dropoff_ts IS NOT NULL
LEFT JOIN reviews rev ON s.user_id = rev.user_id
LEFT JOIN transactions t ON r.ride_id = t.ride_id 
GROUP BY ad.platform
ORDER BY total_downloads DESC;

-- lifetime per platform
WITH user_activity AS (
    SELECT 
        s.user_id,
        a.platform,
        MIN(r.request_ts) AS first_ride_date,
        MAX(r.request_ts) AS last_ride_date,
        COUNT(DISTINCT r.ride_id) AS total_rides
    FROM signups s
    JOIN app_downloads a ON s.session_id = a.app_download_key
    LEFT JOIN ride_requests r ON s.user_id = r.user_id
    WHERE r.request_ts IS NOT NULL
    GROUP BY s.user_id, a.platform
)
SELECT 
    platform,
    COUNT(*) AS active_users,
    ROUND(AVG(total_rides), 2) AS avg_rides_per_user,
    ROUND(AVG(EXTRACT(EPOCH FROM (last_ride_date - first_ride_date)))/86400, 2) AS avg_lifetime_days,
    COUNT(CASE WHEN total_rides >= 10 THEN 1 END) AS users_with_10plus_rides,
    ROUND(100.0 * COUNT(CASE WHEN total_rides >= 10 THEN 1 END) / COUNT(*), 2) AS power_user_rate
FROM user_activity
GROUP BY platform
ORDER BY avg_rides_per_user DESC;


-- Age demographics, platform, and behavior of users

SELECT 
  s.age_range,
  COUNT(DISTINCT s.user_id) AS total_users,
  COUNT(DISTINCT r.user_id) AS active_users,
  COUNT(DISTINCT r.ride_id) AS total_rides,
  COUNT(DISTINCT CASE WHEN r.dropoff_ts IS NOT NULL THEN r.ride_id END) AS completed_rides,
  ROUND(100.0 * COUNT(DISTINCT r.user_id) / COUNT(DISTINCT s.user_id), 2) AS activation_rate,
  ROUND(COUNT(DISTINCT r.ride_id)::numeric / NULLIF(COUNT(DISTINCT r.user_id), 0), 2) AS avg_rides_per_active_user,
  ROUND(COUNT(DISTINCT CASE WHEN r.dropoff_ts IS NOT NULL THEN r.ride_id END)::numeric / NULLIF(COUNT(DISTINCT r.user_id), 0), 2) AS avg_completed_rides_per_user,
  COUNT(DISTINCT rev.review_id) AS total_reviews,
  ROUND(AVG(rev.rating), 2) AS avg_rating,
  ROUND(SUM(t.purchase_amount_usd)::numeric, 0) AS total_revenue,
  ROUND(SUM(t.purchase_amount_usd)::numeric / 
        NULLIF(COUNT(DISTINCT s.user_id), 0), 2) AS revenue_per_user,
  a.platform
FROM signups s
FULL JOIN ride_requests r ON s.user_id = r.user_id
JOIN reviews rev ON rev.ride_id = r.ride_id
JOIN transactions t ON t.ride_id = r.ride_id
JOIN app_downloads a ON a.app_download_key = s.session_id
GROUP BY s.age_range, a.platform
ORDER BY total_users DESC;