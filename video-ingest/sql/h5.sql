SELECT cast(__time as date) AS __timestamp,
       APPROX_COUNT_DISTINCT_DS_HLL(hll_session_id) AS videos_viewed,
       SUM(sum_seconds_viewed)/3600 AS viewing_time,
       APPROX_COUNT_DISTINCT_DS_HLL(hll_account_device_id) AS count_uid
FROM  "user_view_video_rollup_reindex" 
WHERE __time >= cast('2020-07-08' as date)
  AND __time <= cast('2020-07-15' as date)
GROUP BY cast(__time as date)
ORDER BY videos_viewed DESC
LIMIT 50000
