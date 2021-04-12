SELECT 
       platform_level1,
       cast(__time as date) AS __timestamp,
       APPROX_COUNT_DISTINCT_DS_HLL(hll_session_id) AS videos_viewed
FROM "user_view_video_rollup_reindex"
WHERE __time >= timestamp'2020-04-15'
  AND __time <= timestamp'2020-07-15'
GROUP BY platform_level1, cast(__time as date)
ORDER BY videos_viewed DESC
LIMIT 50000
