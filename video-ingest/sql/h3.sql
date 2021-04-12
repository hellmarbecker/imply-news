SELECT  platform_level1,
       cast(__time as date) AS __timestamp,
       
       APPROX_COUNT_DISTINCT_DS_HLL(hll_account_device_id) AS count_uid
FROM  "user_view_video_rollup_reindex"
WHERE __time >= cast('2020-04-16' as date)
  AND __time <= cast('2020-07-16' as date)
GROUP BY platform_level1, cast(__time as date)
ORDER BY count_uid DESC
LIMIT 50000
