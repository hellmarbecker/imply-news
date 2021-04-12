select
tag_title,
sum(sum_seconds_viewed)
from  "user_view_video_rollup_reindex" vv
left join  "video_video" on vv.video_id=video_video.video_id
left join "video_tag" on video_tag.program_id=video_video.program_id
where
vv.__time >= cast('2021-01-01' as date)
and
vv.__time < cast('2021-01-31' as date)
group by tag_title
