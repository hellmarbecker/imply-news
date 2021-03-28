SELECT
  u.__time,
  u.video_title,
  v.folder_name
FROM user_view_video_9 u
INNER JOIN video_folder v 
ON u.video_id = v.video_id
LIMIT 100
