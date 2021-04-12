select
sum(vv.sum_seconds_viewed)/3600 as viewing_time,
ua.age as age,
zip_code  as code_postal
from 
"user_view_video_rollup_with_acct_reindex" vv
left join "user_account_indexed" ua on vv.account_id= ua.account_id
group by ua.age, zip_code 
order by 1 desc
