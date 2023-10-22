-- this works
alter table `imply-news-clicks-sr` add (kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL)

-- work in progress, this one fails with "Temporal table join currently only supports 'FOR SYSTEM_TIME AS OF' left table's time attribute field"
select
  `imply-news-clicks-sr`.`contentId`,
  `imply-news-users-sr`.`version`
from
  `imply-news-clicks-sr` 
left join `imply-news-users-sr` for system_time as of `imply-news-clicks-sr`.`kafka_timestamp`
on `imply-news-clicks-sr`.uid = `imply-news-users-sr`.uid
