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

-- create table for users
CREATE TABLE `default`.`cluster_0`.`imply-news-users-sr` (
  `key` VARBINARY(2147483647),
  `timestamp` BIGINT NOT NULL,
  `version` BIGINT NOT NULL,
  `recordType` VARCHAR(2147483647) NOT NULL,
  `uid` VARCHAR(2147483647) NOT NULL,
  `isSubscriber` INT NOT NULL,
  `gender` VARCHAR(2147483647) NOT NULL,
  `age` VARCHAR(2147483647) NOT NULL,
  `latitude` DOUBLE NOT NULL,
  `longitude` DOUBLE NOT NULL,
  `place_name` VARCHAR(2147483647) NOT NULL,
  `country_code` VARCHAR(2147483647) NOT NULL,
  `timezone` VARCHAR(2147483647) NOT NULL,
  -- `kafka_timestamp` TIMESTAMP(3) WITH LOCAL TIME ZONE METADATA FROM 'timestamp' VIRTUAL,
  primary key (`key`) not enforced
)
PARTITIONED BY (`key`)
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'kafka.max-message-size' = '2097164 bytes',
  'kafka.partitions' = '6',
  'kafka.retention.size' = '0 bytes',
  'kafka.retention.time' = '604800000 ms',
  'key.format' = 'raw',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'avro-registry'
)

