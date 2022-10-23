-- Create a raw stream from input data

CREATE OR REPLACE STREAM `imply-news-raw` (
  `sid_key` STRING KEY, 
  `payload` STRING 
) 
WITH ( KAFKA_TOPIC='imply-news', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA' );

-- Implement topic splitter: only retain records with type 'click'
-- This is one way to splice up a topic that has different types of records
-- Data is still regarded as a blob and the splicing criteria is extracted with an explicit JSON function

CREATE OR REPLACE STREAM `imply-news-clicks` WITH (
  KAFKA_TOPIC='imply-news-clicks',
  PARTITIONS=6,
  KEY_FORMAT='KAFKA',
  VALUE_FORMAT='KAFKA' ) AS
SELECT
  `sid_key`,
  `payload` 
FROM `imply-news-raw` 
WHERE EXTRACTJSONFIELD(`payload`, '$.recordType') = 'click';

-- Reinterpret the stream:
-- This is possible because now we have homogenous data.
-- Still, this is plain schemaless JSON, so up to here there is no enforcement of the governance contract.

CREATE OR REPLACE STREAM `imply-news-cooked` (
  `sid_key` STRING KEY,
  `sid` STRING,
  `timestamp` BIGINT,
  `recordType` STRING,
  `url` STRING,
  `useragent` STRING,
  `statuscode` STRING,
  `state` STRING,
  `uid` STRING,
  `isSubscriber` INT,
  `campaign` STRING,
  `channel` STRING,
  `contentId` STRING,
  `subContentId` STRING,
  `gender` STRING,
  `age` STRING,
  `latitude` DOUBLE,
  `longitude` DOUBLE,
  `place_name` STRING,
  `country_code` STRING,
  `timezone` STRING
)
WITH ( KAFKA_TOPIC='imply-news-clicks', KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON' );

-- Filter by a JSON column

CREATE OR REPLACE STREAM `imply-news-de` WITH (
  KAFKA_TOPIC='imply-news-de',
  KEY_FORMAT='KAFKA',
  VALUE_FORMAT='JSON' ) AS
SELECT *
FROM `imply-news-cooked`
WHERE `country_code` = 'DE';

