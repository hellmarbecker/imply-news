-- create a raw stream from input data

CREATE OR REPLACE STREAM "imply-news-raw" (
  "sid" STRING KEY, 
  "payload" STRING 
) 
WITH ( KAFKA_TOPIC='imply-news', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA' );

-- implement topic splitter

-- CREATE OR REPLACE STREAM "imply-news-sessions" AS
-- SELECT sid, payload 
-- FROM "imply-news-raw" 
-- WHERE extractjsonfield(payload, '$.recordType') = 'session';

CREATE OR REPLACE STREAM "imply-news-clicks" AS
SELECT "sid", "payload" 
FROM "imply-news-raw" 
WHERE extractjsonfield("payload", '$.recordType') = 'click';

-- reinterpret the stream

CREATE OR REPLACE STREAM "imply-news-cooked" (
  "sid" STRING KEY,
  "timestamp" BIGINT,
  "recordType" STRING,
  "url" STRING,
  "useragent" STRING,
  "statuscode" STRING,
  "state" STRING,
  "uid" STRING,
  "isSubscriber" INT,
  "campaign" STRING,
  "channel" STRING,
  "contentId" STRING,
  "subContentId" STRING,
  "gender" STRING,
  "age" STRING,
  "latitude" DOUBLE,
  "longitude" DOUBLE,
  "place_name" STRING,
  "country_code" STRING,
  "timezone" STRING
)
WITH ( KAFKA_TOPIC='imply-news-clicks', KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON' );

-- transform to avro

CREATE OR REPLACE STREAM "imply-news-avro" WITH ( KEY_FORMAT='KAFKA', VALUE_FORMAT='AVRO' ) AS
SELECT
  "sid",
  "timestamp",
  "recordType",
  "url",
  "useragent",
  "statuscode",
  "state",
  "uid",
  "isSubscriber",
  "campaign",
  "channel",
  "contentId",
  "subContentId",
  "gender",
  "age",
  "latitude",
  "longitude",
  "place_name",
  "country_code",
  "timezone"
FROM "imply-news-cooked";
