TERMINATE ALL; -- kill all persistent queries
DROP TABLE IF EXISTS `imply-news-sessions-changes` DELETE TOPIC;
DROP TABLE IF EXISTS `imply-news-sessions` DELETE TOPIC;
DROP STREAM IF EXISTS `imply-news-avro` DELETE TOPIC;
DROP STREAM IF EXISTS `imply-news-de` DELETE TOPIC;
DROP STREAM IF EXISTS `imply-news-cooked`; -- this shares a topic with imply-news-clicks which is dropped in the next step
DROP STREAM IF EXISTS `imply-news-clicks` DELETE TOPIC;
DROP STREAM IF EXISTS `imply-news-raw`; -- do not drop the input topic!!
