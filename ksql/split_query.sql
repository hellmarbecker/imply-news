-- pick out specific record type by parsing JSON dynamically

SELECT
  sid,
  payload,
  EXTRACTJSONFIELD(payload, '$.recordType') AS recordtype 
FROM "imply-news-raw" 
WHERE EXTRACTJSONFIELD(payload, '$.recordType') = 'session' 
EMIT CHANGES;

