#!/bin/bash

DATASOURCE=user_view_video_9

LINES=$(cat ./datelist)

for DATE in $LINES; do

#  echo "\n----Processing ingestion for $DATE"
  ./gen-spec.pl --json-template ingest-user_view_video.json \
    --access-key-id $AWS_ACCESS_KEY_ID \
    --secret-access-key $AWS_SECRET_ACCESS_KEY \
    --date $DATE \
    --data-source $DATASOURCE >/tmp/ingest-$DATE.json
  TASK_ID=$(curl -s -u admin:UGaEl6qo7hx0zh5l526nPw== --cacert cert.pem \
    -X 'POST' -H "Content-Type: application/json" \
    --data @/tmp/ingest-$DATE.json \
    https://imply-b3e-elbexter-n0nnt27t5gdw-723214491.us-east-1.elb.amazonaws.com:9088/druid/indexer/v1/task | jq .task | tr -d \" )
  echo $TASK_ID
  while
    STATUS=$(curl -s -u admin:UGaEl6qo7hx0zh5l526nPw== --cacert cert.pem \
      -X 'GET' -H "Content-Type: application/json" \
      --data @/tmp/ingest-$DATE.json \
      https://imply-b3e-elbexter-n0nnt27t5gdw-723214491.us-east-1.elb.amazonaws.com:9088/druid/indexer/v1/task/$TASK_ID/status)
    echo $(date) $STATUS
    [[ $STATUS == *"HTTP"* || $STATUS == *"RUNNING"* ]]
  do
    sleep 5
  done 

done

