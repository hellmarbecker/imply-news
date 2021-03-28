#!/bin/bash

DATASOURCE="video_publication_history_raw"
ELB_URL="https://imply-b3e-elbexter-1ge3j2tooq0e9-1216598690.us-east-1.elb.amazonaws.com:9088"

LINES=$(cat ./datelist-video_publication_history)

for DATE in $LINES; do

#  echo "############ Processing ingestion for $DATE ############"
  ./gen-spec.pl --json-template ingest-video_publication_history.json \
    --access-key-id $AWS_ACCESS_KEY_ID \
    --secret-access-key $AWS_SECRET_ACCESS_KEY \
    --date $DATE \
    --data-source $DATASOURCE >/tmp/ingest-$DATE.json
  TASK_ID=$(curl -s -u admin:UGaEl6qo7hx0zh5l526nPw== --cacert cert.pem \
    -X 'POST' -H "Content-Type: application/json" \
    --data @/tmp/ingest-$DATE.json \
    $ELB_URL/druid/indexer/v1/task | jq .task | tr -d \" )
  echo $TASK_ID
  while
    STATUS=$(curl -s -u admin:UGaEl6qo7hx0zh5l526nPw== --cacert cert.pem \
      -X 'GET' -H "Content-Type: application/json" \
      --data @/tmp/ingest-$DATE.json \
      $ELB_URL/druid/indexer/v1/task/$TASK_ID/status)
    echo $(date) $STATUS
    [[ $STATUS == *"HTTP"* || $STATUS == *"RUNNING"* ]]
  do
    sleep 5
  done 

done

