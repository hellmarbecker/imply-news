#!/bin/bash

DATASOURCE=user_view_video_raw

LINES=$(cat ./datelist-user_view_video)
ELB_URL="https://imply-b18-elbexter-63dmtx8lxpcb-1363504918.us-east-1.elb.amazonaws.com:9088"

for DATE in $LINES; do

#  echo "\n----Processing ingestion for $DATE"
  ./gen-spec.pl --json-template ingest-user_view_video-d.json \
    --access-key-id $AWS_ACCESS_KEY_ID \
    --secret-access-key $AWS_SECRET_ACCESS_KEY \
    --date $DATE \
    --data-source $DATASOURCE >/tmp/ingest-$DATE.json
  TASK_ID=$(curl -s -u admin:XOJ68cdvVAqWWXVvLJKhUg== --cacert cert.pem \
    -X 'POST' -H "Content-Type: application/json" \
    --data @/tmp/ingest-$DATE.json \
    $ELB_URL/druid/indexer/v1/task | jq .task | tr -d \" )
  echo $DATE - $TASK_ID

  sleep 10

#  exit
#  while
#    STATUS=$(curl -s -u admin:XOJ68cdvVAqWWXVvLJKhUg== --cacert cert.pem \
#      -X 'GET' -H "Content-Type: application/json" \
#      --data @/tmp/ingest-$DATE.json \
#      $ELB_URL/druid/indexer/v1/task/$TASK_ID/status)
#    echo $(date) $STATUS
#    [[ $STATUS == *"HTTP"* || $STATUS == *"RUNNING"* ]]
#  do
#    sleep 5
#  done 

done

