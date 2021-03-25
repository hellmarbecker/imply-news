#!/bin/bash

LINES=$(cat ./datelist)

for DATE in $LINES; do

  echo "----Processing ingestion for $DATE"
  ./gen-spec.pl --json-template ingest-user_view_video.json \
    --access-key-id $AWS_ACCESS_KEY_ID \
    --secret-access-key $AWS_SECRET_ACCESS_KEY \
    --date $DATE \
    --data-source user_view_video_3 >/tmp/ingest-$DATE.json
  curl -u admin:UGaEl6qo7hx0zh5l526nPw== --cacert cert.pem \
    -X 'POST' -H "Content-Type: application/json" \
    --data @/tmp/ingest-$DATE.json \
    https://imply-b3e-elbexter-n0nnt27t5gdw-723214491.us-east-1.elb.amazonaws.com:9088/druid/indexer/v1/task

done

