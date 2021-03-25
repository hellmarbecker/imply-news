#!/bin/bash

LINES=$(cat ./datelist)

for DATE in $LINES; do

  echo "Processing ingestion for $DATE"
  ./gen-spec.pl --json-template ingest-user_view_video.json \
    --access-key-id $AWS_ACCESS_KEY_ID \
    --secret-access-key $AWS_SECRET_ACCESS_KEY \
    --date $DATE \
    --data-source user_view_video_1 >ingest-date.json
  exit 

done

