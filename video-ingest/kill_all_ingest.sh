#!/bin/sh

DATASOURCE=user_view_video_3

#for id in $(
  #curl -s -u 'admin:UGaEl6qo7hx0zh5l526nPw==' --cacert cert.pem \
    #-XGET -H "Content-Type: application/json" \
    #https://imply-b3e-elbexter-n0nnt27t5gdw-723214491.us-east-1.elb.amazonaws.com:9088/druid/indexer/v1/tasks \
    #| jq .[]."id" \
    #| grep $DATASOURCE )
#do
#done
curl -s -u 'admin:UGaEl6qo7hx0zh5l526nPw==' --cacert cert.pem \
  -XPOST -H "Content-Type: application/json" \
  https://imply-b3e-elbexter-n0nnt27t5gdw-723214491.us-east-1.elb.amazonaws.com:9088/druid/indexer/v1/datasources/$DATASOURCE/shutdownAllTasks



