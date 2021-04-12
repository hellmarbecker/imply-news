#!/bin/bash

ELB_URL=https://imply-b18-elbexter-63dmtx8lxpcb-1363504918.us-east-1.elb.amazonaws.com:9088
file=$1

cat $file
query=$(sed -e 's/"/\\"/g' $file)

time \
  echo '{ "query" : "'$query'" }' |
    curl -s -u admin:XOJ68cdvVAqWWXVvLJKhUg== --cacert ../cert.pem -XPOST --data-binary @- -H "Content-Type: application/json" $ELB_URL/druid/v2/sql 

