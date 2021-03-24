#!/usr/bin/perl
  
use strict;
use warnings;

my @urls = qw(
    s3://6cloud-dev-data-m6web-core-data/m6web_core_dim_catalog_v1/video_folder
    s3://6cloud-dev-data-m6web-core-data/m6web_core_dim_catalog_v1/video_program
    s3://6cloud-dev-data-m6web-core-data/m6web_core_dim_catalog_v1/video_publication_history
    s3://6cloud-dev-data-m6web-core-data/m6web_core_dim_catalog_v1/video_tag
    s3://6cloud-dev-data-m6web-core-data/m6web_core_dim_catalog_v1/video_video
    s3://6cloud-dev-data-m6web-core-data/m6web_core_fact_beneficiaries_v1/user_view_video
    s3://6cloud-dev-data-m6web-core-data/m6web_core_dim_users_v1/user_account
);
my $basepath = "/data/bedrock-data";

print "#!/bin/bash\n";

for (@urls) {
    m#s3://[^/]*/(.*)$#;
    my $path = "$basepath/$1";
    print "mkdir -p $path\n";
    print "aws s3 cp --recursive $_ $path\n";
}
