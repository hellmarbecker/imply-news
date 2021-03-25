#!/usr/bin/perl
use strict;
use warnings;
#-------------------------------------------------------------------------------
# Substitute variables in the ingestion spec
#-------------------------------------------------------------------------------

use Getopt::Long;

my $usage = <<"EOF";
    usage: $0 --json-template <json-template> --access-key-id <access key id> --secret-access-key <secret access key> --date <date> --data-source <data source>
EOF

my $json_template;
my $access_key_id;
my $secret_access_key;
my $d_date;
my $data_source;

GetOptions(
    "json-template=s"     => \$json_template,
    "access-key-id=s"     => \$access_key_id,
    "secret-access-key=s" => \$secret_access_key,
    "date=s"              => \$d_date,
    "data-source=s"       => \$data_source)
or die $usage;

open INFILE, "<$json_template" or die "could not open file $json_template";
while (<INFILE>) {
    s/\$access_key_id/$access_key_id/g;
    s/\$secret_access_key/$secret_access_key/g;
    s/\$d_date/$d_date/g;
    s/\$data_source/$data_source/g;
    print;
}
close INFILE;


