#!/usr/bin/perl

use strict;
use warnings;

use Fcntl qw(:seek);
use Data::Dumper;
use Date::Manip qw(ParseDate UnixDate);
use JSON;

open INFILE, "<$ARGV[0]" or die "Could not open input file $ARGV[0]";

my $header = <INFILE>;
chomp $header;
my @fields = split(/,/, $header);
for (@fields) { s/\"//g; }
my %fieldPositions = map { $fields[$_] => $_ } (0 .. $#fields);

print Dumper(\@fields);
# print Dumper(\%fieldPositions);

my $runDay = 0; # replay loop index

do {
    print STDERR "loop index: $runDay\n";

    while (<INFILE>) {
        chomp;
        my @data = split /,/;
        my %line;
        @line{@fields} = @data;

        my $ts = join(' ', @line{('DCTE', 'HRTE')});
        my $date = ParseDate($ts);

        my $json = encode_json \%line;
        print "$date\n";
        # print "$json\n";
        #print Dumper(\%line);
    }
    $runDay++;
    seek(INFILE, 0, SEEK_SET) or die "error: could not reset file handle\n";
    $header = <INFILE>;

} while (1);

