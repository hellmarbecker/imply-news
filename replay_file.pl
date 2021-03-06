#!/usr/bin/perl

use strict;
use warnings;

use Data::Dumper;
use Date::Manip qw(ParseDate UnixDate);

my $header = <>;
chomp $header;
my @fields = split(/,/, $header);
for (@fields) { s/\"//g; }
my %fieldPositions = map { $fields[$_] => $_ } (0 .. $#fields);

print Dumper(\@fields);
# print Dumper(\%fieldPositions);

my %line;

while (<>) {
   chomp;
   my @data = split /,/;
   @line{@fields} = @data;

   my $ts = join(' ', @line{('DCTE', 'HRTE')});
   my $date = ParseDate($ts);
   print "$date\n";
   #print Dumper(\%line);
}
