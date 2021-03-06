#!/usr/bin/perl

use strict;
use warnings;

use Data::Dumper;

my $header = <>;
chomp $header;
my @fields = split(/,/, $header);
my %fieldPositions = map { $fields[$_] => $_ } (0 .. $#fields);

print Dumper(\%fieldPositions);
exit;

my %line;

while (<>) {
   chomp;
   my @data = split /,/;
   @line{@fields} = @data;

   print Dumper(\%line);
}
