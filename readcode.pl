#!/usr/bin/perl

# Take a file that has keys and values alternating,
# with arbitrary empty lines interspersed, and transform
# it into a snippet that can be pasted into a JSON
# lookup map for Druid

use strict;
use warnings;

my $iskey = 1;

while (<>) {
    next if /^\s*$/;
    chomp;
    print "\"$_\"";
    print $iskey ? ": " : ",\n";
    $iskey ^= 1;
}

