#!/usr/bin/perl

use strict;
use warnings;

open INFILE, "<subst.json" or die "could not open file";
my $content = do { local $/; <INFILE> };
close INFILE;

my $var_1 = "foo";
$content =~ s/(\$\w+)/$1/gee;
print $content;

