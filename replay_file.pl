#!/usr/bin/perl

use strict;
use warnings;

use Fcntl qw(:seek);
use Time::HiRes qw(usleep);
use Data::Dumper;
use Date::Manip qw(ParseDate UnixDate DateCalc);
# use JSON;


sub incDate($$) {
    my $origDate = ParseDate($_[0]);
    $_[0] = UnixDate(DateCalc($origDate, "+ $_[1] days"), "%Y-%m-%d");
} # incDate


open INFILE, "<$ARGV[0]" or die "Could not open input file $ARGV[0]";

my $header = <INFILE>;
chomp $header;
my @fields = split(/,/, $header);
for (@fields) { s/\"//g; }
my %fieldPositions = map { $fields[$_] => $_ } (0 .. $#fields);

my $runDay = 0; # replay loop index

do {
    print STDERR "loop index: $runDay\n";

    while (<INFILE>) {
        chomp;
        my @data = split /,/;
        my %line;
        @line{@fields} = @data;

        incDate($line{DCTE}, $runDay);
        incDate($line{DCRCU}, $runDay);
        incDate($line{DDGEP}, $runDay);

        my $csv = join(",", @line{@fields});
        print "$csv\n";
        usleep(2000);
    }
    $runDay++;
    seek(INFILE, 0, SEEK_SET) or die "error: could not reset file handle\n";
    $header = <INFILE>;

} while (1);

