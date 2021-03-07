#!/usr/bin/perl

use strict;
use warnings;

use Fcntl qw(:seek);
use Time::HiRes qw(usleep);
use Log::Log4perl ();
use Log::Log4perl::Level ();
use Getopt::Long;
use YAML::XS qw(LoadFile);
use Data::Dumper;
use Date::Manip qw(ParseDate UnixDate DateCalc);
use Net::Kafka;

#-------------------------------------------------------------------------------
sub incDate($$) {
    my $origDate = ParseDate($_[0]);
    $_[0] = UnixDate(DateCalc($origDate, "+ $_[1] days"), "%Y-%m-%d");
} # incDate
#-------------------------------------------------------------------------------

# Set up logging

Log::Log4perl->easy_init(Log::Log4perl::Level::to_priority('INFO'));
my $logger = Log::Log4perl->get_logger();

# Kafka topic and producer properties
# General->topic
# Kafka->prpoperties in the usual format

my $cfgfile;
GetOptions(
    "config|f=s" => \$cfgfile,
);
my $config = LoadFile($cfgfile);

# Input file

open INFILE, "<$ARGV[0]" or die "Could not open input file $ARGV[0]";

# Read the header, only the first time

my $header = <INFILE>;
chomp $header;
my @fields = split(/,/, $header);
for (@fields) { s/\"//g; }
my %fieldPositions = map { $fields[$_] => $_ } (0 .. $#fields);

my $runDay = 0; # replay loop index

# Replay ad infinitum

do {
    $logger->info("loop index: $runDay");

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

