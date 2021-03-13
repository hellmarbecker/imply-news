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
use Net::Kafka::Producer;
use AnyEvent;

# Set up logging

Log::Log4perl->easy_init(Log::Log4perl::Level::to_priority('INFO'));
my $logger = Log::Log4perl->get_logger();

#-------------------------------------------------------------------------------
sub incDate($$) { # increment by n days
    my $origDate = ParseDate($_[0]);
    $_[0] = UnixDate(DateCalc($origDate, "+ $_[1] days"), "%Y-%m-%d");
} # incDate
#-------------------------------------------------------------------------------
sub incTime($$) { # increment by random interval up to 1 hour
    my $origDate = ParseDate(join(" ", @_));
    my $minutes = rand(60);
    my $newDate = UnixDate(DateCalc($origDate, "+ $minutes minutes"), "%Y-%m-%d %H:%M:%S");
    ($_[0], $_[1]) = split(/ /, $newDate);
} # incTime
#-------------------------------------------------------------------------------
sub produceMessage($$$) {
    my($producer, $topic, $message) = @_;
#    my $condvar = AnyEvent->condvar;
    $producer->produce(
        payload => $message,
        topic   => $topic,
    )->then(sub {
        my $delivery_report = shift;
#        $condvar->send;
        $logger->debug("Message successfully delivered with offset " . $delivery_report->{offset});
    }, sub {
        my $error = shift;
#        $condvar->send;
        $logger->error("Unable to produce a message: " . $error->{error} . ", code: " . $error->{code});
    });
#    $condvar->recv;
} # produceMessage
#-------------------------------------------------------------------------------
sub readConfig($) {
    my $cfg = LoadFile($_[0]);
    # get optional includes
    for my $incf (@{$cfg->{IncludeOptional}}) {
        if ( -e $incf && -f $incf ) {
            $logger->debug("Merging file $incf");
            my $inc = LoadFile($incf);
            # included settings supersede those in the main config
            @{$cfg}{keys %$inc} = values %$inc; 
        } else {
            $logger->debug("File $incf not found, skipping");
        }
    }
} # readConfig
#-------------------------------------------------------------------------------

# Kafka topic and producer properties
# General->topic
# Kafka->properties in the usual format

my $cfgfile;
my $dryRun = 0;
my $runDay = 0; # replay loop index / offset

GetOptions(
    "config|f=s" => \$cfgfile,
    "dry-run|n"  => \$dryRun,
    "offset|o=i" => \$runDay,
);
my $config = readConfig($cfgfile);

$logger->info(Dumper($config->{Kafka}));

my $producer = Net::Kafka::Producer->new(%{$config->{Kafka}}) unless $dryRun;
my $newTopic = $config->{General}->{newTopic};
my $cancellationTopic = $config->{General}->{cancellationTopic};
my $cancelProbability = $config->{General}->{cancelProbability};
# Input file

open INFILE, "<$ARGV[0]" or die "Could not open input file $ARGV[0]";

# Read the header, only the first time

my $header = <INFILE>;
chomp $header;
my @fields = split(/,/, $header);
for (@fields) { s/\"//g; }
my %fieldPositions = map { $fields[$_] => $_ } (0 .. $#fields);

my $lineIndex;

# Replay ad infinitum

do {
    $lineIndex = 0;
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
        if ($dryRun) {
            print "$csv\n";
        } else {
            produceMessage($producer, $newTopic, $csv);
        }
        if (rand() < $cancelProbability) { # create cancellation
            my %cancelLine = %line;
            $cancelLine{CTTE} = 2;
            # incTime($cancelLine{DCTE}, $cancelLine{HRTE});
            my $ccsv = join(",", @cancelLine{@fields});
            if ($dryRun) {
                print "$ccsv\n";
            } else {
                produceMessage($producer, $cancellationTopic, $ccsv);
            }
        }
        $lineIndex++;
        $logger->info("runDay: $runDay line: $lineIndex") if ($lineIndex % 1000 == 0);
    }
    $runDay++;
    seek(INFILE, 0, SEEK_SET) or die "error: could not reset file handle\n";
    $header = <INFILE>;

} while (1);

