#!/usr/bin/perl -w
#
# purge-kicker
#
# Monitor the purge log and if it gets stuck, restart the process
#

use strict;

use Time::Local;
use Getopt::Long;

my ($thresh, $debug) = (60*5, 0);
GetOptions('threshhold|thresh|t=s' => \$thresh,
		   'debug|d'               => \$debug,
);

my $logfile = '/etc/sv/scribe-purger/log/main/current';
my $init_cmd = '/etc/init.d/scribe-purger restart';

my $last = `tail -1 $logfile`;

# Beginning of log line looks like "2011-04-18_22:49:28.05598 ..."
my ($Y, $M, $D, $h, $m, $s) = $last =~ /^(\d{4})-(\d{2})-(\d{2})_(\d{2}):(\d{2}):(\d{2})/; 
my $t0 = timelocal($s, $m, $h, $D, $M-1, $Y-1900);

my $delta = time - $t0;

print "Last log line is ${delta}s old\n" if $debug;

if ($delta > $thresh) {
	# Restart the purgers
	system($init_cmd);

	print STDERR "Kicked purgers after ${delta}s of inactivity\n";
}
