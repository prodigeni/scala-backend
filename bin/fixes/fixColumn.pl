#!/usr/bin/perl
use strict; 
no strict 'refs';

use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";

use DBI;
use DateTime;
use Data::Dumper;
use Getopt::Long;
use Time::HiRes qw(gettimeofday tv_interval);

use Wikia::EventFixStats;
use Wikia::Utils;

$|++;
my ($help, $current, $log, $dbname, $lang, $category, $month) = ();
GetOptions( 
	'help'			=> \$help,
	'log=s'			=> \$log,
	'current'		=> \$current,
	'dbname=s'		=> \$dbname,
	'lang=s'		=> \$lang,
	'category=s'	=> \$category,
	'month=s'		=> \$month
);

sub do_help {
    my $name = __FILE__; 
    print <<EOF
$name [--help] [--listen_mq] [--daemon]

	help\t\t-\tprint this text
	month\t\t-\tformat YYYYMM
	current\t\t-\tcount live stats
	dbname\t\t-\tuse database to run
EOF
;
}

=main ============================================================================================
=cut
if ( (!$month) && (!$current) && (!$help) ) {
	print STDERR "Use option --help to know how to use script \n";
	exit;
} elsif ($help) {
	&do_help();
	exit;
} else {
	if ( $current || $month ) {
		print "Process (with " . ( ($current) ? '--current' : '--month=' . $month ) . " option) started ...\n";
		
		my $start_sec = time();		
		my $start = DateTime->now()->strftime("%F %T");

		my $params = {
			'start_date'	=> Wikia::Utils->first_datetime($month),
			'end_date'		=> Wikia::Utils->last_datetime($month)
		};

		# calculate per Wikia stats
		$params->{'type'} = Wikia::EventFixStats::WIKIA_TYPE;
		my $oEventStats = new Wikia::EventFixStats($params);
		my $monthlyStats = $oEventStats->aggregate_stats($dbname);
		undef($oEventStats);

		# calculate per language stats
		$params->{'type'} = Wikia::EventFixStats::LANG_TYPE;
		$oEventStats = new Wikia::EventFixStats($params);
		my $lCount = $oEventStats->aggregate_stats($lang, '', $monthlyStats);
		undef($oEventStats);
		
		# calculate per category stats
		$params->{'type'} = Wikia::EventFixStats::CAT_TYPE;
		$oEventStats = new Wikia::EventFixStats($params);
		my $cCount = $oEventStats->aggregate_stats($category, '', $monthlyStats);
		undef($oEventStats);

		# calculate per category && language stats
		$params->{'type'} = Wikia::EventFixStats::CAT_LANG_TYPE;
		$oEventStats = new Wikia::EventFixStats($params);
		my $clCount = $oEventStats->aggregate_stats($category, $lang, $monthlyStats);
		undef($oEventStats);
		
		# calculate per summary stats
		$params->{'type'} = Wikia::EventFixStats::SUMMARY_TYPE;
		$oEventStats = new Wikia::EventFixStats($params);
		my $sCount = $oEventStats->aggregate_stats(undef, '', $monthlyStats);
		undef($oEventStats);
		
		my $end_sec = time();
		my @ts = gmtime($end_sec - $start_sec);
		print "Script finished after: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);		
	} 
}
=tables
=cut
