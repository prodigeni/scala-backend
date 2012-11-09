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
use Digest::MD5 qw(md5 md5_hex md5_base64);

use Wikia::EventWeeklyStats;
use Wikia::Utils;

$|++;
my ($help, $start, $end, $log, $dbname, $lang, $category, $days, $today, $all, $emails) = ();
GetOptions( 
	'help'			=> \$help,
	'log=s'			=> \$log,
	'start=s'		=> \$start,
	'end=s' 		=> \$end,
	'dbname=s'	=> \$dbname,
	'lang=s'		=> \$lang,
	'category=s'=> \$category,
	'days=s'		=> \$days,
	'today=s'		=> \$today,
	'all'				=> \$all,
	'emails=s'	=> \$emails
);

=local methods 
=cut

sub do_help {
    my $name = __FILE__; 
    print <<EOF
$name [--help] [--listen_mq] [--daemon]

	help\t\t-\tprint this text
	days\t\t-\tas many days to generate data
	start\t\t-\tstart count stats (YYYYMMDD)
	end\t\t-\tend count stats (YYYYMMDD)
	dbname\t\t-\tuse database to run
	lang\t\t-\tlang ID 
	category\t\t-\t category ID
	emails\t\t-\tcoma separated list of emails to send report
	all\t\t-\tuse this option to generate summary stats for all Wikis
EOF
;
}

=main ============================================================================================
=cut
if ( (!$start) && (!$end) && (!$days) && (!$help) && (!($dbname || $lang || $category || $all)) ) {
	print STDERR "Use option --help to know how to use script \n";
	exit;
} elsif ($help) {
	&do_help();
	exit;
} else {
	if ( $start && $end && $days ) {
		print "Process (with --start=$start --end=$end --days=$days option) started ...\n";
		
		my $start_sec = time();
		# all dates between $start and $end		
		my $dates = Wikia::Utils->days_between_dates($start, $end, $days);
		
		if ( scalar ( @$dates ) == 0 ) {
			 print "Cannot find any date \n";
			 exit;
		}

		print Dumper(@$dates);
		print "Today is " . $today . "\n";

		$today = DateTime->now()->strftime("%Y-%m-%d") unless ( $today );
		my $exist = undef;
		for ( my $loop = 0; $loop < scalar @$dates; $loop++ ) {
			if ( $dates->[$loop] eq $today ) {
				 $exist = $loop;			
			}
		}

		if ( !defined $exist ) {
			 print "Today script should not be run \n";
			 exit;
		}
		
		if ( !defined $dates->[$exist-1] ) {
			 print "Period exceeded \n";
		}

		my $params = {
			'start_date'	=> sprintf( "%s 00:00:00", $dates->[$exist-1] ),
			'end_date'		=> sprintf( "%s 23:59:59", $dates->[$exist] )
		};

		my $filename = sprintf( "/tmp/%s.csv", md5_base64(DateTime->now()->strftime("%Y%m%d%H%M%s")) );
		my $oConf = new Wikia::Config( {csvfile => $filename} );
		
		my $title = sprintf("Weekly stats for period: %s - %s", $dates->[$exist-1], $dates->[$exist]);
		$oConf->output_csv($title);
				
		my $oEventWeeklyStats;
		# calculate per Wikia stats
		if ( $dbname ) {
			$params->{'type'} = Wikia::EventWeeklyStats::WIKIA_TYPE;
			$oEventWeeklyStats = new Wikia::EventWeeklyStats($params);
			my $data = $oEventWeeklyStats->aggregate_stats($dbname);
			#print Dumper($data);
			$oEventWeeklyStats->prepare_email($oConf, $data);
			undef($oEventWeeklyStats);
		}

		if ( $lang ) {
			# calculate per language stats
			$params->{'type'} = Wikia::EventWeeklyStats::LANG_TYPE;
			$oEventWeeklyStats = new Wikia::EventWeeklyStats($params);
			my $data = $oEventWeeklyStats->aggregate_stats($lang);
			$oEventWeeklyStats->prepare_email($oConf, $data);
			undef($oEventWeeklyStats);
		}

		if ( $category ) {		
			# calculate per category stats
			$params->{'type'} = Wikia::EventWeeklyStats::CAT_TYPE;
			$oEventWeeklyStats = new Wikia::EventWeeklyStats($params);
			my $data = $oEventWeeklyStats->aggregate_stats($category);
			$oEventWeeklyStats->prepare_email($oConf, $data);
			undef($oEventWeeklyStats);
		}

		# calculate per summary stats
		if ( $all ) {
			$params->{'type'} = Wikia::EventWeeklyStats::SUMMARY_TYPE;
			$oEventWeeklyStats = new Wikia::EventWeeklyStats($params);
			my $data = $oEventWeeklyStats->aggregate_stats(undef);
			$oEventWeeklyStats->prepare_email($oConf, $data);
			undef($oEventWeeklyStats);
		}
		
		print "Send $filename to " . $emails . "\n";
		#Wikia::Utils->send_file($title, $emails, $filename);	
		$oConf->send_file('moli@wikia.com', $title, $emails);	
		#unlink($filename);
		
		my $end_sec = time();
		my @ts = gmtime($end_sec - $start_sec);
		print "Script finished after: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);		
	} 
	
}
=tables
=cut
