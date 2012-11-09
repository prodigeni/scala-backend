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

use Wikia::EventStats;
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

=pod stats log table 
CREATE TABLE `events_log` (
  `el_type` char(32) NOT NULL,
  `el_wiki` int(5) default 0,
  `el_language` int(5) default 0,
  `el_category` int(5) default 0,
  `el_summary` int(5) default 0,
  `el_start` timestamp default '0000-00-00 00:00:00',
  `el_end` timestamp default '0000-00-00 00:00:00',
  PRIMARY KEY  (`el_type`),
  KEY `el_start_end` (`el_type`, `el_start`, `el_end`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
=cut

=pod local methods 
=cut

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

=pod main ============================================================================================
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
			'start_date'	=> ( !$month ) ? Wikia::EventStats->get_events_log('stats') : Wikia::Utils->first_datetime($month),
			'end_date'		=> ( !$month ) ? DateTime->now()->strftime("%F %T") : Wikia::Utils->last_datetime($month)
		};

		# calculate per Wikia stats
		$params->{'type'} = Wikia::EventStats::WIKIA_TYPE;
		my $oEventStats = new Wikia::EventStats($params);
		my $monthlyStats = $oEventStats->aggregate_stats($dbname);
		undef($oEventStats);

		# calculate per language stats
		$params->{'type'} = Wikia::EventStats::LANG_TYPE;
		$oEventStats = new Wikia::EventStats($params);
		my $lCount = $oEventStats->aggregate_stats($lang, '', $monthlyStats);
		undef($oEventStats);
		
		# calculate per category stats
		$params->{'type'} = Wikia::EventStats::CAT_TYPE;
		$oEventStats = new Wikia::EventStats($params);
		my $cCount = $oEventStats->aggregate_stats($category, '', $monthlyStats);
		undef($oEventStats);

		# calculate per category && language stats
		$params->{'type'} = Wikia::EventStats::CAT_LANG_TYPE;
		$oEventStats = new Wikia::EventStats($params);
		my $clCount = $oEventStats->aggregate_stats($category, $lang, $monthlyStats);
		undef($oEventStats);
		
		# calculate per summary stats
		$params->{'type'} = Wikia::EventStats::SUMMARY_TYPE;
		$oEventStats = new Wikia::EventStats($params);
		my $sCount = $oEventStats->aggregate_stats(undef, '', $monthlyStats);
		undef($oEventStats);
		
		# update events log 
		my $values = {
			"start"		=> $start,
			"wiki"		=> 0,
			"language"	=> 0,
			"category"	=> 0,
			"summary"	=> 0,
			"cat_lang"  => 0
		};
		# wikis
		foreach my $m (keys %{$monthlyStats}) {
			$values->{wiki} += Wikia::Utils->intval($monthlyStats->{$m}->{records});
		}

		# languages
		foreach my $m ( keys %{$lCount} ) {
			$values->{language} += Wikia::Utils->intval($lCount->{$m}->{records});
		}
		
		# categories
		foreach my $m ( keys %{$cCount} ) {
			$values->{category} += Wikia::Utils->intval($cCount->{$m}->{records});
		}		
		
		# cat lang
		foreach my $m ( keys %{$clCount} ) {
			$values->{cat_lang} += Wikia::Utils->intval($clCount->{$m}->{records});
		}
		
		#summary
		foreach my $m ( keys %{$sCount} ) {
			$values->{summary} += Wikia::Utils->intval($sCount->{$m}->{records});
		}
		
		Wikia::EventStats->update_events_log('stats', $values);
		
		my $end_sec = time();
		my @ts = gmtime($end_sec - $start_sec);
		print "Script finished after: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);		
	} 
}
=pod tables
=cut
