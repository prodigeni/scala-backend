#!/usr/bin/perl

use FindBin qw/$Bin/;
use lib "$Bin/../lib";

my $YML;
BEGIN {
	$YML = "$Bin/../../wikia-conf/DB.localhost.yml" if ($Bin =~ /moli/) ;
}

use DBI;

use Wikia::Config;
use Wikia::Utils;
use Wikia::DB;
use Wikia::LB;
use Wikia::ExternalLB;
use Wikia::Revision;

use Getopt::Long;
use Data::Dumper;
use CGI::Carp qw(fatalsToBrowser);
use Time::Local ;
use Storable qw(dclone);
use Encode;
use HTML::Entities;
use Compress::Zlib;
use Switch;

=info
Some globals here!
=cut
my $oConf = new Wikia::Config( {logfile => "/tmp/test_stats.log" } );

my ($help, $month, $count, $column) = ();
GetOptions(
	'help' => \$help,
	'month=s' => \$month,
	'count=s' => \$count,
	'column=s' => \$column
);

if ($help) { 
	&usage(); exit; 
} else {
	if ( (!$month) || (!$count) || (!$column) ) {
		print STDERR "Use option --help to know how to use script \n";
		exit;
	}
}

#read long options
sub usage() {
    my $name = "test_stats.pl";
    print "$name [--help] [--skip=db[,db2[,...]]] [--usedb=db[,db2[,...]]] [--all]\n\n";
    print "\thelp\t\t-\tprint this text\n";
    print "\tmonth\t\t-\tyear with month (YYYYMM)\n";
    print "\tcolumn\t\t-\tcolumn to analyze\n";
    print "\tcount\t\t-\tnumber of Wikis to analyze\n";
}

my $lb = Wikia::LB->instance;
$lb->yml( $YML ) if ($YML);
my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED )} );
my $dbr_stats = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::STATS )} );

my %db_fields = (
'A' => 'editors_month_allns',
'B' => 'editors_month_contentns',
'C' => 'editors_month_5times',
'D' => 'editors_month_100times',
'E' => 'articles',
'F' => 'articles_day',
'G' => 'database_edits'
);

my %RESULTS = ();

$oConf->log("Get results from stats table for column " . $db_fields{$column} . "($column) and month = $month", 1);

my @options = ( 
	" group by 2 ", 
	" order by 1 desc ", 
	" limit $count " 
);
my @where = ( "stats_date = '".$month."'" );
my $sth_w = $dbr_stats->select_many(
	"max(".$db_fields{$column}.") as ".$column.", wikia_id", 
	"stats_summary_part", 
	\@where, 
	\@options
);

if ( $sth_w ) {
	while ( my ($col, $city_id) = $sth_w->fetchrow_array() ) {
		%{$RESULTS{$city_id}} = ('stats' => $col, 'original' => 0 );
	}
	$sth_w->finish();
}

my @db_keys = keys %RESULTS;
my @where_db = ("city_public = 1");
if (scalar @db_keys) {
	push @where_db, "city_id in (".join(",", map { $dbr->quote($_) } @db_keys).")";
}

my $process_start_time = time();

$oConf->log("get list of wikis from city list", 1);
my ($databases) = $dbr->get_wikis(\@where_db);
my $main_loop = 0;
foreach my $num (sort ( map { sprintf("%012u",$_) } (keys %{$databases}) ))
{
	#--- set city;
	my $city_id = int $num;
	#--- set start time
	my $start_sec = time();
	$oConf->log ($databases->{$city_id} . " processed (".$city_id.")", 1);

	my $contentNamespaces = $dbr->__content_namespaces($city_id);

	my $dbl = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', $databases->{$city_id} )} );
	switch ($column) {
		case 'A' {
			@options = ();
			@where = ( 
				" page_id = rev_page ",
				" date_format(rev_timestamp, '%Y%m') = '$month' ",
				" page_is_redirect = 0 "
			);
			my $oRow = $dbl->select(
				" count(distinct(rev_user)) as val ",
				" page, revision ",
				\@where,
				\@options
			);
			$RESULTS{$city_id}{'original'} = $oRow->{val};
		}
		case 'B' { 
			@options = ();
			@where = ( 
				" page_id = rev_page ",
				" date_format(rev_timestamp, '%Y%m') = '$month' ",
				" page_is_redirect = 0 ",
				" page_namespace in (".join(",", map { $dbl->quote($_) } @{$contentNamespaces}).") ",
			);
			my $oRow = $dbl->select(
				" count(distinct(rev_user)) as val ",
				" page, revision ",
				\@where,
				\@options
			);
			$RESULTS{$city_id}{'original'} = $oRow->{val};
		}
		case 'C' { 
			@options = ("group by rev_user");
			@where = ( 
				" page_id = rev_page ",
				" date_format(rev_timestamp, '%Y%m') = '$month' ",
				" page_is_redirect = 0 ",
				" page_namespace in (".join(",", map { $dbl->quote($_) } @$contentNamespaces).") ",
			);
			my $sth = $dbl->select_many(
				" rev_user, count(rev_id) as cnt ",
				" page, revision ",
				\@where,
				\@options
			);
			my $cnt = 0;
			if ( $sth ) {
				while ( my ($rev_user, $cnt) = $sth->fetchrow_array() ) {
					$cnt++ if ( $cnt >= 5 );
				}
				$sth->finish();
			}
			$RESULTS{$city_id}{'original'} = $cnt;
		}
		case 'D' { 
			@options = ("group by rev_user");
			@where = ( 
				" page_id = rev_page ",
				" date_format(rev_timestamp, '%Y%m') = '$month' ",
				" page_is_redirect = 0 ",
				" page_namespace in (".join(",", map { $dbl->quote($_) } @$contentNamespaces).") ",
			);
			my $sth = $dbl->select_many(
				" rev_user, count(rev_id) as cnt ",
				" page, revision ",
				\@where,
				\@options
			);
			my $cnt = 0;
			if ( $sth ) {
				while ( my ($rev_user, $cnt) = $sth->fetchrow_array() ) {
					$cnt++ if ( $cnt >= 100 );
				}
				$sth->finish();
			}
			$RESULTS{$city_id}{'original'} = $cnt;
		}
		case 'E' { 
			@options = ();
			@where = ( 
				" page_id = rev_page ",
				" date_format(rev_timestamp, '%Y%m') <= '$month' ",
				" page_is_redirect = 0 ",
				" page_namespace in (".join(",", map { $dbl->quote($_) } @$contentNamespaces).") ",
				" page_len > 0 "
			);
			my $oRow = $dbl->select(
				" count(distinct(page_id)) as val ",
				" page, revision ",
				\@where,
				\@options
			);
			$RESULTS{$city_id}{'original'} = $oRow->{val};
		}
		case 'F' { 
			@options = ();
			@where = ( 
				" page_id = rev_page ",
				" date_format(rev_timestamp, '%Y%m') = '$month' ",
				" page_is_redirect = 0 ",
				" page_namespace in (".join(",", map { $dbl->quote($_) } @$contentNamespaces).") ",
				" page_len > 0 ",
				" page_id not in (select rev_page from revision date_format(rev_timestamp, '%Y%m') < '$month') " 
			);
			my $oRow = $dbl->select(
				" ROUND(COUNT(distinct(page_id))/DAY(LAST_DAY('".$month."01')), 0) as val ",
				" page, revision ",
				\@where,
				\@options
			);
			$RESULTS{$city_id}{'original'} = $oRow->{val};
		}
		case 'G' { 
			@options = ();
			@where = ( 
				" page_id = rev_page ",
				" date_format(rev_timestamp, '%Y%m') = '$month' ",
				" page_is_redirect = 0 ",
				" page_namespace in (".join(",", map { $dbl->quote($_) } @$contentNamespaces).") ",
				" page_len > 0 "
			);
			my $oRow = $dbl->select(
				" count(rev_id) as val ",
				" page, revision ",
				\@where,
				\@options
			);
			$RESULTS{$city_id}{'original'} = $oRow->{val};
		}
	}
	$dbl->disconnect();

	$RESULTS{$city_id}{'dbname'} = $databases->{$city_id};
	my $end_sec = time();
	my @ts = gmtime($end_sec - $start_sec);
	$oConf->log ($databases->{$city_id} . " processed (".$databases->{$city_id}.") ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]), 1);
	$main_loop++;
}
#---
$dbr->disconnect();
$dbr_stats->disconnect();

open (CSVFILE, ">>/tmp/".$month."_".$column.".csv");
print CSVFILE "Wikia(ID);Stats;Local database;Diff;\n";
foreach my $city_id ( keys %RESULTS ) {
	my $diff = Wikia::Utils->intval($RESULTS{$city_id}{'original'} - $RESULTS{$city_id}{'stats'});
	print CSVFILE $RESULTS{$city_id}{'dbname'} . " ($city_id);" . $RESULTS{$city_id}{'stats'} . ";" . $RESULTS{$city_id}{'original'}.";" . $diff . ";\n";
}
close (CSVFILE);

my $process_end_time = time();
@ts = gmtime($process_end_time - $process_start_time);
$oConf->log("\nscript processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]), 1);
$oConf->log("done", 1);

1;
