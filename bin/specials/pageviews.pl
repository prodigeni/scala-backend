#!/usr/bin/perl

use strict;
my $YML = undef;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

BEGIN {
	$YML = "$Bin/../../../wikia-conf/DB.localhost.yml" if ($Bin =~ /moli/) ;
}

use DBI;
use Wikia::LB;
use Wikia::DB;
use Wikia::Utils;
use Wikia::Config;

use Getopt::Long;
use Data::Dumper;

my $oConf = new Wikia::Config( { logfile => "/tmp/pageviews.log" } );

$|++;

my $weeks = 4;
my $insert = 500;
GetOptions(
	'insert=s'		=> \$insert,
	'weeks=s' 		=> \$weeks 
);

#read long options

#----
my $lb = Wikia::LB->instance;
$lb->yml( $YML ) if $YML;
my $dbh = $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::STATS );
#----
my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED )} );
my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS )} );

my $process_start_time = time();

my $oRow = $dbw->select('date_format(date_sub(now(), INTERVAL ' . $weeks . ' week), \'%Y%m%d\') as sdate', 'dual');
my $sdate = $oRow->{sdate};

$oConf->log("get all data from page_views_tags");
my $q = "SELECT city_id, tag_id, city_lang, sum(pv_views) FROM stats.page_views_tags WHERE use_date > ". $sdate . " GROUP BY 1, 2, 3 ORDER BY null";
my $sth = $dbh->prepare($q);
if($sth->execute()) {
	my $loop=0;
	my $y = 0;

	$oConf->log("Build array with records");	
	my $keys = ['city_id', 'tag_id', 'city_lang', 'pv_views'];
	my $records = [];
    while(my ($city_id, $tag_id, $city_lang, $pv_views) = $sth->fetchrow_array()) {
		
		$y++ if ( $loop > 0 && $loop % $insert == 0 );
		$records->[$y] = [] unless $records->[$y];
		
		my @data = (
			Wikia::Utils->intval($city_id),
			$tag_id,
			$city_lang,
			$pv_views
		);
		push @{$records->[$y]}, "(" . join(",", map { $dbr->quote($_) } @data). ")";
		undef(@data);		

		$loop++;
    }

	$oConf->log("Insert to the database");

	if ( scalar @$records ) {
		$oConf->log("Remove old data");
		$dbw->execute("BEGIN");	
		$dbw->execute("DELETE FROM specials.page_views_summary_tags");		
		my $x = 1;
		foreach my $k ( @{$records} ) {
			my $values = join(",", map { $_ } @$k);
			if ( $values ) {
				$oConf->log("added $x pack of records");
				my $sql = "INSERT IGNORE INTO specials.page_views_summary_tags  ( " . join(',', @$keys) . " ) values " . $values;
				$sql = $dbw->execute($sql);
				sleep(1);
			}
			$x++;
		}
		$dbw->execute("COMMIT");		
	}
} 


$oConf->log("\n\nget all data for page_views_articles");

$oConf->log("get all data from page_views_tags");
$q = "SELECT pv_city_id, pv_page_id, pv_namespace, sum(pv_views) FROM stats.page_views_articles WHERE pv_use_date > ". $sdate . " GROUP BY 1, 2, 3 ORDER BY null";
$sth = $dbh->prepare($q);
if($sth->execute()) {
	my $loop=0;
	my $y = 0;
	$oConf->log("Build array with records");	
	my $keys = ['city_id', 'page_id', 'page_ns', 'pv_views'];
	my $records = [];
    while(my ($city_id, $page_id, $page_ns, $pv_views) = $sth->fetchrow_array()) {
		
		$y++ if ( $loop > 0 && $loop % $insert == 0 );
		$records->[$y] = [] unless $records->[$y];
		
		my @data = (
			Wikia::Utils->intval($city_id),
			$page_id,
			$page_ns,
			$pv_views
		);
		push @{$records->[$y]}, "(" . join(",", map { $dbr->quote($_) } @data). ")";
		undef(@data);		

		$loop++;
    }	

	$oConf->log("Insert to the database");

	if ( scalar @$records ) {
		$oConf->log("Remove old data");
		$dbw->execute("BEGIN");	
		$dbw->execute("DELETE FROM specials.page_views_summary_articles");
			
		my $x = 1;
		foreach my $k ( @{$records} ) {
			my $values = join(",", map { $_ } @$k);
			if ( $values ) {
				$oConf->log("added $x pack of records");
				my $sql = "INSERT IGNORE INTO specials.page_views_summary_articles  ( " . join(',', @$keys) . " ) values " . $values;
				$sql = $dbw->execute($sql);
				sleep(2);
			}
			$x++;
		}
		$dbw->execute("COMMIT");		
	}
} 

#----
# get data from databases
#----
$dbh->disconnect();
$dbr->disconnect();
$dbw->disconnect();

my $process_end_time = time();
my @ts = gmtime($process_end_time - $process_start_time);
$oConf->log ("\n\nscript processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]));
$oConf->log ("done");

1;
