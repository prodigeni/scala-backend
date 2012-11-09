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

#read long options

#----
my $lb = Wikia::LB->instance;
$lb->yml( $YML ) if $YML;
my $dbh = $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::STATS );
#----
my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED )} );
my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS )} );

my $process_start_time = time();
$oConf->log("get all page_views");
my $q = "select pv_city_id , pv_use_date, pv_namespace, pv_views, pv_timestamp, pv_city_lang from city_page_views ";
my $sth = $dbh->prepare($q);
my $languages = {};
if($sth->execute()) {
	my $loop=1;
    while(my ($pv_city_id , $pv_use_date, $pv_namespace, $pv_views, $pv_timestamp, $pv_city_lang) = $sth->fetchrow_array()) {
		if ( !$languages->{$pv_city_lang} ) {
			my $city_lang = $dbr->get_lang_by_code($pv_city_lang);
			$languages->{$pv_city_lang} = $city_lang->{lang_id};
			$languages->{$pv_city_lang} = 75 unless $languages->{$pv_city_lang};
		}
    	
    	$pv_use_date =~ s/\-//g;
    	
		my %data = (
			"pv_city_id" 	=> Wikia::Utils->intval($pv_city_id),
			"pv_use_date"	=> $pv_use_date, 
			"pv_namespace" 	=> $pv_namespace,
			"pv_views"		=> $pv_views,
			"pv_city_lang"	=> $languages->{$pv_city_lang},
			"pv_ts"			=> $pv_timestamp
		);

		my @options = (
			" ON DUPLICATE KEY UPDATE pv_views = pv_views + values(pv_views) ",
		);
		
		my $ins = $dbw->insert( "page_views", "", \%data, \@options, 1 );    	
		$oConf->log("+") if ( ( $loop % 100000 ) == 0 );
		$loop++;
    }
} 


$oConf->log("\n\nget all tags page_views");

$q = "select city_id, tag_id, use_date, city_lang, namespace, pviews, ts from tags_pv ";
$sth = $dbh->prepare($q);
if($sth->execute()) {
	my $loop = 1;
    while(my ($pv_city_id, $tag_id, $pv_use_date, $pv_city_lang, $pv_namespace, $pv_views, $pv_timestamp) = $sth->fetchrow_array()) {
		if ( !$languages->{$pv_city_lang} ) {
			my $city_lang = $dbr->get_lang_by_code($pv_city_lang);
			$languages->{$pv_city_lang} = $city_lang->{lang_id};
			$languages->{$pv_city_lang} = 75 unless $languages->{$pv_city_lang};
		}
    	
    	$pv_use_date =~ s/\-//g;
    	
		my %data = (
			"city_id" 		=> Wikia::Utils->intval($pv_city_id),
			"tag_id"		=> Wikia::Utils->intval($tag_id),
			"use_date"		=> $pv_use_date, 
			"namespace" 	=> $pv_namespace,
			"pv_views"		=> $pv_views,
			"city_lang"		=> $languages->{$pv_city_lang},
			"ts"			=> $pv_timestamp
		);
		my @options = (
			" ON DUPLICATE KEY UPDATE pv_views = pv_views + values(pv_views) ",
		);
		my $ins = $dbw->insert( "page_views_tags", "", \%data, \@options, 1  );    	
		$oConf->log("+") if ( ( $loop % 100000 ) == 0 );
		$loop++;		
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
