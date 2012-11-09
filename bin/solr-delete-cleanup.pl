#!/usr/bin/perl

use strict;
use warnings;
use LWP::UserAgent;
use Data::Dumper;
use JSON::XS;
use URI::Escape;
use DBI;
$|++;
use DBD::mysql;
binmode STDIN, ":utf8";
binmode STDOUT, ":utf8";
binmode STDERR, ":utf8";

my $deletes = 0;

my ($days) = @ARGV;

my $username = "search";
my $password = "japOxjekset3";
my $stat_dbh = DBI->connect("DBI:mysql:database=stats;host=statsdb-s1", $username, $password, {RaiseError => 1, AutoCommit => 0});
my $mw_dbh = DBI->connect("DBI:mysql:database=wikicities;host=db-sa1", $username, $password, {RaiseError => 1, AutoCommit => 0});
my $db_cities = $mw_dbh->prepare("SELECT city_id,city_url from city_list");
my $db_events = $stat_dbh->prepare("SELECT wiki_id,page_id from events where event_type = 3 and rev_timestamp >= date_sub(now(), interval ? day)");

$db_cities->execute();
my %cities;
while ( my ($city_id,$city_url) = $db_cities->fetchrow_array() ) {
    $cities{$city_id} = $city_url;
}
$mw_dbh->disconnect;

$stat_dbh->do("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;");
$db_events->execute($days);
while ( my ($wiki_id,$page_id) = $db_events->fetchrow_array() ) {
    my %event = ( 'serverName'  => $cities{$wiki_id},
		  'pageId' => $page_id );
    open my $scribe_cat, "|-", "/usr/bin/scribe_cat", "search_bulk"
	or die "Could not run scribe_cat: $!";
    print {$scribe_cat} encode_json(\%event);
    close $scribe_cat;
    $deletes++;
}
$stat_dbh->disconnect;

print "Removed articles: $deletes\n";
