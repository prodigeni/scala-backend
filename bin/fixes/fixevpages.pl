#!/usr/bin/perl
package EventsRecFix;

use strict;
use warnings;
use Data::Dumper;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

my $YML = undef;
$YML = "$Bin/../../../wikia-conf/DB.moli.yml" if -e "$Bin/../../../wikia-conf/DB.moli.yml" ;

use Wikia::Utils;
use Wikia::LB;
use Wikia::DB;
use Wikia::Config;

use Switch;
use Getopt::Long;
use Data::Dumper;
use base qw/Class::Accessor::Fast/;

$|++;
my $workers = 10;
my $month = "";
my $usedbs = "";
my $todbs = "";
my $dry = 0;
my $debug = 0;
GetOptions(
	'workers=s' 	=> \$workers,
	'month=s'		=> \$month,
	'usedb=s'		=> \$usedbs,
	'todbs=s'	=> \$todbs,
	'dry=s'			=> \$dry,
	'debug'			=> \$debug
);

my $oConf = new Wikia::Config( {logfile => "/tmp/fixevents.log", csvfile => "/home/moli/$month/fixevents.sql"} );

sub new {
    my ($class, @args) = @_;
    my $self  = $class->SUPER::new;
    bless $self, $class;
}

sub fetch_data($;$$) {
	my ($self, $city_id, $dbname) = @_;
	my @res = ();
	#---
	
	my $lb = Wikia::LB->instance;
	$lb->yml( $YML ) if defined $YML;				
	my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', $dbname )} );
		
	my @db_fields = ('page_id', 'page_latest as rev_id', 'page_namespace');
	
	my $q = "SELECT " . join( ',', @db_fields ) . " from page, revision where page_id = rev_page order by page_id, rev_id" ;
	my $sth_w = $dbr->prepare($q);
	if ($sth_w->execute() ) {
		my %results;
		@results{@db_fields} = ();
		$sth_w->bind_columns( map { \$results{$_} } @db_fields );
		
		@res = (\%results, sub {$sth_w->fetch() }, $sth_w, $dbr);
	}
	
	return @res;	
}

sub update_redirect($$$) {
	my ($self, $dbs, $row, $value) = @_;
	
	my @options = ('ORDER BY rev_timestamp desc', 'LIMIT 1');
	my @where = ( 
		"wiki_id = " . $dbs->quote($row->{city_id}),
		"page_id = " . $dbs->quote($row->{page_id}),
		"rev_id = " . $dbs->quote($row->{rev_id})
	);
	my $oRow = $dbs->select(
		" is_redirect ",
		" events ",
		\@where,
		\@options
	);
	my $is_redirect = $oRow->{is_redirect};	
	my $res = 0;
	my $update_redirect = "";
	if ( defined $is_redirect ) {
		if ( $value == 1 && ( $is_redirect eq 'N' ) ) {
			$update_redirect = 'Y';
		} elsif ( $value == 0 && ( $is_redirect eq 'Y' ) ) {
			$update_redirect = 'N';
		}
	}
		
	if ( $update_redirect ne '' ) {
		my %data = (
			"is_redirect" => $is_redirect
		);
		
		my $q = $dbs->update('events', \@where, \%data);	
		$res = 1;	
	}
	
	return $res;
}

sub rec_exists($$$) {
	my ($self, $dbs, $row) = @_;
	
	my @options = ();
	my @where = ( 
		"wiki_id = " . $dbs->quote($row->{city_id}),
		"page_id = " . $dbs->quote($row->{page_id}),
		"rev_id = " . $dbs->quote($row->{rev_id})
	);
	my $oRow = $dbs->select(
		" count(0) as cnt ",
		" events ",
		\@where,
		\@options
	);
	my $cnt = $oRow->{cnt};
	
	return $cnt > 0;
}

package main;

use Thread::Pool::Simple;
use Data::Dumper;

print "Starting daemon ... \n";
# check time
my $script_start_time = time();

my $oEStats = new EventsRecFix();

# load balancer
my $lb = Wikia::LB->instance;
$lb->yml( $YML ) if defined $YML;

# connect to wikicitiee
my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::EXTERNALSHARED )} );

# connect to the stats db
my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS ) } );
print "Fetch data ($month records) \n";

my @where_db = ("city_public = 1", "city_url not like 'http://techteam-qa%'");
if ($usedbs) {
	if ( $usedbs && $usedbs =~ /\+/ ) {
		# dbname=+177
		$usedbs =~ s/\+//i;
		push @where_db, "city_id > " . $usedbs;
	} elsif ( $usedbs && $usedbs =~ /\-/ ) {
		# dbname=+177
		$usedbs =~ s/\-//i;
		push @where_db, "city_id < " . $usedbs;
	} else { 
		my @use_dbs = split /,/,$usedbs;
		push @where_db, "city_dbname in (".join(",", map { $dbr->quote($_) } @use_dbs).")";
	}
}
if ($todbs) {
	push @where_db, "city_id <= " . $todbs;
}

$oConf->log("get list of wikis from city list", 1);
my ($databases) = $dbr->get_wikis(\@where_db);
#$dbr->disconnect();

foreach my $num (sort ( map { sprintf("%012u",$_) } (keys %{$databases}) ))
{
	#--- set city;
	my $city_id = int $num;
	#--- set start time
	my $start_sec = time();
	print $databases->{$city_id} . " processed (".$city_id.")";

	my $oWikia = $dbr->id_to_wikia($city_id);
	# check events
	$lb->yml( $YML ) if defined $YML;				
	my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', $databases->{$city_id} )} );
		
	my @db_fields = ('page_id', 'rev_id', 'page_namespace', 'page_is_redirect');
	
	my @where = (
		"page_id = rev_page",
	);
	my @options = ("order by page_id, rev_id");
	my $from = 'page, revision';

	my $found = 0;
	my $fixed = 0;

	my $sth = $dbr->select_many(join(',', @db_fields), $from, \@where, \@options);
	
	if ($sth) {
		my $prev_page = 0;
		my $is_redirect = 0;
		my $last_rev = 0;
		while(my $values = $sth->fetchrow_hashref()) {	
			
			my $ev_id = 1;
			if ( $prev_page != $values->{page_id} ) {
				# check redirect 
				
				my $row = {
					"city_id" => $city_id,
					"page_id" => $prev_page,
					"rev_id"  => $last_rev
				};
							
				my $fix = $oEStats->update_redirect($dbs, $row, $is_redirect);
				if ( $fix == 1 ) {
					$fixed++;
				}
				
				$ev_id = 2;
				$prev_page = $values->{page_id};
			}

			my $row = {
				"city_id" => $city_id,
				"page_id" => $values->{page_id},
				"rev_id" => $values->{rev_id}				
			};
			
			my $rec_exists = $oEStats->rec_exists($dbs, $row);

			if ( !$rec_exists ) {
				my %data = (
					"ev_id"			=> Wikia::Utils->intval( $ev_id ),
					"city_id"		=> Wikia::Utils->intval( $city_id ),
					"page_id"		=> Wikia::Utils->intval( $values->{page_id} ),
					"rev_id"		=> Wikia::Utils->intval( $values->{rev_id} ),
					"city_server"	=> $oWikia->{server},
					"priority"		=> 1
				);

				my $res = $dbs->insert( 'scribe_events', "", \%data );	
				$found++;		
			}
			
			$is_redirect = $values->{page_is_redirect};
			$last_rev = $values->{rev_id};
		}
		
		if ( $is_redirect == 1 ) {
			my $row = {
				"city_id" => $city_id,
				"page_id" => $prev_page,
				"rev_id"  => $last_rev
			};
						
			$oEStats->update_redirect($dbs, $row);
		}		
	}
	
	my $end_time = time();
	my @ts = gmtime($end_time - $start_sec);

	print "Wikia finished after: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]) . " missing records: " . $found . ", fixed: $fixed \n";

}
$dbs->disconnect() if ( $dbs );
$dbr->disconnect() if ( $dbr );

my $script_end_time = time();
my @ts = gmtime($script_end_time - $script_start_time);

print "Process done: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);
1;
