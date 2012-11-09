#!/usr/bin/perl
package EventsContentFix;

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
use Wikia::Settings;
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

sub new {
    my ($class, @args) = @_;
    my $self  = $class->SUPER::new;
    bless $self, $class;
}


sub update_event($$$$) {
	my ($self, $dbs, $row, $value) = @_;
	
	my @where = ( 
		"wiki_id = " . $dbs->quote($row->{city_id}),
		"page_id = " . $dbs->quote($row->{page_id}),
		"rev_id = " . $dbs->quote($row->{rev_id}),
		"log_id	= " . $dbs->quote($row->{log_id})
	);

	my %data = (
		"is_content" => $value
	);
	
	my $q = $dbs->update('events', \@where, \%data);	

	return $q;
}

package main;

use Thread::Pool::Simple;
use Data::Dumper;

print "Starting daemon ... \n";
# check time
my $script_start_time = time();

my $oEStats = new EventsContentFix();

# load balancer
my $lb = Wikia::LB->instance;
$lb->yml( $YML ) if defined $YML;

# connect to wikicitiee
my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::EXTERNALSHARED )} );

# connect to the stats db
my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS ) } );
print "Fetch data ($month records) \n";

my @where_db = ();
if ($usedbs) {
	if ( $usedbs && $usedbs =~ /\+/ ) {
		# dbname=+177
		$usedbs =~ s/\+//i;
		push @where_db, "wiki_id > " . $usedbs;
	} elsif ( $usedbs && $usedbs =~ /\-/ ) {
		# dbname=+177
		$usedbs =~ s/\-//i;
		push @where_db, "wiki_id < " . $usedbs;
	} else { 
		my @use_dbs = split /,/,$usedbs;
		push @where_db, "wiki_id in (".join(",", map { $dbs->quote($_) } @use_dbs).")";
	}
}
if ($todbs) {
	push @where_db, "wiki_id <= " . $todbs;
}

print "Fetch list of events \n";

my @db_fields = ('wiki_id', 'page_id', 'rev_id', 'log_id', 'page_ns', 'is_content');
my @options = ("order by wiki_id");
my $from = 'events';

my $sth = $dbs->select_many(join(',', @db_fields), $from, \@where_db, \@options);
my $prev_wiki = 0;
my $prev_dbname = '';
my $fixed = 0;
#--- set start time
my $start_sec = 0; 
while(my $values = $sth->fetchrow_hashref()) {	
	#--- set city;
	my $city_id = int $values->{wiki_id};

	my $oWikia = undef;
	my $content_namespaces = undef;
	if ( $prev_wiki != $city_id ) {
		if ( $start_sec > 0 ) {
			my $end_time = time();
			my @ts = gmtime($end_time - $start_sec);
			print "Wikia $prev_dbname finished after: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]) . " fixed: $fixed \n";
		}
		
		$start_sec = time();		
		$oWikia = $dbr->id_to_wikia($city_id);
		$content_namespaces = $dbr->__content_namespaces($city_id);
		print $oWikia->{city_dbname} . " processed (".$city_id."), content_ns = " . Dumper(@$content_namespaces) . "\n";
		
		$prev_wiki = $city_id;
		$prev_dbname = $oWikia->{city_dbname};
		$fixed = 0;
	}

	# check is content
	my $page_ns = $values->{page_ns};
	my $is_content = ( grep /^\Q$page_ns\E$/, @{$content_namespaces} );
	
	my $update_content = "";
	if ( !$is_content && $values->{is_content} eq 'Y' ) {
		$update_content = 'N';
	} elsif ( $is_content && $values->{is_content} eq 'N' ) {
		$update_content = 'Y';
	}

	if ( $update_content ne '' ) {
		$oEStats->update_event($dbs, $values, $update_content);
		$fixed++;
	}

}
$dbs->disconnect() if ( $dbs );
$dbr->disconnect() if ( $dbr );

my $script_end_time = time();
my @ts = gmtime($script_end_time - $script_start_time);

print "Process done: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);
1;
