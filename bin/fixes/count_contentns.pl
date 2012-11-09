#!/usr/bin/perl

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
my $usedbs = "";
my $notusedbs = "";
my $todbs = "";
my $dry = 0;
my $debug = 0;
GetOptions(
	'usedb=s' => \$usedbs,
	'notusedb=s' => \$notusedbs,
	'todbs=s' => \$todbs,
	'dry=s' => \$dry,
	'debug'	=> \$debug
);

my @disabled_ns = (2, 4, 6, 8, 10, 12, 14, 400, 700, 1000, 1010, 1200, 1202);

print "Starting daemon ... \n";
# check time
my $script_start_time = time();

# load balancer
my $lb = Wikia::LB->instance;
$lb->yml( $YML ) if defined $YML;

# connect to wikicitiee
my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::EXTERNALSHARED )} );

my @where_db = ('city_public = 1');
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
		push @where_db, "city_id in (".join(",", map { $dbr->quote($_) } @use_dbs).")";
	}
}
if ( $notusedbs ) {
	my @notuse_dbs = split /,/, $notusedbs;
	push @where_db, "city_id not in (".join(",", map { $dbr->quote($_) } @notuse_dbs).")";
}
if ($todbs) {
	push @where_db, "city_id <= " . $todbs;
}

print Dumper( @where_db );
print "Fetch list of Wikis \n";

my @db_fields = ('city_id', 'city_dbname');
my @options = ("order by city_id");
my $from = 'city_list';
my $sth = $dbr->select_many(join(',', @db_fields), $from, \@where_db, \@options);
my %namespace = ( 'content' => 0, 'our_content' => 0, 'content_wo_redirect' => 0, 'our_content_wo_redirect' => 0);
while(my $values = $sth->fetchrow_hashref()) {	
	#--- set city;
	my $city_id = int $values->{city_id};
	my $start_sec = time();		
	my $content_namespaces = $dbr->__content_namespaces($city_id);
	my $content_ns = join( ",", @$content_namespaces);
	print "Check: " . $values->{city_dbname} . " (".$city_id."), content_ns = " . $content_ns . "\n";
	
	# connect to Wikia
	my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, undef, $values->{city_dbname} ) } );
	
	# calculate MW content
	my @where = ( "page_namespace in ( $content_ns )" );
	my @options = ();
	my $oRow = $dbw->select( " count( page_id ) as val ", "page", \@where, \@options );
	$namespace{ content } += $oRow->{val};

	# calculate our content definition
	@where = ( "page_namespace in ( " . join(',', @disabled_ns) . ") and page_namespace % 2 = 0 ");
	@options = ();
	$oRow = $dbw->select( " count( page_id ) as val ", "page", \@where, \@options );
	$namespace{ our_content } += $oRow->{val};
	
	# calculate MW content w/o redirects
	@where = ( "page_namespace in ( $content_ns )", "page_is_redirect = 0");
	@options = ();
	$oRow = $dbw->select( " count( page_id ) as val ", "page", \@where, \@options );
	$namespace{ content_wo_redirect } += $oRow->{val};
	
	# calculate our content w/o redirects
	@where = ( "page_namespace in ( " . join(',', @disabled_ns) . ") and page_namespace % 2 = 0", "page_is_redirect = 0");
	@options = ();
	$oRow = $dbw->select( " count( page_id ) as val ", "page", \@where, \@options );
	$namespace{ our_content_wo_redirect } += $oRow->{val};
	
	my $end_time = time();
	my @ts = gmtime($end_time - $start_sec);
	print "Finished after: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]) . "\n";
}
$dbr->disconnect() if ( $dbr );

my $script_end_time = time();
my @ts = gmtime($script_end_time - $script_start_time);

print "Process done: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);

print Dumper( %namespace );
1;
