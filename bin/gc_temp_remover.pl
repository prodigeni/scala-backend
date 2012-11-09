#!/usr/bin/perl -l

use common::sense;
use FindBin qw/$Bin/;
use lib "$Bin/../lib/";
use File::Path;
use DateTime;

use Getopt::Long qw(:config pass_through); # params for Wikia::LB
#
# database handler
#
use Wikia::LB;

#
# defaults
#
my $keep = 1; # days

my $dbh =  Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::EXTERNALSHARED );
my $sth = $dbh->prepare( "SELECT * FROM garbage_collector WHERE TO_DAYS(NOW()) - TO_DAYS(gc_timestamp) > ?" );
$sth->execute( $keep );
while( my $row = $sth->fetchrow_hashref ) {

	my $dt = DateTime->now();
	my $ts = DateTime->new(
		year   => substr( $row->{ "gc_timestamp" }, 0, 4 ),
		month  => substr( $row->{ "gc_timestamp" }, 4, 2 ),
		day    => substr( $row->{ "gc_timestamp" }, 6, 2 ),
		hour   => substr( $row->{ "gc_timestamp" }, 8, 2 ),
		minute => substr( $row->{ "gc_timestamp" }, 10, 2 ),
		second => substr( $row->{ "gc_timestamp" }, 12, 2 ),
	);
	my $ago = $dt - $ts;

	#
	# check if file exists, if so remove it
	#
	if( -f $row->{ 'gc_filename' } ) {
		print "removing file: $row->{ 'gc_filename' }, created: " . $ago->days . " days ago" ;
		unlink( $row->{ 'gc_filename' } );
	}
	else {
		print "missing file: $row->{ 'gc_filename' }, created: " . $ago->days . " days ago" ;
	}
	my @thumb = ();
	my @parts = split( "/", $row->{ 'gc_filename' } );
	push @thumb, pop @parts;
	push @thumb, pop @parts;
	push @thumb, pop @parts;
	push @thumb, "thumb";
	while ( @parts ) {
		push @thumb, pop @parts;
	}
	my $thumb = join( "/", reverse@thumb );
	if( -d $thumb ) {
		print "removing thumb folder: $thumb, created: " . $ago->days . " days ago";
		rmtree( $thumb )
	}

	#
	# remove row from database
	#
	my $sth2 = $dbh->prepare( "DELETE FROM garbage_collector WHERE gc_id = ? LIMIT 1" );
	$sth2->execute( $row->{ "gc_id" } );
}
