#!/usr/bin/perl -w

#
# options
#
use common::sense;
use feature "say";

use FindBin qw/$Bin/;
use lib "$Bin/../lib/";

#
# private
#
use Wikia::Settings;
use Wikia::WikiFactory;
use Wikia::Utils;
use Wikia::LB;
use Wikia::SimpleQueue;

#
# public
#
use MediaWiki::API;
use Pod::Usage;
use Getopt::Long;
use Thread::Pool::Simple;
use Time::HiRes qw(gettimeofday tv_interval);
use Try::Tiny;
use List::Util qw(shuffle);
use DateTime;

package main;

sub worker {
	my( $worker_id, $event_id, $beacon_id, $rev_timestamp ) = @_;

	my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::METRICS );
	
	my $ibd = 0;
	my $sql = "SELECT user_id_via_beacon from metrics.event where event_id = " . $dbh->quote( $event_id ) ;
	my $sth = $dbh->prepare( $sql );
	if ( my $row = $sth->fetchrow_hashref ) {
		$ibd = $row->{"user_id_via_beacon"};
	}
	$sth->finish;
	
	if ( $ibd == 0 ) {
		my $sql = "SELECT user_id, event_id from metrics.event where beacon_id = " . $dbh->quote( $beacon_id ) . " and ";
		$sql .= "user_id > 0 and rev_timestamp < " . $dbh->quote( $rev_timestamp ) . " order by rev_timestamp desc limit 1";
		my $sth = $dbh->prepare( $sql );
		$sth->execute();
		if ( my $row = $sth->fetchrow_hashref ) {
			say "Update user_id_via_beacon for beacon_id: " . $beacon_id . " and event_id > " . $row->{"event_id"};
			$sql = "UPDATE metrics.event set user_id_via_beacon = " . $dbh->quote( $row->{"user_id"} ) . " where ";
			$sql .= "beacon_id = " . $dbh->quote( $beacon_id ) . " and event_id > " . $dbh->quote( $row->{"event_id"} ) . " and user_id = 0 ";
			$dbh->do( $sql );
		}
		$sth->finish;
	}
}

my ( $help, $workers, $month ) = undef;

$|++;        # switch off buffering
$workers     = 3; # by default 50 processes
$month = DateTime->now()->strftime("%Y%m");
GetOptions(
	"help|?"      => \$help,
	"workers=i"   => \$workers,
	"month=i"	  => \$month
) or pod2usage( 2 );



pod2usage( 1 ) if $help;

my $pool = Thread::Pool::Simple->new(
	min => 1,
	max => $workers,
	load => 4,
	do => [sub {
		worker( @_ );
	}],
	monitor => sub {
		say "done";
	},
	passid => 1,
);

my $start_sec = time();

my $first_date = Wikia::Utils->first_datetime($month);
my $last_date = Wikia::Utils->last_datetime($month);

say "Update all events for not empty user IDs and beacon IDs";
my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::METRICS );

my $sql = "update metrics.event set user_id_via_beacon = user_id ";
$sql .= "where user_id > 0 and beacon_id != '' ";
$sql .= "and rev_timestamp between '" . $first_date . "' and '" . $last_date . "' ";
$dbh->do( $sql );

my $end_sec = time();
my @ts = gmtime($end_sec - $start_sec);
say "Updated in: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);	

#####################################################
## update all events where user_id_via_beacon = 0 
#####################################################

$sql = "SELECT rev_timestamp, user_id, beacon_id as cnt from metrics.event where ";
$sql .= "user_id_via_beacon = 0 and user_id = 0 and beacon_id != '' and ";
$sql .= "rev_timestamp between '" . $first_date . "' and '" . $last_date . "' ";
my $sth = $dbh->prepare( $sql );
$sth->execute();
while( my $row = $sth->fetchrow_hashref ) {
	# quick job on local database
	say "Proceed beacon: " . $row->{ "beacon_id" }. " and ts: " . $row->{"rev_timestamp"};
	$pool->add( $row->{ "beacon_id" }, $row->{"rev_timestamp"} );
}
$sth->finish;

$pool->join;

1;
__END__

=head1 NAME

user_beacon.pl - update user_id_via_beacon column in event table

=head1 SYNOPSIS

user_beacon.pl [options]

 Options:
  --help            brief help message
  --workers=<nr>    how many workers should be run (default 10)

=head1 OPTIONS

=over 8

=item B<--help>

Print a brief help message and exits.

=head1 DESCRIPTION

B<This programm> iterates through all active databases in city_list and rebuild local_users table on statsdb
=cut
