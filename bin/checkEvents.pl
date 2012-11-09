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

package main;

sub worker {
	my( $worker_id, $city_id, $month, $iowa ) = @_;

	my $cmd = qq(/usr/bin/perl /usr/wikia/source/backend/bin/fixes/fixevents.pl --usedb=+$city_id --todbs=$city_id --month=$month);
	if ( $iowa ) {
		$cmd = qq(/usr/bin/perl /usr/wikia/source/backend/bin/fixes/fixiowaevent.pl --usedb=+$city_id --todbs=$city_id --month=$month);
	}
	my $result = 1;
	if (system($cmd) != 0) {
		say "Failed to run $cmd";
		$result = 0;
	}
	return $result;
}

my ( $help, $workers, $month, $iowa ) = undef;

$|++;        # switch off buffering
$workers     = 50; # by default 50 processes
GetOptions(
	"help|?"      	=> \$help,
	"workers=i"     => \$workers,
	"month=s"		=> \$month,
	"iowa=i"		=> \$iowa
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

my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::EXTERNALSHARED );
my $sth = $dbh->prepare( "SELECT city_id, city_dbname FROM city_list WHERE city_public = 1" );
$sth->execute();
while( my $row = $sth->fetchrow_hashref ) {
	# quick job on local database
	say "Proceed " . $row->{ "city_dbname" } . " (" . $row->{ "city_id" }. ")" ;
	$pool->add( $row->{ "city_id" }, $month, $iowa );
}
$sth->finish;

$pool->join;

1;
__END__

=head1 NAME

checkEvents.pl - check all events in events table

=head1 SYNOPSIS

checkEvents.pl [options]

 Options:
  --help            brief help message
  --workers=<nr>    number of workers (default 50)
  --month=<YYYYMM>  year and month to check
  --iowa=<0|1> 		check Iowa events? default 0

=head1 OPTIONS

=over 8

=item B<--help>

Print a brief help message and exits.

=head1 DESCRIPTION

B<This programm> iterates through all active databases in city_list and check events in event table
=cut
