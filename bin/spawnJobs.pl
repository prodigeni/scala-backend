#!/usr/bin/perl -w

#
# options
#
use strict;
use common::sense;

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

package main;

=pod jobs_summary

CREATE TABLE jobs_summary (
	city_id  int(8) unsigned NOT NULL PRIMARY KEY,
	total int(8) unsigned default 0,
	`timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) Engine=InnoDB;

=cut

=pod jobs_dirty

CREATE TABLE `jobs_dirty` (
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `city_id` int(8) unsigned NOT NULL,
  KEY (`timestamp`)
) ENGINE=Memory;

=cut

=item worker

worker job, handle all stuffs

=cut
sub worker {
	my( $worker_id, $city_id, $max, $nodes, $skip_proxy ) = @_;

	#
	# proxy is reference to list
	#
	my $proxy = @{ [ shuffle( @$nodes ) ] }[ 0 ];
	my $queue = Wikia::SimpleQueue->instance( name => "spawnjob" );
	my $settings = Wikia::Settings->instance();

	my $t_start = [ gettimeofday() ];
	#
	# check if wiki is valid
	#
	my $wfactory = Wikia::WikiFactory->new( city_id => $city_id );
	my $wgServer = $wfactory->variables()->{ "wgServer" };
	my $delete = 0;
	my $requeue = 0;
	if( $wgServer ) {
		my $username = $settings->variables()->{ "wgWikiaBotUsers" }->{ "staff" }->{ "username" };
		my $password = $settings->variables()->{ "wgWikiaBotUsers" }->{ "staff" }->{ "password" };
		my $domain = shift @{ $wfactory->domains() };

		my $mw = MediaWiki::API->new();
		$mw->{config}->{api_url} = $wgServer . "/api.php";
		my $via = "";
		unless( $skip_proxy ) {
			$mw->{ua}->proxy( "http", $proxy );
			$via = "via $proxy";
		}
		$mw->{ua}->default_header( "X-MW-API-RunJob" => 1 );
		$mw->login( { lgname => $username, lgpassword => $password }  );

		if ( $mw->{error}->{code} > 0 ) {
		    # can't log in
			#
			# check, maybe wikis is redirected
			#
			if( $mw->{error}->{details} =~ /^302 Found/ ) {
				say "$city_id $domain $via: error ${ \$mw->{error}->{code} }: ${ \$mw->{error}->{details} }. Wiki is redirected, ignoring";
				$delete = 1;
			}
			else {
				say "$city_id $domain $via: error ${ \$mw->{error}->{code} }: ${ \$mw->{error}->{details} }. Requeing and sleeping 5s";
				$delete = 0;
			}
		} else {
			my $runjob = $mw->api( {
				action => "runjob",
				max    => $max
			} );
			if( $mw->{error}->{code} == 0 ) { # we're fine
				my $total = $runjob->{ "runjob" }->{ "left" }->{ "total" };
				my $done = $runjob->{ "runjob" }->{ "left" }->{ "done" };

				#
				# requeue if any jobs left
				#
				my $t_elapsed = tv_interval( $t_start, [ gettimeofday() ] ) ;
				if( $total ) {
					say "$city_id $domain $via: time $t_elapsed, $done done, some jobs left. Requeing";
					$delete = 0;
				}
				else {
					say "$city_id $domain $via: time $t_elapsed, $done done, 0 jobs left. Finishing";
					$delete = 1;
				}
			}
			else {
				my $t_elapsed = tv_interval( $t_start, [ gettimeofday() ] ) ;
				say "$city_id $domain $via: time $t_elapsed, error ${ \$mw->{error}->{code} }: ${ \$mw->{error}->{details} }. Requeing";
				$delete = 0;
			}
		}
	}
	else {
		say "Can't establish connection to city_id = {$city_id}";
		$delete = 1;
	}

	if ( $delete ) {
		say "Remove $city_id from queue";
		$queue->cleanup( $city_id );
	} else {
		say "Requeing $city_id";
		$queue->push( $city_id );
	}
}

my ( $help, $max, $timestamp, $skip_proxy, $refill, $workers, $refill_only ) = undef;

$|++;        # switch off buffering
$max         = 5;  # by default 5 jobs
$workers     = 10; # by default spawn 10 processes
$refill_only = 0;
GetOptions(
	"max=i"       => \$max,
	"help|?"      => \$help,
	"skip-proxy"  => \$skip_proxy,
	"refill"      => \$refill,
	"refill-only" => \$refill_only,
	"workers=i"   => \$workers
) or pod2usage( 2 );

pod2usage( 1 ) if $help;

#
# discover all active apaches (hackish way)
#
#my $output = qx(knife search node '(roles:SJC AND roles:wikia-www-new) NOT roles:NoDeploy NOT roles:wikia-www-adtest NOT roles:wikia-www-stage NOT roles:wikia-www-preview'  |grep 'Node Name');
my $output = qx(knife search node '(roles:SJC AND roles:wikia-www-new AND recipe:cron-machine) NOT roles:NoDeploy'  |grep 'Node Name');

say "Looking for nodes...";
my @nodes = ();
while ( $output =~ m/^Node Name:\s*([\w\-]+)$/mgs ) {
	my $node = "http://$1:80/";
	say "Found $node";
	push @nodes, $node;
}

#
# refill option
# @todo use one connection and switch to database
#
my $queue = Wikia::SimpleQueue->instance( name => "spawnjob" );
if( $refill || $refill_only ) {
	my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::EXTERNALSHARED );
	my $sth = $dbh->prepare( "SELECT city_id, city_dbname FROM city_list WHERE city_public = 1" );
	$sth->execute();
	while( my $row = $sth->fetchrow_hashref ) {
		# quick job on local database
		my $dbl = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, $row->{ "city_dbname" } );
		if( $dbl ) {
			my $job = $dbl->selectrow_hashref( "SELECT * FROM job" );
			if( $job ) {
				say "Jobs found in ${ \$row->{city_dbname} } (${ \$row->{city_id}}). Added to queue.";
				$queue->push( $row->{ "city_id" } );
			}
			$dbl->disconnect();
		}
	}
	$sth->finish;
}

exit( 0 ) if $refill_only;

my $pool = Thread::Pool::Simple->new(
	min => 2,
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

while ( my $city_id = $queue->pop() ) {
	$pool->add( $city_id, $max, \@nodes, $skip_proxy );
}

$pool->join;
say "Queue is empty";
$queue->unlockAll();
1;
__END__

=head1 NAME

spawnJobs.pl - smart spawner for MediaWiki maintenance/runJobs.php

=head1 SYNOPSIS

spawnJobs.pl [options]

 Options:
  --help            brief help message
  --skip-proxy      skip proxy defined in settings file
  --refill          refill Redis queue with sites with penging jobs
  --refill-only     exit after refilling, do not process queue
  --max=<nr>        how many jobs should be run in api call (default 5)
  --workers=<nr>    how many workers should be spawned (default 10)

=head1 OPTIONS

=over 8

=item B<--help>

Print a brief help message and exits.

=item B<--refill>

Iterate through all active databases in city_list, check if there are jobs in
job table, add to Redis all sites with pending jobs

=item B<--skip-proxy>

On production servers api connection is rerouted via local proxy. Use this param to skip this.
=back

=head1 DESCRIPTION

B<This programm> will call API method for running jobs on MediaWiki instance.
This is proof-of-concept version, it is for checking if jobs can be run by apaches.
=cut
