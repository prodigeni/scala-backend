#!/usr/bin/perl -w

use Pod::Usage;
use Getopt::Long;

$|++;
GetOptions(
	'port=i' 			=> \( my $port        = 9991 ),
	'workers=i' 		=> \( my $workers     = 10 ),
	'queue_host=s' 		=> \( my $hqueue      = '127.0.0.1' ),
	'queue_port=i'		=> \( my $pqueue      = 1218 ),
	'insert=i'			=> \( my $insert      = 25 ),
	'debug'				=> \( my $debug       = 0 ),
	'help|?'			=> \( my $help        = 0 ),
	'iowa'				=> \( my $iowa        = 0 )
) or pod2usage( 2 );
pod2usage( 1 ) if $help;

package main;

use common::sense;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib";
use Wikia::Events;

say "port: $port, workers: $workers, queue host: $hqueue, queue port: $pqueue" if ( $debug );
my $events = new Wikia::Events( { debug => $debug, host => $hqueue, port => $pqueue, workers => $workers, iowa => $iowa } );
$events->run();

__END__

=head1 NAME

queue-to-db.pl - move dirty events from HttpSQS to MySQL database 

=head1 SYNOPSIS

queue-to-db.pl [options]

 Options:
  --help                    brief help message
  --port=<9990>             server port (default: 9991)
  --workers=<nr>            number of workers (default 2)
  --queue_host=<host>' 		HttpSQS host,
  --queue_port=<port>'		HttpSQS port (default: 1218),
  --insert=<25>				number of SQL insert statements in one query
  --debug                   enable debug option

=head1 OPTIONS

=over 8

=item B<--help>:
  Brief help message
	
=item B<--port>:
  Server port (default: 9990)
  
=item B<--workers>:
  Number of daemon workers (default: 2)

=item B<--queue_host>:		
  Queue (HttpSQS) host
  
=item B<--queue_port>:		
  Queue (HttpSQS) port (default: 1218)

=item B<--insert>:
  Number of SQL insert statements in one SQL query (default 25)
  
=item B<--debug>:
  Enable debug option
  
=head1 DESCRIPTION

B<This programm> collects all Scribe requests in dirty_events MySQL table.
=cut
