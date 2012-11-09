#!/usr/bin/perl -w

use Pod::Usage;
use Getopt::Long;

$|++;
# parse params
GetOptions(
	'port=i' 			=> \( my $port        = 9990 ),
	'workers=i' 		=> \( my $workers     = 2 ),
	'queue_host=s' 		=> \( my $hqueue      = '127.0.0.1' ),
	'queue_port=i'		=> \( my $pqueue      = 1218 ),
	'debug'				=> \( my $debug       = 0 ),
	'recv-timeout=s'	=> \( my $recvTimeout = 10000 ),
	'send-timeout=s'	=> \( my $sendTimeout = 10000 ),	
	'help|?'			=> \( my $help        = 0 )
) or pod2usage( 2 );
pod2usage( 1 ) if $help;

package main;
use common::sense;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib";
use Wikia::SimplePreforkServer;
use Wikia::DirtyEvents;

=test 
use Wikia::Utils;
for ( my $i = 0; $i < 100000; $i++ ) {
	my $params = {
		'cityId' => '177',
		'serverName' => 'http://www.wikia.com',
		'userIp' => '127.0.0.1',
		'hostname' => 'ap-s24',
		'beaconId' => 'R242ETGR',
		'archive' => '0',
		'pageId' => '242971',
		'pageNamespace' => '0',
		'revId' => '0',
		'userId' => '115748',
		'userIsBot' => '0',
		'isContent' => '1',
		'isRedirect' => '0',
		'revTimestamp' => '2011-10-17 13:35:01',
		'revSize' => '172',
		'mediaType' => '0',
		'imageLinks' => '1',
		'videoLinks' => '0',
		'totalWords' => '30',
		'languageId' => 75,
		'categoryId' => 3,
		'pageTitle' => 'This_is_the_test!'
	};	
	my @message = ( Wikia::Utils->json_encode({ 'category' => 'log_edit', 'message' => $params } ) );
	print "call 1 \n";

	my $to_send = Wikia::Utils->json_encode(@message);
	my $obj = new Wikia::DirtyEvents( { debug => $debug, host => $hqueue, port => $pqueue} );
	$obj->Log( \@message );
}
=cut
say "port: $port, workers: $workers, queue host: $hqueue, queue port: $pqueue" if ( $debug );
my $oEvent = new Wikia::DirtyEvents( { debug => $debug, host => $hqueue, port => $pqueue} );
my $server = Wikia::SimplePreforkServer->new( $oEvent, '', $port, $workers, $sendTimeout, $recvTimeout );
$server->run;

__END__

=head1 NAME

scribe-server.pl - collect Scribe "drity events in HttpSQS queue

=head1 SYNOPSIS

scribe-server.pl [options]

 Options:
  --help                    brief help message
  --port=<9990>             server port (default: 9990)
  --workers=<nr>            number of workers (default 2)
  --recv-timeout=<milisec>  number of miliseconds for the client to receive " response
  --send-timeout=<milisec>  number of miliseconds for the client to send reply
  --queue_host=<host>' 		HttpSQS host,
  --queue_port=<port>'		HttpSQS port (default: 1218),
  --debug                   enable debug option

=head1 OPTIONS

=over 8

=item B<--help>
  Brief help message

=item B<--port>
  Server port (default: 9990)
  
=item B<--workers>
  Number of daemon workers (default: 2)

=item B<--recv-timeout>
  Number of miliseconds for the client to receive the response
  
=item B<--send-timeout>
  Number of miliseconds for the client to send reply
  
=item B<--queue_host>		
  Queue (HttpSQS) host
  
=item B<--queue_port>		
  Queue (HttpSQS) port (default: 1218)
  
=item B<--debug>
  Enable debug option
  
=head1 DESCRIPTION

B<This programm> collects all Scribe requests in dirty_events MySQL table.
=cut
