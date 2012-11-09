#!/usr/bin/perl

package StatsHandler;

use strict;
use warnings;
use Data::Dumper;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

my $YML = undef;
BEGIN {
	$YML = "$Bin/../../../wikia-conf/DB.localhost.yml" if ($Bin =~ /moli/) ;
}

use Wikia::SimplePreforkServer;
use Wikia::Scribe;
use Wikia::Utils;
use Wikia::LB;
use Wikia::DB;
use Wikia::Settings;
use Wikia::Log;

use Switch;
use Getopt::Long;
use Data::Dumper;
use base qw/Class::Accessor::Fast/;

=pod table

CREATE TABLE `scribe_events` (
  `ev_id` tinyint(2) unsigned NOT NULL,
  `city_id` int(8) unsigned NOT NULL,
  `page_id` int(8) unsigned NOT NULL,
  `rev_id` int(8) unsigned NOT NULL,
  `log_id` int(8) unsigned NOT NULL default '0',
  `city_server` varchar(255) NOT NULL,
  `ev_date` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  PRIMARY KEY (`ev_id`, `city_id`, `page_id`, `rev_id`, `log_id`),
  KEY add_date(`ev_date`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

=cut

$|++;
my $port = 9090;
my $workers = 1;
my $debug = 0;
my $insert = 66;
my ($help, $recvTimeout, $sendTimeout) = ();
GetOptions(
	'port=s' 		=> \$port, 
	'workers=s' 	=> \$workers,
	'recvTimeout=s' => \$recvTimeout,
	'sendTimeout=s' => \$sendTimeout,
	'insert=s' 		=> \$insert,
	'debug'			=> \$debug,
	'help'			=> \$help
);
my $test_data = '{"cityId":"177","pageId":"28725","revId":0,"logId":14257,"serverName":"http:\/\/www.lan.jumon.net"}';
my $test_cat  = 'delete_log';

sub new {
    my ($class, @args) = @_;
    my $self  = $class->SUPER::new;
    bless $self, $class;
}

sub help {
    my ($self) = @_;
    my $name = __FILE__; 
    print <<EOF
$name [--help] [--listen_mq] [--daemon]
	port=9090		=> default port 9090,
	workers=s 		=> number of server instances (default 1),
	recvTimeout=s		=> default 10000 ms
	sendTimeout=s 		=> default 10000 ms
	insert=s 		=> number of multi inserts (default 66)
	debug			=> debug enabled
EOF
;
}


sub Log {
	my ($self, $messages) = @_;

	# check time
	my $process_start_time = time();
	
	# default result;
	my $ok = 1;
	my ($processed, $invalid) = 0;
	my $sc_keys = {};
	if ( defined($messages) && UNIVERSAL::isa($messages,'ARRAY') ) {
		my $loop = 1;
		
		print "Number of messages: " . scalar @$messages . "\n"; 
		# connect to db 
		my $lb = Wikia::LB->instance;
		$lb->yml( $YML ) if defined $YML;
		my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS )} );

		# inserts array 
		my $records = [];
		my $hosts = {};
		# get messages from Scribe
		my $y = 0;
		foreach ( @$messages ) {
			# from scribe
			my $s_key = $_->{category};
			my $s_msg = $_->{message};

			print sprintf("\t%d. %s: %s\n", $loop, $s_key, $s_msg) if ( $debug );
			$sc_keys->{$s_key} = 0 unless ( $sc_keys->{$s_key} ) ;
			$sc_keys->{$s_key}++;

			# decode message
			my $oMW = Wikia::Utils->json_decode($_->{message});
			# keys
			my $allowed_keys = [keys %{$Wikia::Scribe::scribeKeys}];
			
			# index of array inserts table
			$y = ($loop % $insert) == 0 ? ++$y : $y;
			@{$records->[$y]} = () unless $records->[$y];
			#print "$y, $loop % $insert = " . ( $loop % $insert ) . "\n";
			
			# check response
			my ($id, $id_value) = ();
			if ( UNIVERSAL::isa($oMW, 'HASH') && ( Wikia::Utils->in_array($s_key, $allowed_keys) ) ) { 
				# server name and identifier of page is not set
				if ( !$oMW->{serverName} || !$oMW->{pageId} ) {
					print "\tInvalid message text: " . $s_msg . "\n" if ( $debug );
					$invalid++;
					next;
				}
				
				my %data = (
					"ev_id"			=> Wikia::Utils->intval($Wikia::Scribe::scribeKeys->{$s_key}),
					"city_id" 		=> Wikia::Utils->intval($oMW->{cityId}),
					"page_id" 		=> Wikia::Utils->intval($oMW->{pageId}),
					"rev_id" 		=> Wikia::Utils->intval($oMW->{revId}),
					"log_id" 		=> Wikia::Utils->intval($oMW->{logId}),
					"priority"		=> Wikia::Utils->intval($oMW->{archive}),
					"city_server" 	=> $oMW->{serverName},
					"beacon_id"		=> ( defined( $oMW->{beaconId} ) ) ? $oMW->{beaconId} : ''
				);
				
				print sprintf("%0d, %0d, %0d, %0d, %0d, %0d, %s, %s \n", 
					Wikia::Utils->intval($Wikia::Scribe::scribeKeys->{$s_key}),
					Wikia::Utils->intval($oMW->{cityId}),
					Wikia::Utils->intval($oMW->{pageId}),
					Wikia::Utils->intval($oMW->{revId}),
					Wikia::Utils->intval($oMW->{logId}),
					Wikia::Utils->intval($oMW->{archive}),
					$oMW->{serverName},
					( defined( $oMW->{beaconId} ) ) ? $oMW->{beaconId} : ''
				);
				
				my $ins = "";
				if ( scalar @{$records->[$y]} == 0 ) {
					$ins = "INSERT IGNORE " . Wikia::Scribe::SCRIBE_EVENTS_TABLE . " (".join(",", (keys %data)).") values ";
				}
				push @{$records->[$y]}, $ins . "(" . join(",", map { $dbs->quote($_) } (values %data) ) .")";
					
				# update hostname 
				if ( defined $oMW->{hostname} ) {
					$hosts->{$oMW->{hostname}}  = 0 unless ( $hosts->{$oMW->{hostname}} ) ;
					$hosts->{$oMW->{hostname}}++;
				} 
				
				$processed++;
				$loop++;
			}
			undef($allowed_keys);
		}
		
		if ( scalar @$records ) {
			foreach my $y ( @$records ) { 
				my $sql = join(",", map { $_ } @$y);
				if ( $sql ) {
					$sql = $dbs->execute($sql);
				}
			}
		}
		
		if ( scalar keys %$hosts ) {
			foreach my $hostname ( keys %$hosts ) {
				my $sql = "insert into scribe_log ( hostname, logdate, logcount ) values ";
				$sql .= " ( '" . $hostname . "', curdate()+0, " . $hosts->{$hostname} . " ) ";
				$sql .= " ON DUPLICATE KEY UPDATE logcount = logcount + values(logcount) ";
				$sql = $dbs->execute($sql);				
			}
		}
	}

	my $process_end_time = time();
	my @ts = gmtime($process_end_time - $process_start_time);
	
	my $msg = "";
	if ( scalar keys %$sc_keys ) {
		foreach my $sck ( sort keys %$sc_keys) {
			$msg .= $sck . ": " . $sc_keys->{$sck} . ",";
		}
	}
	print "\n" . $msg . "\n";
	print sprintf("result: %0d records, %0d invalid messages\n", Wikia::Utils->intval($processed), Wikia::Utils->intval($invalid) );
	print "messages processed: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);

	print "ok = $ok \n" if ( $debug );
	
	# update log #bugid: 6713
	if ( $ok ) {
		my $log = Wikia::Log->new( name => "scribec" );
		$log->update();
	}
				
	return ($ok) ? Scribe::Thrift::ResultCode::OK : Scribe::Thrift::ResultCode::TRY_LATER;
}

package main;

=pod test

my $handler = new StatsHandler;
my $record = { 'category' => $test_cat, 'message' => $test_data};
my @message = ($record);
$handler->Log(\@message);

=cut

use Data::Dumper;
use Wikia::Settings;
use FindBin qw/$Bin/;
use lib "$Bin/../lib";

=pod test

#http://janitor.wikia.com/api.php?action=query&prop=wkevinfo&pageid=2032&revid=4195&meta=siteinfo&siprop=wikidesc&format=json
my $settings = Wikia::Settings->instance;
my $t = $settings->variables();
my $params = {
	'action' => 'query',
	'prop' => 'wkevinfo',
	'pageid' => 2032,
	'revid' => 4195,
	'meta' => 'siteinfo',
	'siprop' => 'wikidesc',
	'format' => 'json'
};	
my $login = {
	'username' => $t->{ "wgWikiaBotUsers" }->{ "staff" }->{ "username" },
	'password' => $t->{ "wgWikiaBotUsers" }->{ "staff" }->{ "password" }
};
print "call 1 \n";
my $response = Wikia::Utils->call_mw_api('janitor.wikia.com', $params); 
if ( !defined $response ) {
	print "call 2 \n";
	$response = Wikia::Utils->call_mw_api('janitor.wikia.com', $params, $login); 
}
print "Res: " . Dumper($response);
exit;

=cut

print "port = $port, workers = $workers \n" if ( $debug );
if ( $help ) {
	StatsHandler::help();
	exit;
}
my $server = Wikia::SimplePreforkServer->new( new StatsHandler, '', $port, $workers, $sendTimeout, $recvTimeout );
$server->run;
