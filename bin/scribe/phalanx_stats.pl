#!/usr/bin/perl
package PhalanxStatsHandler;

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

=table
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
		my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::DATAWARESHARED )} );

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
			my $allowed_keys = [Wikia::Scribe::PHALANX_CATEGORY];
			
			# index of array inserts table
			$y = ($loop % $insert) == 0 ? ++$y : $y;
			@{$records->[$y]} = () unless $records->[$y];
			#print "$y, $loop % $insert = " . ( $loop % $insert ) . "\n";
			
			# check response
			my ($id, $id_value) = ();
			if ( UNIVERSAL::isa($oMW, 'HASH') && ( Wikia::Utils->in_array($s_key, $allowed_keys) ) ) { 
				# server name and identifier of page is not set
				if ( !$oMW->{blockId} || !$oMW->{blockUser} ) {
					print "\tInvalid message text: " . $s_msg . "\n" if ( $debug );
					$invalid++;
					next;
				}
				
				my %data = (
					'ps_blocker_id' 	=> Wikia::Utils->intval($oMW->{blockId}),
					'ps_blocker_type'	=> Wikia::Utils->intval($oMW->{blockType}),
					'ps_timestamp'		=> Wikia::Utils->intval($oMW->{blockTs}),
					'ps_blocked_user'	=> $oMW->{blockUser},
					'ps_wiki_id'		=> Wikia::Utils->intval($oMW->{city_id}),
				);
				
				#print sprintf ("%s \n", join (", ", values %data ) );
				
				my $ins = "";
				if ( scalar @{$records->[$y]} == 0 ) {
					$ins = "INSERT IGNORE " . Wikia::Scribe::SCRIBE_PHALANX_TABLE . " (".join(",", (keys %data)).") values ";
				}
				push @{$records->[$y]}, $ins . "(" . join(",", map { $dbs->quote($_) } (values %data) ) .")";
				
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
		my $log = Wikia::Log->new( name => "phalanxd" );
		$log->update();
	}
	
	return ($ok) ? Scribe::Thrift::ResultCode::OK : Scribe::Thrift::ResultCode::TRY_LATER;
}

package main;

use Data::Dumper;
use Wikia::Settings;
use FindBin qw/$Bin/;
use lib "$Bin/../lib";

print "port = $port, workers = $workers \n" if ( $debug );
if ( $help ) {
	PhalanxStatsHandler::help();
	exit;
}
my $server = Wikia::SimplePreforkServer->new( new PhalanxStatsHandler, '', $port, $workers, $sendTimeout, $recvTimeout );
$server->run;
