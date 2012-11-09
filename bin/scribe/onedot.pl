#!/usr/bin/perl
package main;

use common::sense;
use Data::Dumper;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use Wikia::SimplePreforkServer;
use Wikia::Utils qw( note );
use Wikia::LB;
use Wikia::DB;
use Wikia::Settings;
use Wikia::Onedot;

use Switch;
use Getopt::Long;
use Data::Dumper;
use base qw/Class::Accessor::Fast/;

$|++;
my $port = 9099;
my $workers = 1;
my $debug = 0;
my $raw_logging = 0;
my $insert = 150;
my $option = undef;
my $file = '';
my $table = '';
my ($help, $recvTimeout, $sendTimeout) = ();
GetOptions(
	'port=s' 		=> \$port, 
	'workers=s' 	=> \$workers,
	'recvTimeout=s' => \$recvTimeout,
	'sendTimeout=s' => \$sendTimeout,
	'insert=s' 		=> \$insert,
	'debug'			=> \$debug,
	'raw-logging'   => \$raw_logging,
	'option=s'		=> \$option,
	'file=s'			=> \$file,
	'help'			=> \$help,
	'table=s'		=> \$table
);

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
	option			=> null (run as server (default)) or move (to move data from local DB to mysql)
	file 			=> db_file to move
EOF
;
}

note "port = $port, workers = $workers" if ( $debug );
if ( $help ) {
	&help();
	exit;
}

=test
if ( $insert == 1 ) { 
	for ( my $i = 0; $i < 1000; $i++ ) {
		my $params = {
			'method' => 'collect',
			'params' => {
				'r' => "",
				'a' => int(rand($i)),
				'x' => 'wikicities',
				'lv' => "2010-11-22 12:21:57",
				"cb" => "1290428517215",
				"y" => "",
				"u" => "115748",
				"n" => "0",
				"c" => "177",
				"lc" => "en"
			} 
		};
		my $obj = new Wikia::Onedot( { debug => $debug } );
		my $message = Wikia::Utils->json_encode($params);
		my $messages = [
			{ 'category' => 'log_view', 'message' => $message }
		];
		$obj->Log($messages);
	}
} else {

my $params = {
	'method' => 'move',
	'params' => {
	} 
};
my $obj = new Wikia::Onedot( { debug => $debug } );
my $message = Wikia::Utils->json_encode($params);
my $messages = [
	{ 'category' => 'log_view', 'message' => $message }
];
$obj->Log($messages);
}
=cut

if ( $option eq 'move' ) {
	my $response = 0;
	if ( $table eq 'wikia' ) {
		my $onedot = new Wikia::Onedot( { debug => $debug, insert => $insert, wikia_db_file => $file } );
		$response = $onedot->_read_wikia();
	} elsif ( $table eq 'pages' ) {
		my $onedot = new Wikia::Onedot( { debug => $debug, insert => $insert, page_db_file => $file } );
		$response = $onedot->_read_articles();
	} elsif ( $table eq 'users' ) {
		my $onedot = new Wikia::Onedot( { debug => $debug, insert => $insert, user_db_file => $file } );
		$response = $onedot->_read_users();
	} elsif ( $table eq 'namespaces' ) {
		my $onedot = new Wikia::Onedot( { debug => $debug, insert => $insert, ns_db_file => $file } );
		$response = $onedot->_read_namespaces();
	} elsif ( $table eq 'tags' ) {
		my $onedot = new Wikia::Onedot( { debug => $debug, insert => $insert, tags_db_file => $file } );		
		$response = $onedot->_read_tags();
	} elsif ( $table eq 'weekly_users' ) {
		my $onedot = new Wikia::Onedot( { debug => $debug, insert => $insert, weekly_user_db_file => $file } );
		$response = $onedot->_read_weekly_users();
	} if ( $table eq 'weekly_wikia' ) {
		my $onedot = new Wikia::Onedot( { debug => $debug, insert => $insert, weekly_wikia_db_file => $file } );
		$response = $onedot->_read_weekly_wikia();
	}
} else {
	my $server = Wikia::SimplePreforkServer->new( new Wikia::Onedot( { debug => $debug, raw_logging => $raw_logging, insert => $insert, limit => 30_000 } ), '', $port, $workers, $sendTimeout, $recvTimeout );
	$server->run;
}

