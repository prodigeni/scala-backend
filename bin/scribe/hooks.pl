#!/usr/bin/perl
package main;

use common::sense;
use Data::Dumper;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

my $YML = undef;
BEGIN {
	$YML = "$Bin/../../../wikia-conf/DB.localhost.yml" if ($Bin =~ /moli/) ;
}

use Wikia::SimplePreforkServer;
use Wikia::Hooks;
use Wikia::Utils;
use Wikia::LB;
use Wikia::DB;
use Wikia::Settings;

use Switch;
use Getopt::Long;
use Data::Dumper;
use base qw/Class::Accessor::Fast/;

$|++;
my $port = 9099;
my $workers = 1;
my $debug = 0;
my $insert = 66;
my $threads = 10;
my ($help, $recvTimeout, $sendTimeout) = ();
GetOptions(
	'port=s' 		=> \$port, 
	'workers=s' 	=> \$workers,
	'recvTimeout=s' => \$recvTimeout,
	'sendTimeout=s' => \$sendTimeout,
	'insert=s' 		=> \$insert,
	'debug'			=> \$debug,
	'help'			=> \$help,
	'threads=i' => \$threads
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
EOF
;
}

print "port = $port, workers = $workers \n" if ( $debug );
if ( $help ) {
	&help();
	exit;
}

=test
my $params = {
	'method' => 'login',
	'params' => {
		'user_id' => 1,
		'city_id' => 177,
		'from'	 => 1
	}
};
my $obj = new Wikia::Hooks( { debug => $debug } );
my $message = Wikia::Utils->json_encode($params);
my $messages = [
	{ 'category' => 'log_hook', 'message' => $message }
];
$obj->Log($messages);

=test
my $params = {
	"user_id"          => 1031,
	"user_name"        => 'Jaffa',
	"user_real_name"   => '',
	"user_password"    => '24917510fd28ebb155a4a6f84cfabfad',
	"user_newpassword" => '',
	"user_email"       => '',
	"user_options"     => '',
	"user_touched"     => '20101103145510',
	"user_token"       => '50353984481621296197459843988197'
};
my $obj = new Wikia::Hooks( { debug => $debug } );
my $message = Wikia::Utils->json_encode($params);
my $messages = [
	{ 'category' => 'log_savepreferences', 'message' => $message }
];
$obj->Log($messages);
=cut

my $server = Wikia::SimplePreforkServer->new( new Wikia::Hooks( { debug => $debug, threads => $threads } ), '', $port, $workers, $sendTimeout, $recvTimeout );
$server->run;


