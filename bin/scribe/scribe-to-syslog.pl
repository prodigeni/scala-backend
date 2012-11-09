#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use Wikia::ScribeSyslogHandler;
use Wikia::SimplePreforkServer;
use Getopt::Long;

my ($help, $recvTimeout, $sendTimeout);
my $port    = 9090;
my $workers = 1;
my $debug   = 0;

GetOptions(
	'port=s' 	=> \$port, 
	'workers=s'	=> \$workers,
	'recvTimeout=s'	=> \$recvTimeout,
	'sendTimeout=s'	=> \$sendTimeout,
	'debug'		=> \$debug,
	'help'		=> \$help
);

print "port = $port, workers = $workers \n" if $debug;
if ($help) {
	help();
	exit;
}

my $processor = Wikia::ScribeSyslogHandler->new(debug => $debug);
my $server = Wikia::SimplePreforkServer->new($processor, '', $port, $workers, $sendTimeout, $recvTimeout);
$server->run;

sub help {
    my ($self) = @_;
    my $name = __FILE__; 
    print <<EOF
$name [--help] [--listen_mq] [--daemon]
	port=9090     => default port 9090,
	workers=s     => number of server instances (default 1),
	recvTimeout=s => default 10000 ms
	sendTimeout=s => default 10000 ms
	debug         => debug enabled
EOF
;
}
