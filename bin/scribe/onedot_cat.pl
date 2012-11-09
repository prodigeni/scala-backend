#!/usr/bin/env perl

use common::sense;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use JSON::XS;
use Try::Tiny;
use Getopt::Long;
use Sys::Syslog;
use IO::Select;

use Thrift::BinaryProtocol;
use Thrift::FramedTransportFactory;
use Thrift::Socket;
use Scribe::Thrift::scribe;

# Ping the logs every this many seconds
use constant HEARTBEAT_SECS => 5*60;

# Create a select object to monitor our pipe to varnishlog
our $SELECT = IO::Select->new();

#
# switch off buffering
#
$|++;

my $host   = "localhost";
my $port   = 1463;
my $update = 500000;
my $syslog = 0;
my $debug  = 0;

GetOptions( "host=s"   => \$host,
			"port=i"   => \$port,
			"update=i" => \$update,
			"syslog"   => \$syslog,
			"debug"    => \$debug );

#
# open connection to local scribe
#
my $socket = new Thrift::Socket( $host, $port );
my $transport = new Thrift::FramedTransport( $socket );
my $protocol = new Thrift::BinaryProtocol( $transport );

$socket->setSendTimeout( 60000 );
$socket->setRecvTimeout( 60000 );

openlog( "onedot", "pid,nofatal", "LOG_LOCAL6" ) if $syslog;

my $client = Scribe::Thrift::scribeClient->new( $protocol, $protocol );

my $i = 0;
while ( my $line = read_varnishlog() ) {
	# Show some indications in the log that we are alive and running
	heartbeat();

	next unless $line =~ /__onedot\?(.+)/;

	my $p = $1; # params
	$p =~ s/\&amp;/\&/g;

	my $beacon = undef;
	( $beacon ) = $line =~ /BEACON: (\w+)/;

	my %result;
	my @parts = split( '&', $p );
	for my $p (@parts) {
		my ($key, $value) = split( "=", $p );

		$result{$key} = $value;
	}

	if( defined $beacon ) {
		$result{ "beacon" } = $beacon;
	}

	next unless ( $result{'c'} =~ /^[+-]?\d+$/ );
	next unless ( $result{'n'} =~ /^[+-]?\d+$/ );
	next unless ( $result{'a'} =~ /^[+-]?\d+$/ );
	next unless ( $result{'u'} =~ /^[+-]?\d+$/ );

	my @ts = localtime();
	$result{ "lv" } = sprintf("%04d-%02d-%02d %02d:%02d:%02d", $ts[5]+1900, $ts[4]+1, @ts[3,2,1,0]);

	my %params = ( 'method' => ( $i > 0 && $i % $update == 0 ) ? 'move' : 'collect', 'params' => \%result );
	my $json = encode_json \%params;

	my $entry = Scribe::Thrift::LogEntry->new( { category => "log_view", message => $json });
	try {
		$transport->open() unless $transport->isOpen();
		if ( $client->Log( [ $entry ] ) == Scribe::Thrift::ResultCode::TRY_LATER )  {
			debug("skipping...");
		} else {
			debug("processed $json and send to $host:$port");
			logmsg("processed $update and send to $host:$port") if $params{method} eq "move";
		}
		$i++;
	}
	catch {
		debug($_->{"message"});
	};
}

$transport->close();
closelog() if $syslog;

################################################################################

our $BEATTIME = time;
sub heartbeat {
	return if (time-$BEATTIME) < HEARTBEAT_SECS();
	logmsg("onedot_cat.pl running ... ");
	$BEATTIME = time;
}

sub debug {
	my ($msg) = @_;
	return unless $debug;
	logmsg($msg);
}

sub logmsg {
	my ($msg) = @_;
	if ($syslog){
		syslog("info", $msg);
	} else {
		print STDERR $msg."\n";
	}
}

sub read_varnishlog {
	my ($is_reopen) = @_;
	my ($pipe) = $SELECT->can_read(5);

	# Return the line imediately if we have a line
	if ($pipe) {
		my $data;
		eval {
			local $SIG{ALRM} = sub { die "timeout\n" };

			alarm 5;
			$data = scalar(<$pipe>);
			alarm 0;
		};
		if (my $err = $@) {
			die "Read error: $err\n" unless $err eq "timeout\n";
		} else {
			return $data;
		}
	}

	# Nothing ready after a 5 second delay...too long.  Close out and reopen
	foreach my $fh ($SELECT->handles()) {
		close($fh);
	}

	# If we've already tried to reopen this sleep for a bit
	if ($is_reopen) {
		logmsg("NOTE: Reopening pipe failed to return data.  Sleeping 30 and trying again ... ");
		sleep(30);
	}

	logmsg("Reopening pipe to varnishlog");

	open(my $fh, "/usr/bin/varnishlog -c -I 'BEACON:' | ")
		or die "Can't open pipe to varnishlog: $!\n";
	$SELECT->add($fh);
	return read_varnishlog(1);
}
