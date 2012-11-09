#!/usr/bin/perl -w

use strict;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use Getopt::Long;
use Wikia::SimplePreforkServer;
use IO::Socket::INET;

our $STREAM_HOST = 'purge.fastly.net';
our $STREAM_PORT = '5012';
our $STREAM_SOCKET;

my $port = 9090;
my $workers = 1;
my $debug = 0;
my ($help, $recvTimeout, $sendTimeout);
my ($varnish_server, $varnish_port) = ('127.0.0.1', '80');
my $quiet = 0;

GetOptions(
	# Scribe options
	'port|p=s'         => \$port, 
	'workers|w=s'      => \$workers,
	'recvTimeout|rt=s' => \$recvTimeout,
	'sendTimeout|st=s' => \$sendTimeout,

	# Varnish options
	'varnish-server=s' => \$varnish_server,
	'varnish-port=s'   => \$varnish_port,

	# General options
	'quiet|q'          => \$quiet,
	'debug|d'          => \$debug,
	'help|h'           => \$help
);

if ($help) {
	help();
	exit;
}

print "Listening on port $port with $workers worker(s)\n" if $debug;

my $purger = PurgeHandler->new({debug          => $debug,
								quiet          => $quiet,
								varnish_server => $varnish_server,
								varnish_port   => $varnish_port});
my $server = Wikia::SimplePreforkServer->new($purger,
											'',
											$port,
											$workers,
											$sendTimeout,
											$recvTimeout);
$server->run;

sub help {
    my $name = $0;
 	$name =~ s!^.*/!!;

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

###############################################################################

package PurgeHandler;

use JSON::XS;
use LWP::UserAgent;
use Time::HiRes;

# Log any purges longer than 10ms
use constant PURGE_LENGTH_TRIGGER => 0.01;

sub new {
	my $class = shift;
	my ($param) = @_;
	my $self = bless {}, (ref $class || $class); 

	$self->{varnish_server} = $param->{varnish_server};
	$self->{varnish_port}   = $param->{varnish_port};
	$self->{debug}          = $param->{debug};
	$self->{quiet}          = $param->{quiet};
	
	my $ua = LWP::UserAgent->new(keep_alive => 1);
	$ua->agent("HTCP Purger");
	$ua->default_header('Accept-Encoding' => 'NOTVALID');
	$ua->proxy('http', 'http://'.$self->{varnish_server}.':'.$self->{varnish_port}.'/');
	$self->{user_agent} = $ua;
	
	return $self;
}

sub Log {
	my $self = shift;
	my ($messages) = @_;
	my $ok = 1;
	
	# check time
	my $process_start_time = [Time::HiRes::gettimeofday];

	my $oldest_purge = time;
	my ($total, $processed) = (0, 0);
	my %dups;
	if ($messages && ref($messages)) {

		# Process messages from Scribe
		foreach my $mesg (@$messages) {
			my $key      = $mesg->{category};
			my $msg_data = $mesg->{message};

			print sprintf("\t%s: %s\n", $key, $msg_data) if $debug;

			# decode message
			my $data = decode_json($msg_data);
						
			# Skip this message if there's no data
			next unless $data && ref($data);
			my $url = $data->{url};

			# Skip this message if we've already purged it in this set of messages
			next if $dups{$url};
			$dups{$url} = 1;

			# Purge from varnish
			$self->purge_url($url);
			$oldest_purge = $data->{time} if $oldest_purge > $data->{time};

			$processed++;
		}
	}

	my $elapsed = Time::HiRes::tv_interval($process_start_time, [Time::HiRes::gettimeofday]);
	my $secs  = $elapsed % 60 + ($elapsed-int($elapsed));
	my $mins  = int($elapsed / 60) % 60;
	my $hours = int($elapsed / 60 / 60);

	elapsed_monitor($elapsed);
	processed_monitor($processed);
	printf("%d messages processed in %d hours %d minutes %.2f seconds\n", $processed, $hours, $mins, $secs);
	
	my $age = time - $oldest_purge;
	$secs  = $age % 60;
	$mins  = int($age / 60) % 60;
	$hours = int($age / 60 / 60);
	
	age_monitor($age);
	printf("\tEarliest purge request was %d hours %d minutes %d seconds old\n", $hours, $mins, $secs);

	print "ok = $ok\n" if $debug;
	return $ok ? Scribe::Thrift::ResultCode->OK : Scribe::Thrift::ResultCode->TRY_LATER;
}

sub get_stream {
	unless ($STREAM_SOCKET) {
		my $addr = $STREAM_HOST.':'.$STREAM_PORT;
		$STREAM_SOCKET = IO::Socket::INET->new(PeerAddr => $addr,
											   Proto    => 'tcp')
			or die "Can't open socket to '$addr': $!\n";

		$STREAM_SOCKET->print("POST / HTTP/1.1\r\n");
		$STREAM_SOCKET->print("Content-Type: text/html\r\n");
		$STREAM_SOCKET->print("Transfer-Encoding: chunked\r\n\r\n");
	}
	return $STREAM_SOCKET;
}

sub stream_purge {
	my $self = shift;
	my ($url) = @_;

	my $stream = get_stream();

	$stream->printf("%x\r\n", length($url));
	$stream->print("$url\r\n") or die "Purge of '$url' failed\n";
}

sub single_purge {
	my $self = shift;
	my ($url) = @_;
	my $ua = $self->{user_agent};

	my $start_purge = [Time::HiRes::gettimeofday];

	my $req = HTTP::Request->new(PURGE => $url);
	my $res = $ua->request($req);
	
	my $delta = Time::HiRes::tv_interval($start_purge, [Time::HiRes::gettimeofday]);
	if ($delta > PURGE_LENGTH_TRIGGER()) {
		print "Warning: Long purge: $delta s: $url\n";
		purge_monitor($delta);
	} elsif ((time % 60) == 0) {
		# When nothing interesting is going on output a sampled $delta every 5m
		# This will help ganglia make a reasonable graph since with very few data
		# points it will try to average out the last long $delta in weird ways
		purge_monitor(0);
	}
	
	if ($res->is_success()) {
    	warn "Purged $url\n" if $self->{debug};
	} else {
		warn "Purging $url failed: @{[$res->status_line]}\n" unless $self->{quiet};
	}
}

sub purge_url {
	my $self = shift;
	my ($url) = @_;

	if ($url !~ m!^http://!) {
		warn "Ignoring URL not beginning with http://: $url\n";
		return;
	}

	print "Purging URL '$url'\n" if $debug;
	
	$0 = gmtime() . " -  " . __PACKAGE__ . " worker - purging $url";
	
	# Do the one of purging we've already been doing
	$self->single_purge($url);

	# Do the streaming purging via Fastly
	$self->stream_purge($url);

	$0 = gmtime() . " -  " . __PACKAGE__ . " worker - done purging $url";
}

sub elapsed_monitor {
	my ($val) = @_;

	update_ganglia(name  => 'scribe-purge-elapsed',
				   value => $val,
				   type  => 'float',
				   units => 'seconds',
				   slope => 'both');
}

sub age_monitor {
	my ($val) = @_;
	
	update_ganglia(name  => 'scribe-purge-oldest',
				   value => $val,
				   type  => 'uint32',
				   units => 'seconds',
				   slope => 'both');
}

sub processed_monitor {
	my ($val) = @_;
	
	update_ganglia(name  => 'scribe-purge-processed',
				   value => $val,
				   type  => 'uint32',
				   units => 'URLs',
				   slope => 'both');
}

sub purge_monitor {
	my ($val) = @_;
	
	update_ganglia(name  => 'scribe-purge-long',
				   value => $val,
				   type  => 'uint32',
				   units => 'seconds',
				   slope => 'both');
}

sub update_ganglia {
	my (%param) = @_;

	my $cmd  = 'gmetric';
	my @args = map { '--'.$_.'='.$param{$_} } keys %param;
	if (system($cmd, @args) == 0) {
		# Success
	} else {
		warn "Problem reporting to ganglia\n";
	}
}

1;
