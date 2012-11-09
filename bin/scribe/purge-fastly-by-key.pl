#!/usr/bin/perl -w

use strict;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use Getopt::Long;
use Wikia::SimplePreforkServer;
use IO::Socket::INET;

my $port    = 9091;
my $workers = 1;
my ($recvTimeout, $sendTimeout);

my ($stream_host, $stream_port) = ('purge.fastly.net', 5012);

my ($read_only, $quiet, $debug, $help) = (0, 0, 0);

my @surrkey;

GetOptions(
	# One off options
	'key|k=s@'         => \@surrkey,

	# Scribe options
	'port|p=s'         => \$port,
	'workers|w=s'      => \$workers,
	'recvTimeout|rt=s' => \$recvTimeout,
	'sendTimeout|st=s' => \$sendTimeout,

	# Fastly options
	'purge-host=s'     => \$stream_host,
	'purge-port=s'     => \$stream_port,

	# General options
	'read-only|ro'     => \$read_only,
	'quiet|q'          => \$quiet,
	'debug|d'          => \$debug,
	'help|h'           => sub { help(); exit; }
);

print "Listening on port $port with $workers worker(s)\n" if $debug;

my $purger = PurgeHandler->new({read_only      => $read_only,
								debug          => $debug,
								quiet          => $quiet,
								host           => $stream_host,
								port           => $stream_port});

if (@surrkey) {
	print "\nPurging KEYs from the command line ...\n";
	foreach my $k (@surrkey) {
		print "\tpurging $k\n";
		$purger->purge_key($k);
	}
	exit(0);
}

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
	workers=s		=> number of server instances (default 1),
	recvTimeout=s	=> default 10000 ms
	sendTimeout=s	=> default 10000 ms
	insert=s		=> number of multi inserts (default 66)
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

# Rate limit the number of URLs we purge per second
use constant MAX_PER_SEC => 400;

# Timeout for IO operations (in seconds)
use constant TIMEOUT => 3;

sub new {
	my $class = shift;
	my ($param) = @_;
	my $self = bless {}, (ref $class || $class);

	$self->{read_only} = $param->{read_only};
	$self->{debug}     = $param->{debug};
	$self->{quiet}     = $param->{quiet};
	$self->{host}      = $param->{host};
	$self->{port}      = $param->{port};

	return $self;
}

sub Log {
	my $self = shift;
	my ($messages) = @_;
	my $ok = 1;

	local $| = 1;

	# check time
	my $process_start_time = [Time::HiRes::gettimeofday];

	my $oldest_purge = time;
	my ($total, $processed) = (0, 0);

	my %dups;
	my %num_per_sec;

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
			my $surrkey = $data->{key};

			# Skip this message if we've already purged it in this set of messages
			next if $dups{$surrkey};
			$dups{$surrkey} = 1;

			# Purge from varnish
			$self->purge_key($surrkey);
			$oldest_purge = $data->{time} if $oldest_purge > $data->{time};

			$processed++;

			# Make sure we don't send more than MAX_PER_SEC URLs to purge per second
			my $now = time;
			$num_per_sec{$now}++;
			if ($num_per_sec{$now} > MAX_PER_SEC()) {
				print "At ".MAX_PER_SEC()." messages: rate limiting\n";
				while ($now == time) {}
			}
		}
	}

	my $elapsed = Time::HiRes::tv_interval($process_start_time, [Time::HiRes::gettimeofday]);
	my $secs  = $elapsed % 60 + ($elapsed-int($elapsed));
	my $mins  = int($elapsed / 60) % 60;
	my $hours = int($elapsed / 60 / 60);


	my $prefix = $self->{read_only} ? '[READ-ONLY] ' : '';
	printf("$prefix%d messages processed in %d hours %d minutes %.2f seconds\n", $processed, $hours, $mins, $secs);

	my $age = time - $oldest_purge;
	$secs  = $age % 60;
	$mins  = int($age / 60) % 60;
	$hours = int($age / 60 / 60);

	printf("\tEarliest purge request was %d hours %d minutes %d seconds old\n", $hours, $mins, $secs);

	print "ok = $ok\n" if $debug;
	return $ok ? Scribe::Thrift::ResultCode->OK : Scribe::Thrift::ResultCode->TRY_LATER;
}

sub purge_key {
	my $self = shift;
	my ($key) = @_;

	print "Purging KEY '$key'\n" if $debug;
	#$0 = gmtime() . " -  " . __PACKAGE__ . " worker - purging key $key";

	my $addr = $self->{host}.':'.$self->{port};
	my $stream = IO::Socket::INET->new(PeerAddr => $addr,
									Proto    => 'tcp',
									Timeout  => TIMEOUT())
		or die "Can't open socket to '$addr': $!\n";

	$stream->print("POST /service/5NzYW6HIKNZhcSUjVHUzWP/purge/$key HTTP/1.0\r\n\r\n");

	#$0 = gmtime() . " -  " . __PACKAGE__ . " worker - done purging key $key";


}

1;
