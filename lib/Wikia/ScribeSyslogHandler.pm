package Wikia::ScribeSyslogHandler;

use strict;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use Sys::Syslog;
use Scribe::Thrift::scribe;
use Wikia::Utils;
use URI::Escape;

# Map these event keys to normalized key names
our %KEY_MAP = (
	cityId        => 'c',
	pageId        => 'a',
	beaconId      => 'beacon',
	logId         => 'log',
	revId         => 'rev',
	serverName    => 'url',
	hostname      => 'server',
	userIsBot     => 'bot',
	pageTitle     => 'title',
	isContent     => 'content',
	userIp        => 'ip',
	languageId    => 'lid',
	pageNamespace => 'n',
	userId        => 'u',
	isRedirect    => 'redirect',
	revTimestamp  => 'rev_ts',
);

# Don't buffer output from this script
$| = 1;

sub new {
	my $class = shift;
    my (@args) = @_;
    my $self = bless {}, ref $class || $class;

	openlog('scribesyslog','ndelay','local0');

	return $self;
}

DESTROY {
	# This assumes there's only ever one instance of this class
	closelog();
}

sub Log {
	my $self = shift;
	my ($messages) = @_;

	# check time
	my $process_start_time = time();
	
	# Count categories we process
	my %sc_keys;

	# Don't go any further if this has an empty payload
	return Scribe::Thrift::ResultCode::OK
		unless $messages && ref $messages;

	print "Received ".scalar(@$messages)." message(s)\n"; 

	# Iterate through each message
	foreach my $m (@$messages) {
		# Get the category and the content
		my $s_key = $m->{category};
		my $s_msg = $m->{message};

		# Remove the 'log_' at the beginning of these messages
		$s_key =~ s!^log_!!;

		$sc_keys{$s_key} ||= 0;
		$sc_keys{$s_key}++;

		# Decode message
		my $mstruct = Wikia::Utils->json_decode($s_msg);

		my @pairs;

		# Add the rest of the key value pairs
		foreach my $k (keys %$mstruct) {
			if ($k eq 'categoryId' && $mstruct->{$k}) {
				my $c_id   = $mstruct->{$k}->{cat_id};
				my $c_name = $mstruct->{$k}->{cat_name};
				push @pairs, "cat=$c_id";
				push @pairs, 'catname='.uri_escape_utf8($c_name);
			} else {
				my $new_key = $KEY_MAP{$k} || $k;
				push @pairs, $new_key.'='.uri_escape_utf8($mstruct->{$k});
			}
		}

		my $smess = "/__track/event/$s_key?" . join '&', @pairs;

		eval { syslog('info', $smess) };
		if (my $err = $@) {
			print STDERR "Error writing to syslog: $err\n";
			return Scribe::Thrift::ResultCode::TRY_LATER;
		}
	}

	my $process_end_time = time();
	my @ts = gmtime($process_end_time - $process_start_time);
	
	my $msg = join ',',
			  map { $_.': '.$sc_keys{$_} }
			  sort keys %sc_keys;

	print "Categories processed: $msg\n";
	printf ("Time elapsed: %d hours %d minutes %d seconds\n", @ts[2,1,0]);
		
	return Scribe::Thrift::ResultCode::OK;
}

1;
