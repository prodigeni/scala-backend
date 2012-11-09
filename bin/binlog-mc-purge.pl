#!/usr/bin/perl -w

# Script to purge memcached

use strict;

use FindBin qw( $Bin );
use lib "$Bin/../lib";

use Cache::Memcached::libmemcached;
use Digest::MD5;
use DBI;
use Getopt::Long;

use Wikia::LB;
use Wikia::DB;
use Wikia::Config;
use Wikia::BinLog;
use Wikia::Settings;

use constant WAIT_TIME => 5;

our $MC;

our $LOG_DIR = '/var/lib/mysql/logs';
our $LOG_INDEX;
our $LOG_FILE;
our $LOG_POSITION;

our $DB_NAME = 'wikia_mc_purge';
our $DB_TABLE = 'memcached';
our $VERBOSE;
our $TEST;
our $SERVERS;

our ($wait);
GetOptions('wait-time|t=s' => \$wait,
           'servers=s'     => \$SERVERS,
           'log-dir=s'     => \$LOG_DIR,
           'log-index=s'   => \$LOG_INDEX,
           'log-file=s'    => \$LOG_FILE,
           'position|p=s'  => \$LOG_POSITION,
           'db-name=s'     => \$DB_NAME,
           'verbose|v'     => \$VERBOSE,
           'test'          => \$TEST,
);

# Make sure we have a log dir to find the current binlog
die "Can't find log dir '$LOG_DIR'\n" unless -e $LOG_DIR;

# If they passed a log index file, make sure it exists
die "Can't find log index '$LOG_INDEX'\n" if $LOG_INDEX and not -e "$LOG_DIR/$LOG_INDEX";

$wait ||= WAIT_TIME();

my ($file, $last_file, $pos, $last_pos) = ('', '', 0, 0);
my $finish_file = 0;

while (1) {
	($file, $pos) = cur_binlog_start();
	
	# No file, wait for a file and try again
	if (!$file) {
		print "No file found, waiting $wait seconds before trying again ...\n" if $VERBOSE;
		sleep($wait);
		next;
	}
	
	# File and position the same as last time
	if (($file eq $last_file) && ($pos eq $last_pos)) {
		# If $finish_file is set it means we were draining this file, let the
		# following conditional grab this, otherwise wait for more data and try again
		unless ($finish_file) {
			print "No changes found, waiting $wait seconds before trying again ...\n" if $VERBOSE;
			sleep($wait);
			next;
		}
	}
	
	# If file is different from last time ...
	if ($last_file and ($file ne $last_file)) {
		# If $finish_file is set, we just finished reading the remaining lines of the
		# previous file, go on to the new one, starting at the beginning
		if ($finish_file) {
			$finish_file = 0;
			$pos = 0;
		}
		# Otherwise set $finish_file and read the rest of this file
		else {
			$finish_file = 1;
			$file = $last_file;
			$pos  = $last_pos;
		}
	}

	# Finally if we had a last file and its the same one, continue where we left off
	if ($last_file and ($file eq $last_file)) {
		$pos = $last_pos;
	}

	print "Reading '$file' @ $pos\n" if $VERBOSE;

	my $num_processed;
	($num_processed, $last_pos) = process_binlog_file($file, $pos);
	$last_file = $file;

	print "Ending position $last_pos: " if $VERBOSE;

	if ($num_processed) {
		print "processed $num_processed purge(s)\n" if $VERBOSE;
		# We've run out, give a little bit of time for more transactions to come in
		# but not as much as $wait time.
		sleep(1);
	} else {
		# If we didn't process anything wait and try again in a bit
		print "found no purges, waiting $wait seconds\n" if $VERBOSE;
		sleep($wait);
	}
}

###############################################################################

# This is the hashing function used by the app.
sub wikia_hash_func { hex(substr(Digest::MD5::md5_hex($_[0]), 0, 8)) & 0x7fffffff }

sub mc_client {
	unless ($MC) {
		my $servers;
		if ($SERVERS) {
			$SERVERS =~ s/\s+//g;
			$servers = [split(',', $SERVERS)];
		} else {
			my $ws = Wikia::Settings->instance;
			$servers = $ws->variables->{wgMemCachedServers};
		}

		$MC = Cache::Memcached::libmemcached->new( {servers => $servers} );
		die "Can't create memcached client\n" unless $MC;
	}

	return $MC;
}

sub cur_binlog_start {

=pod

# This will be useful if we read the master status.  Currently doesn't seem to be a way to get connection info for the iowa 'master' slaves.

	my $lb = Wikia::LB->instance;
	my $dbh = $lb->getConnection( Wikia::LB::DB_MASTER, undef, $CLUSTERS[$CLUSTER-1] );

	my $ret = $dbh->selectall_arrayref('show master status');

	my ($file, $pos) = $ret ? @{$ret->[0]} : ('', 0);

	return ($file, $pos);
	
=cut

	my $log_file = cur_log_file();
	my $position = 0;
	
	# If we got a log position, use it the first time
	if ($LOG_POSITION) {
		$position = $LOG_POSITION;
		undef $LOG_POSITION;
	}
	
	return ($log_file, $position);
}

sub process_binlog_file {
	my ($file, $pos) = @_;

	my $mc = mc_client();
	my $binlog = Wikia::BinLog->new(database => $DB_NAME,
									file     => $file,
									start    => $pos,
                    	           );

	# Show some status if --verbose is set
	show_record(1);
	           
	my $processed = 0;
	while (my $rec = $binlog->next_record) {
		# The records could be a few lines long
		foreach my $stmt (@$rec) {
			if ($stmt =~ /insert.+into.+$DB_TABLE.+values.+\('([^']+)'\)/i) {
				my $key = $1;
				
				print "\nPurging key '$key'\n" if $VERBOSE;
				$mc->delete([wikia_hash_func($key), $key]) unless $TEST;
				$processed++;
			}		
		}

		# Show status if with --verbose
		show_record();
	}

	# End the status from show_record() with a new line if --verbose was given
	print "\n" if $VERBOSE;

	return ($processed, $binlog->get_position);
}

sub cur_log_file {
	# If there's a log file set from the commandline, pass it back the first time
	if ($LOG_FILE) {
		my $log_file = $LOG_FILE =~ m!^/! ? $LOG_FILE : $LOG_DIR.'/'.$LOG_FILE;
		undef $LOG_FILE;
		return $log_file;
	}

	unless ($LOG_INDEX) {
		opendir(DIR, $LOG_DIR) or die "Can't open log directory '$LOG_DIR': $!\n";
		while (my $line = readdir(DIR)) {
			# Just want the index files
			next unless $line =~ /\.index$/;
			# Don't want the relay index
			next if $line =~ /relay/;

			# Take the first file we find that matches; should be what we want
			$LOG_INDEX = $line;
			last;
		}
		closedir(DIR);
	}
	
	my $log_file;
	my $index_file = $LOG_DIR.'/'.$LOG_INDEX;
	
	# Run through the file and just take the last line.  Feel like there's a better
	# way but this file isn't too long ever.
	open(INDEX, $index_file) or die "Can't open index file '$index_file': $!\n";
	while (my $line = <INDEX>) {
		chomp($log_file = $line);
	}
	close(INDEX);

	# Add the log dir if the file isn't absolute
	$log_file = $LOG_DIR.'/'.$log_file if $log_file !~ m!^/!;

	die "Log file $log_file doesn't exist\n" unless -e $log_file;
	
	return $log_file;
}

our $REC;
sub show_record {
	my ($clear) = @_;
	return unless $VERBOSE;
	
	local $|=1;
	
	if ($clear) {
		$REC = 1;
		print "Processing record $REC";
		return;
	}
	
	print "\cH" x length($REC);
	$REC++;
	print $REC;
}
