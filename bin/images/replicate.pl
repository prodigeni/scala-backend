#!/usr/bin/perl -w

=pod

=head NAME

replicate.pl

=head SYNOPSIS

Maintain copies of every uploaded image on all slave files servers.  The upload_log table is used to determine which files need to be replicated and tells where they have been copied to.

=cut

use strict;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";

use Fcntl ":flock";
use Getopt::Long;
use Sys::Hostname;

use Wikia::LB;
use IO::Socket;

use constant IMAGE_DIR => "/raid/images/by_id";
use constant HOST_MASK => {'file-i4' => 1,
						   'file-s3' => 2,
						   'file-s4' => 4,
						   'file-s5' => 8,
						   'file-i6' => 16,
						  };
use constant MAX_MASK   => 16;

my ($limit, $host, $host_mask, $rate, $wait, $ttl, $verbose, $master, $image_dir);
GetOptions('limit|l=s'           => \$limit,
		   'host|h=s'            => \$host,
		   'host-mask=s'         => \$host_mask,
		   'master|m=s'          => \$master,
		   'image-dir=s'         => \$image_dir,
		   'rate-limit|rate|r=s' => \$rate,
		   'wait|w=s'            => \$wait,
		   'ttl=s'               => \$ttl,
		   'verbose|v'           => \$verbose,
		   'help|?'              => sub { help() and exit(0) },
		  ) or (help() and exit(1));

die "Argument --master required\n" unless $master;

print STDERR "Replication starting.\n";

# By default wait 60 seconds when there were no files replicated on the last round
$wait  ||= 60;
$limit ||= 200;

my $info = {master       => ($master),
			image_dir    => ($image_dir || IMAGE_DIR()),
			hostname     => ($host      || hostname()),
			ip           => IO::Socket::inet_ntoa((gethostbyname($host))[4]),
			bit_mask     => ($host_mask || HOST_MASK->{$host}),
		   };

my $start = time;
while (1) {
	last if $ttl && (time - $start) > $ttl;

	my $cnt = replicate_files($info, $limit);
	if (!$cnt) {
		# If we didn't replicate anything, sleep for a bit
		print STDERR "Nothing to copy, sleeping ${wait}s\n";
		sleep($wait);
	} else {
		# If we have a rate limit, do it now.
		print "Rate limiting ${rate}s\n" if $verbose;
		sleep($rate) if $rate;
	}
}

print STDERR "Replicate exiting.\n";

################################################################################

sub replicate_files {
	my ($info, $limit) = @_;
	$limit ||= 100;

	my $files = grab_files($limit, enumerate_bitmask($info->{bit_mask}));
	return unless $files and @$files;

	print "Grabbed ".scalar(@$files)." files to replicate\n";

	# Reformat image paths from:
	#   /images/foobar/images/8/81/Blah.png
	# to:
	#   f/foobar/images/8/81/Blah.png
	#
	foreach my $f (@$files) {
		$f->[1] =~ s!^/images/!!;
		$f->[1] =~ s!^(([^/])[^/]+)!$2/$1!;
	}
	
	my $file_list = escape_files(map { $_->[1] } @$files);
	my $exist = existing_files($info, $file_list);

	# See if there are any files missing
	my @missing;
	if (scalar keys %$exist < @$files) {
		foreach my $f (@$files) {
			push @missing, $f->[0] unless $exist->{$f->[1]};
		}
		$file_list = escape_files(keys %$exist);
	}

	print "Discovered ".scalar(@missing)." missing files\n";
	return 0 if scalar(@missing) == $limit;

	my $image_dir = $info->{image_dir};
	my $master = $info->{master};
	my $cmd = qq(ssh $master "cd $image_dir; tar -c $file_list" | tar xf -);

	print "Running: $cmd\n" if $verbose;

	chdir($image_dir);

	my $success = 1;
	if (system($cmd) != 0) {
		warn "Failed to transfer images";
		$success = 0;
	}

	release_lock($success, \@missing, $info->{bit_mask});
	return $success ? scalar($files) : 0;
}

sub grab_files {
	my ($limit, $flag_values) = @_;

	push @$flag_values, -1;
	my $in_clause = '('.join(', ', ('?') x @$flag_values).')';

	my $dbh_w = Wikia::LB->instance->getConnection( Wikia::LB->DB_MASTER, undef, Wikia::LB->DATAWARESHARED );
	my $sth = $dbh_w->prepare(qq{
UPDATE upload_log
SET up_repl_lock = NOW(), up_repl_pid = ?
WHERE up_repl_lock IS NULL
  AND up_flags not in $in_clause
LIMIT ?
});
	my $count = $sth->execute($$, @$flag_values, $limit);

	if ($count and $count ne '0E0') {
		my $dbh_w = Wikia::LB->instance->getConnection( Wikia::LB->DB_MASTER, undef, Wikia::LB->DATAWARESHARED );
		my $sth = $dbh_w->prepare(qq{
SELECT up_id, up_path
FROM upload_log
WHERE up_repl_pid = ?
});
		$sth->execute($$);
		return $sth->fetchall_arrayref;
	} else {
		return;
	}
}

sub release_lock {
	my ($success, $missing, $mask) = @_;

	my $dbh_w = Wikia::LB->instance->getConnection( Wikia::LB->DB_MASTER, undef, Wikia::LB->DATAWARESHARED );

	# If there were missing images, mark them here
	if ($missing and @$missing) {
		my $in_clause = join(',', ('?') x scalar(@$missing));

		my $sth = $dbh_w->prepare(qq{
UPDATE upload_log
SET up_repl_lock = NULL, up_repl_pid = NULL, up_flags = -1
WHERE up_id in ($in_clause)
});

		$sth->execute(@$missing);
	}

	my $bit_set = $success ? ", up_flags = up_flags|$mask" : '';

	my $sth = $dbh_w->prepare(qq{
UPDATE upload_log
SET up_repl_lock = NULL, up_repl_pid = NULL, up_sent = NOW() $bit_set
WHERE up_repl_pid = ?
  AND up_flags != -1
});

	my $released = $sth->execute($$);
	return $released eq '0E0' ? 0 : $released;
}

# Rather than do a logical AND in the DB to find which have our bit set, enumerate
# all integers up to the max bitmask value that contain our bit
sub enumerate_bitmask {
	my ($bitmask) = @_;
	my $max_val = MAX_MASK() << 1;
	
	my @enum = ($bitmask);
	my $v = $bitmask + 1;
	while ($v < $max_val) {
		push @enum, $v if ($v & $bitmask);
		$v++;
	}
	return \@enum;
}

sub escape_files {
	my (@files) = @_;


# @_ =~ s/([;<>\*\|`&\$!#\(\)\[\]\{\}:'"])/\\$1/g;
# return @_;

	my $file_list = join ' ',
					map { s/([;<>\*\|`&\$!#\(\)\[\]\{\}:'"])/\\$1/g; $_ }
					@files;

	return $file_list;
}

sub existing_files {
	my ($info, $files) = @_;
	my $image_dir = $info->{image_dir};
	my $master    = $info->{master};
	
	# Some files don't exist on the master.  Make sure we know what exists.
	my $cmd = qq(ssh $master "cd $image_dir; ls $files 2>/dev/null");
	open(PIPE, "$cmd|") or die "Can't connect to $master: $!\n";
	my %exist;
	while (my $line = <PIPE>) {
		chomp($line);
		$exist{$line} = 1;
	}
	close(PIPE);

	return \%exist;
}

sub help {
	my ($prog) = $0 =~ m!([^/]+)$!;
	
	print qq(
NAME
	$prog

SYNOPSIS
	$prog [--host HOST] [--host-mask BIT_MASK] [--master MASTER] [--image-dir DIR] [--limit NUM] [--rate-limit SECS] [--wait SECS] [--ttl SECS] [--verbose] [--test]

DESCRIPTION
	Read newly uploaded image paths from the upload_log table and copy these images from the master fileserver to the server this script is running on.

OPTIONS
	--host, -h HOST
		Set the name of the host running this script.  Useful to masquerade as another host or if the current host has a different hostname than what's in DNS.  Default is result of hostname()

	--host-mask BIT_MASK
		Set the bit mask to use for this host.  This bit mask is used in the upload_log table to determine which files this host has copies of

	--master, -m MASTER
		Set the name/ip of the master file host.  Default is to get this information from chef via a knife query.

	--image-dir DIR
		The base directory where images can be found.

	--limit, -l NUM
		Set the maximum number of images to copy at one time.  Default is 200.

	--rate-limit, --rate, -r SECS
		Set a limit to how often we update implemented as a pause after each copy of --limit images.  Default is 0

	--wait, -w SECS
		If the last run found no images to copy, wait SECS seconds before trying again.  Default is 60

	--ttl SECS
		Exit after this many seconds.  Default is unset (run forever)

	--verbose, -v
		Display verbose output

	--help, -h
		Display this help message
);	
}