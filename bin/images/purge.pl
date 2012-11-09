#!/usr/bin/perl -w

use strict;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";

use Fcntl ":flock";
use Getopt::Long;
use Sys::Hostname;

use Wikia::LB;
use Wikia::FileServerInfo;

my ($limit, $rate, $wait, $ttl, $verbose, $host, $show_files);
GetOptions('host=s'              => \$host,
		   'limit|l=s'           => \$limit,
		   'show-files'          => \$show_files,
		   'rate-limit|rate|r=s' => \$rate,
		   'wait|w=s'            => \$wait,
		   'ttl=s'               => \$ttl,
		   'verbose|v'           => \$verbose,
		   'help|?'              => sub { help() and exit(0) },
		  ) or (help() and exit(1));

print STDERR "Purge starting.\n";

my $info      = Wikia::FileServerInfo->by_name($host || hostname());
my $image_dir = $info->image_dir;

# By default wait 60 seconds when there were no files replicated on the last round
$wait  ||= 60;
$limit ||= 200;

my $max_id = max_id();
my $start_id = 0;
my $start = time;
while ($start_id <= $max_id) {
	last if $ttl && (time - $start) > $ttl;

	my $cnt = check_files($image_dir, $start_id, $start_id + $limit, $show_files);
	$start_id += $limit;

	if ($rate) {
		# If we have a rate limit, do it now.
		print "Rate limiting ${rate}s\n" if $verbose;
		sleep($rate);
	}
}

print STDERR "Purge ended.\n";

################################################################################

sub check_files {
	my ($image_dir, $start_id, $end_id, $show_files) = @_;
	my $dbh_r = Wikia::LB->instance->getConnection( Wikia::LB->DB_SLAVE, undef, Wikia::LB->DATAWARESHARED );
	my $sth = $dbh_r->prepare(qq{
SELECT up_id, up_path
FROM upload_log
WHERE up_flags = -1
  AND up_id >= ? and up_id < ?
});
	$sth->execute($start_id, $end_id);
	my $files = $sth->fetchall_arrayref;
	$sth->finish;

	return unless $files and @$files;

	$|=1;
	print "Checking ".scalar(@$files)." files ";

	my $n = 0;
	my @clear_flag;
	foreach my $f (@$files) {
		$f->[1] =~ s!^/images/!!;
		$f->[1] =~ s!^(([^/])[^/]+)!$image_dir/$2/$1!;
		
		if (-e $f->[1]) {
			print "x";
			push @clear_flag, $f;
		} else {
			print "." if $n++ % 10 == 0;
		}
	}
	print "\n";

	if (@clear_flag) {
		if ($show_files) {
			print "Found files marked as deleted:\n";
			foreach my $f (@clear_flag) {
				printf("% 8d: %s\n", $f->[0], $f->[1]);
			}
		}
	
		my $in_clause = join(', ', ('?') x @clear_flag);
		my $dbh_w = Wikia::LB->instance->getConnection( Wikia::LB->DB_MASTER, undef, Wikia::LB->DATAWARESHARED );
		my $sth = $dbh_w->prepare(qq{
UPDATE upload_log
SET up_flags = 0
WHERE up_id in ($in_clause)
});
		$sth->execute(map { $_->[0] } @clear_flag);
	}

	return scalar(@$files);
}

sub max_id {
	my $dbh_r = Wikia::LB->instance->getConnection( Wikia::LB->DB_SLAVE, undef, Wikia::LB->DATAWARESHARED );
	my $sth = $dbh_r->prepare(qq{
SELECT max(up_id)
FROM upload_log
});
	$sth->execute();
	my $res = $sth->fetchall_arrayref;
	$sth->finish;
	
	return $res->[0]->[0];
}
