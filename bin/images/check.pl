#!/usr/bin/perl -w

use strict;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";

use Fcntl ":flock";
use Getopt::Long;
use Sys::Hostname;

use Wikia::LB;
use Wikia::FileServerInfo;

my ($limit, $run_as, $test, $host, $rate);
GetOptions('user|u=s'            => \$run_as,
		   'limit|l=s'           => \$limit,
		   'test|t'              => \$test,
		   'host|h'              => \$host,
		   'rate-limit|rate|r=s' => \$rate,
		  );


# By default wait 60 seconds when there were no files replicated on the last round
$limit ||= 200;

my $info = Wikia::FileServerInfo->by_name($host || hostname());

my $last_id = 0;

my $max_id = get_max_id();

print STDERR "Scanning files ";
while ($last_id <= $max_id) {
	my $cnt = check_files($info, $last_id, $limit);

	print STDERR ".";

	# If we have a rate limit, do it now.
	sleep($rate) if $rate;
	
	$last_id += $limit + 1;
}

################################################################################

sub check_files {
	my ($info, $last_id, $limit) = @_;

	my $files = grab_files($last_id, $limit);
	return unless $files and @$files;

	my $pre = "\n";

	foreach my $f (@$files) {
		my ($id, $mask, $path) = @$f;
		next if $mask == -1;

		$path =~ s!^/images/!!;
		$path =~ s!^(([^/])[^/]+)!$2/$1!;
		$path = $info->image_dir.$path;

		# See if the mask for this file claims to include this fileserver
		if ($mask & $info->bit_mask) {
			if (! -e $path) {
				print STDERR "$pre$id on DB but not FS\n";
				$pre = '';
			}
		} else {
			if (-e $path) {
				print STDERR "$pre$id on FS but not DB\n";
				$pre = '';
			}
		}
	}
	
	return scalar @$files;
}

sub grab_files {
	my ($last_id, $limit, $flag_values) = @_;
	
	my $dbh_r = Wikia::LB->instance->getConnection( Wikia::LB->DB_SLAVE, undef, Wikia::LB->DATAWARESHARED );
	my $sth = $dbh_r->prepare(qq{
SELECT up_id, up_flags, up_path
FROM upload_log
WHERE up_id BETWEEN ? AND ?
});

	$sth->execute($last_id, $last_id + $limit);
	return $sth->fetchall_arrayref;
}

sub get_max_id {
		
	my $dbh_r = Wikia::LB->instance->getConnection( Wikia::LB->DB_SLAVE, undef, Wikia::LB->DATAWARESHARED );
	my $sth = $dbh_r->prepare(qq{
SELECT max(up_id)
FROM upload_log
});

	$sth->execute();
	my $row = $sth->fetchrow_arrayref;
	return $row->[0];
}