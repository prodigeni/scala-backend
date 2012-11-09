#!/usr/bin/perl -w

use strict;

use Getopt::Long;

our $S3_URL     = 's3://onedotdata';
our $ONEDOT_DIR = '/tokyo_data/raw';
our $VERBOSE    = 0;
our $TEST       = 0;
our $KEEP_FILES = 10;

GetOptions('verbose|v' => \$VERBOSE,
		   'test|t'    => \$TEST,
		   'keep|k=s'  => \$KEEP_FILES,
		  );

# Test mode assumes verbose
$VERBOSE ||= $TEST;

debug("== TEST MODE ==\n") if $TEST;

my $files = get_files();
exit(0) unless $files;

# Save to s3
archive($files);

# Rename files out of the way
mark_copied($files);

# Cleanup old, copied files
cleanup();

################################################################################

sub debug {
	my ($msg) = @_;
	local $| = 1;
	print "$msg" if $VERBOSE;
}

sub get_files {
	debug("-- Finding files ... ");

	my %files;
	my $found = 0;
	
	opendir(my $dh, $ONEDOT_DIR) or die "Can't open directory '$ONEDOT_DIR': $!\n";
	while (my $file = readdir($dh)) {
		next unless $file =~ /^onedot-(\d{8})\d{6}$/;
		my $date = $1;
	
		$files{$date} ||= [];
		push @{$files{$date}}, $ONEDOT_DIR.'/'.$file;
		$found = 1;
	}
	closedir($dh);

	debug("done\n");

	return unless $found;
	return \%files;
}

sub archive {
	my ($files) = @_;

	foreach my $day (keys %$files) {
		my $day_files = $files->{$day};

		debug("-- Archiving ".(scalar @$day_files)." file(s) for $day ... ");

		my $args = $VERBOSE ? '--progress' : '--no-progress';
		my $cmd = "s3cmd $args put ".join(' ', @$day_files)." $S3_URL/$day/";
		
		if ($TEST) {
			debug("\nTEST: $cmd\n");
		} else {
			system($cmd) == 0 or die "Failed to run s3cmd '$cmd': $?\n";
		}

		debug("done\n");
	}
}

sub mark_copied {
	my ($files) = @_;

	debug("-- Renaming copied files ... ");
	debug("\n") if $TEST;

	foreach my $day (keys %$files) {
		my $day_files = $files->{$day};

		foreach my $file (@$day_files) {
			if ($TEST) {
				debug("TEST: mv $file $file.copied\n");
			} else {
				rename($file, $file.'.copied') or die "Unable to rename '$file': $!\n";
			}
		}
	}

	debug("done\n");
}

sub cleanup {
	my $kept = 0;

	debug("-- Deleting old copied files ... ");
	debug("\n") if $TEST;

	opendir(my $dh, $ONEDOT_DIR) or die "Can't open directory '$ONEDOT_DIR': $!\n";
	my @files = readdir($dh);
	closedir($dh);
	
	foreach my $file (sort {$b cmp $a} @files) {
		next unless $file =~ /^onedot-\d{14}\.copied$/;
		next unless $kept++ >= $KEEP_FILES;

		if ($TEST) {
			debug("TEST: rm $ONEDOT_DIR.'/'.$file");
		} else {
			unlink($ONEDOT_DIR.'/'.$file) or die "Unable to delete '$file': $!\n";
		}
	}

	debug("done\n");
}
