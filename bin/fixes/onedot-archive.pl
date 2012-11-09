#!/usr/bin/perl -w

#
# options
#
use common::sense;
use feature "say";

use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";

#
# private
#
use Wikia::Settings;
use Wikia::WikiFactory;
use Wikia::Utils;
use Wikia::LB;

#
# public
#
use MediaWiki::API;
use Pod::Usage;
use Getopt::Long;
use Thread::Pool::Simple;
use Time::HiRes qw(gettimeofday tv_interval);
use Try::Tiny;
use List::Util qw(shuffle);

package main;

sub worker {
	my( $worker_id, $cmd, $file ) = @_;

	say "Run cmd: $cmd ... ";
	my $result = 1;
	if (system($cmd) != 0) {
		say "Failed to run $cmd";
		$result = 0;
	} 
	
	if ( $result != 0 ) {
		unlink ( $file );
	}
	say "Uploaded and removed ";
	return $result;
}

our $S3_URL     = 's3://onedotdata';
our $ONEDOT_DIR = '/tokyo_data/raw';

my ( $help, $workers ) = undef;

$|++;        # switch off buffering
$workers     = 10; # by default 50 processes
GetOptions(
	"help|?"      	=> \$help,
	"workers=i"     => \$workers
) or pod2usage( 2 );

pod2usage( 1 ) if $help;

my $pool = Thread::Pool::Simple->new(
	min => 1,
	max => $workers,
	load => 4,
	do => [sub {
		worker( @_ );
	}],
	monitor => sub {
		say "done";
	},
	passid => 1,
);

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

foreach my $day (keys %files) {
	my $day_files = $files{$day};

	say "-- Archiving ".(scalar @$day_files)." file(s) for $day ... ";

	foreach ( @$day_files ) {
		my $path = $_;
		my $cmd = "s3cmd put ". $path . " $S3_URL/$day/";
		$pool->add( $cmd, $path );		
	}

	say "done\n";
}

$pool->join;

1;
__END__

=head1 NAME

onedot-archive.pl - move all onedot files to Amazon S3

=head1 SYNOPSIS

onedot-archive.pl [options]

 Options:
  --help            brief help message
  --workers=<nr>    number of workers (default 10)

=head1 OPTIONS

=over 8

=item B<--help>

Print a brief help message and exits.

=head1 DESCRIPTION

B<This programm> iterates through all archive onedot files and move it to Amazon S3
=cut
