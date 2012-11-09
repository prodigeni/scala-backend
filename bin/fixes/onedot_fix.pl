#!/usr/bin/perl

use strict;
my $YML = undef;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use DBI;
use Wikia::LB;
use Wikia::DB;
use Wikia::Utils qw( note );
use Wikia::Config;

use Getopt::Long;
use Data::Dumper;

use Thread::Pool::Simple;

my $process_start_time = time();
my $dir = '/tokyo_data/stats/';
opendir DIR, $dir or die "cannot open dir $dir: $!";
my @file= readdir DIR;
closedir DIR;

my $pool = Thread::Pool::Simple->new(
	min => 1,
	max => 8,
	load => 3,
	do => [sub {
		my ( $f ) = @_;
		my @name = split( /\_/, $f, 3);
		if ( $name[0] =~ /^(\d+\.?\d*|\.\d+)$/ ) {
			note "parse " . $f . " file";
			$name[2] =~ s/\.tch//g; 
			my $cmd = "/usr/bin/perl " . $FindBin::Bin . "/../scribe/onedot.pl --option=move --file=$f --table=" . $name[2] . " >> /tmp/onedot_move.log";
			note "run $cmd";
			system "$cmd";
			note "parse done";	
		}
	}],
	monitor => sub {
		print "done \n";
	}
);

foreach ( @file ) {
	my $f = $_;
	$pool->add( $f );
}

print "Wait until all threads finish ... \n";
$pool->join();

my $process_end_time = time();
my @ts = gmtime($process_end_time - $process_start_time);
note "\n\nscript processed ".sprintf ("%d hours %d minutes %d seconds",@ts[2,1,0]);
note "done";

1;
