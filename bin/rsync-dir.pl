#!/usr/bin/perl -w

use strict;
use common::sense;

use FindBin qw/$Bin/;
use lib "$Bin/../lib";

use Data::Dumper;
use Getopt::Long;

GetOptions(
    'hostfile=s'=> \( my $hostfile	= '/etc/dsh/group/all_web' ),
    'script=s'	=> \( my $script ),
    'slot=i'	=> \( my $slot ),
    'wiki=i'	=> \( my $wiki		= 177 ),
    'all'		=> \( my $all 		= 0 ),
    'addr=s@' 	=> \( my $addr		= [ '^ap', '^cron' ] ),
    'dir=s'		=> \( my $dir ),
	'debug'		=> \( my $debug		= 0 ),
	'test'		=> \( my $test		= 0 )
);
die "Invalid params \n" unless ( $hostfile || $dir || $script || $slot );

my $start_ts = time();
my @hosts = ();
say "Reading host file: $hostfile";
open FILE, "<", "$hostfile" || die "Cannot open $hostfile $!";
while (<FILE>) {
	say "Found host $_" if ( $debug );
    next if /^\s*#/ ; 
    next unless /\S/; 
    chomp;
    my $name = $_;
    if ( $all ) {
		push @hosts, $name;
	} else {
		foreach ( @$addr ) {
			if ( $name =~ m/($_)/i ) {
				push @hosts, $name;
			}
		}
	}
}
close FILE;
say "Found " . scalar (@hosts) . " hosts";

$script = sprintf( 
	"SERVER_ID=%d /usr/bin/php /usr/wikia/slot%d/code/maintenance/%s --conf=/usr/wikia/slot%d/docroot/LocalSettings.php",
	$wiki,
	$slot,
	$script,
	$slot
);

if ( $test ) {
	say "Run script $script in --test mode";
} else {
	say "Run script $script";
	if ( system ( $script ) ) {
		die "Failed to execute $script\n";
	}
}

say "Sync all files to all hosts";
foreach ( @hosts ) {
	my $host_ts = time();
	say "Sync directory $dir to host $_";
	my $dest_dir = sprintf( "%s:%s", $_, $dir );
	
	my $cmd = "/usr/bin/rsync -Pavq $dir $dest_dir";
	
	if ( $test ) {
		say $cmd;
	} else {
		if ( system( $cmd ) ) {
			say "\tcannot sync to host $_";
		}
	}
	my @hostts = gmtime( time() - $host_ts );
	say "Host synced after " . sprintf ("%d hours %d minutes %d seconds\n", @hostts[2,1,0]);
}

my @ts = gmtime( time() - $start_ts );
say "Finished after " . sprintf ("%d hours %d minutes %d seconds", @ts[2,1,0]);

1;
