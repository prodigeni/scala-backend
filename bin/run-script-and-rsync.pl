#!/usr/bin/perl -w

use strict;
use common::sense;

use FindBin qw/$Bin/;
use lib "$Bin/../lib";

use Data::Dumper;
use Getopt::Long;
use Pod::Usage;
use File::Find;

GetOptions(
    'hostfile=s'=> \( my $hostfile	= '/etc/dsh/group/all_web' ),
    'script=s'	=> \( my $script ),
    'slot=i'	=> \( my $slot ),
    'wiki=i'	=> \( my $wiki		= 177 ),
    'all'		=> \( my $all 		= 1 ),
    'addr=s@' 	=> \( my $addr		= [ '^ap', '^cron' ] ),
    'dir=s'		=> \( my $dir ),
    'sysuser=s'	=> \( my $sysuser	= 'release.www-data' ),
	'debug'		=> \( my $debug		= 0 ),
	'test'		=> \( my $test		= 0 ),
	'help|?'	=> \( my $help		= 0 ),
) or pod2usage( 2 );
pod2usage( 1 ) if $help;

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

# run script 
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

# chown user.group
say "Run chown on all files";
my ( $user, $group ) = split '.', $sysuser;
my $uid = getpwnam( $user ) or die 'Invalid sysuser!';
my $gid = getgrnam( $group ) or die 'Invalid group!';

find ( sub { chown $uid, $gid, $_ or die "could not chown '$_': $!"; }, $dir );

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
__END__

=head1 NAME

run-script-and-rsync.pl - run MW maintenance script and sync result for all servers

=head1 SYNOPSIS

run-script-and-rsync.pl [options]

 Options:
	--hostfile=s    default: /etc/dsh/group/all_web,
	--script=s      script from maintenance directory,
	--slot=i        id=[1|2|3|4]
	--wiki=i        SERVER_ID to run MW script
	--all           sync results to all servers (default option)
	--addr=s@       sync results to some hosts (example --addr=ap-s46 --addr=^cron)
	--dir=s         directory to sync
	--sysuser=s     USER.GROUP - set user/group for generated files
	--debug         default 0
    --test          run script without action
	--help|?		show this page

=head1 OPTIONS

=over 8

=item B<--help>

Print a brief help message and exit.

=head1 DESCRIPTION

B<This programm> run MW script and sync result of this script to all/some Wikia servers
=cut

