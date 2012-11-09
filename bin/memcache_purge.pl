#!/usr/bin/perl -w

use Socket;
use common::sense;
use feature "say";

use FindBin qw/$Bin/;
use lib "$Bin/../lib/";
use Getopt::Long;
use Cache::Memcached::libmemcached;
use Thread::Pool::Simple;
use IO::Interface::Simple;

use Wikia::Settings;
use Wikia::WikiFactory;
use Wikia::Utils;
use Wikia::LB;

$|=1;
my $op = 'listen';
my $table = 'memcache_keys';
my $help = undef;
my $port = 3306;
my $debug = 0;
my $workers = 10;

GetOptions(
	"op=s"		=> \$op,
	"table=s"	=> \$table,
	"help|?"	=> \$help,
	"debug"		=> \$debug,
	"workers"	=> \$workers,
	"port=i"	=> \$port
) or pod2usage( 2 );

pod2usage( 1 ) if $help;


sub purge_memcache {
	my $key = shift;
	my $ws = Wikia::Settings->instance;
	my $servers = $ws->variables()->{ "wgMemCachedServers" } || undef;
	
	say "Cannot found any memcache server " unless $servers;
	
	my $pool = Thread::Pool::Simple->new(
		min => 4,
		max => $workers,
		load => 2,
		do => [sub {
			my ( $self, $server, $key ) = @_;
			my $mc = Cache::Memcached::libmemcached->new( { servers => [ $server ], connect_timeout => 0.2, io_timeout => 0.5, close_on_error => 1, nowait => 1 } );
			next unless $mc;
			$mc->delete( $key );
			say "purged $key on $server" if ( $debug );
		}],
		monitor => sub {
			say "done";
		},
		passid => 1,
	);
	
	$pool->add( $_, $key ) foreach ( @$servers );
	$pool->join;
	
	say "Removed $key on all machines";
}

my $iface = IO::Interface::Simple->new_from_index(1);

if ( $op eq 'test' ) {
	my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS );
	$dbh->{ "mysql_auto_reconnect" } = 1;
	for ( my $i = 0; $i<255; $i++ ) {
		my $sth = $dbh->prepare( "INSERT INTO $table VALUES ( ? )" );
		$sth->execute( 'test ' . $i );
	}
} 
elsif ( $op eq 'listen' ) {
	open (STDIN,"/usr/sbin/tcpdump -i $iface -s 0 -l -n -w - -q -tttt dst port $port | strings |");
	while (<>) {
		chomp; next if /^[^ ]+[ ]*$/;
		if (/^INSERT(.*)$table(.*)VALUES\s*\((.*)[^\)]\)(.*)?/i) {
			purge_memcache($3);
		}
	}
}

1;
__END__

=head1 NAME

memcache_purge.pl - read memcache keys from mysql and remove them on all memcaches

=head1 SYNOPSIS

memcache_purge.pl [options]

 Options:
  --help            brief help message
  --table=<table>	mysql table to listen
  --port=<port>     listen on port (default 3306)

=head1 OPTIONS

=over 8

=item B<--help>

Print a brief help message and exits.

=head1 DESCRIPTION

B<This programm> will read memcache keys from mysql and remove them on all memcache machines.
=cut

