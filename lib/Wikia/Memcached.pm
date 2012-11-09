package Wikia::Memcached;

use Wikia::Settings;
use Data::Dumper;
use YAML::XS;
use Cache::Memcached::libmemcached;
use MooseX::Singleton;
use IO::File;

our $VERSION = "0.01";

has "servers" 	=> ( is => "rw", lazy_build => 1 );
has "memc" 	=> ( is => "rw", lazy_build => 1 );

=head1 NAME

Wikia::Memcached - expose Mediawiki settings

=head1 VERSION

version $VERSION

=cut

#
# builders
#
sub _build_servers {
	my ( $self ) = @_;

	my $ws = Wikia::Settings->instance;
	my $servers = $ws->variables()->{ "wgMemCachedServers" } || undef;
	$self->servers ( $servers );
}

sub _build_memc {
	my ( $self ) = @_;

	my $mc = Cache::Memcached::libmemcached->new( {
		servers => $self->servers,
		connect_timeout => 0.2,
		io_timeout => 0.5,
		close_on_error => 1,
		compress_threshold => 100_000,
		compress_ratio => 0.9,
		compress_methods => [ \&IO::Compress::Gzip::gzip, \&IO::Uncompress::Gunzip::gunzip ]
	} );
	$self->memc( $mc );
}

1;
