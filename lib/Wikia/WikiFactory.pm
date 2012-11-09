package Wikia::WikiFactory;

use Wikia::LB;
use Wikia::Utils;
use Wikia::Settings;
use Data::Types        qw(:int);
use PHP::Serialization qw/serialize unserialize/;
use Cache::Memcached::libmemcached;
use Moose;

use constant LOG_VARIABLE  => 1;
use constant LOG_DOMAIN    => 2;
use constant LOG_CATEGORY  => 3;
use constant LOG_STATUS    => 4;
use constant BASEPATH      => "/usr/wikia/slot%d/code";
use constant SETTINGS      => "/usr/wikia/slot%d/docroot/LocalSettings.php";


has "city_dbname" => (
	is            => "rw",
	isa           => "Str",
	predicate     => "has_dbname",
	documentation => "Atrribute for city_dbname from city_list table",
	trigger       => sub {
		my ( $self, $db ) = @_;
		$self->_initialize_wiki( "city_dbname", $db );
	}
);

has "city_id" => ( is => "rw", isa => "Int", predicate => "has_id", "trigger" => sub {
	my ( $self, $id ) = @_;
	$self->_initialize_wiki( "city_id", $id );
} );

has "city_url"     => ( is => "rw", isa => "Str" );
has "city_cluster" => (
	is            => "rw",
	isa           => "Str",
	documentation => "Atrribute for city_cluster from city_list table"
);

has "city_public"  => ( is => "rw", isa => "Int" );
has "city_lang"    => ( is => "rw", isa => "Str" );
has "city_flags"   => ( is => "rw", isa => "Int" );
has "city_path" => (
	is => "rw",
	isa => "Str",
	documentation => "city_path value from city_list table"
);
has "city_last_timestamp" => ( is => "rw" );
has "namespaces"   => ( is => "rw", isa => "HashRef", lazy_build => 1 );

has "variables" => (
	is            => "rw",
	isa           => "HashRef",
	lazy_build    => 1,
	documentation => "Container for city_variables defined for wiki"
);

has "category"     => ( is => "rw", isa => "HashRef", lazy_build => 1 );
has "domains"      => ( is => "rw", isa => "ArrayRef[Str]", lazy_build => 1 );

has "debug" => (
	is => "rw",
	isa => "Int",
	documentation => qq{Debug severity. Use DEBUG=<int> for displaying DEBUG informations. Higher number gives more verbosity.},
	default => sub{ 0 }
);

has "dbh" => (
	is => "rw",
	lazy_build => 1,
	documentation => "DBI database handler"
);

has "slot" => (
	is => "rw",
	isa => "Int",
	lazy_build => 1,
	documentation => "Slot in medusa infrastructure."
);

has "ip" => (
	is => "rw",
	isa => "Str",
	lazy_build => 1,
	documentation => "Path to MediaWiki source code"
);

has "settings" => (
	is => "rw",
	isa => "Str",
	lazy_build => 1,
	documentation => "Path to MediaWiki LocalSettings.php file."
);

__PACKAGE__->meta->make_immutable;

#
# builders
#

sub _initialize_wiki {
	my( $self, $column, $value ) = @_;

	say STDERR "Calling ". __PACKAGE__ . "::_build_namespaces" if $self->debug > 5;

	#
	# check, maybe already everything is loaded
	#
	return if( $self->has_dbname && $self->has_id );

	#
	# read whole row from city_list and initialize
	#
	my $dbh = $self->dbh();

	if( defined( $value ) ) {
		my $sth = $dbh->prepare( sprintf("SELECT * FROM city_list WHERE %s = ?", $column ) );
		$sth->execute( $value );
		my $row = $sth->fetchrow_hashref;
		if( $row ) {
			$row->{ "city_cluster" } = "c1" unless defined $row->{ "city_cluster" };
			$self->city_dbname( $row->{ "city_dbname" } ) unless $self->has_dbname;
			$self->city_id( $row->{ "city_id" } ) unless $self->has_id;
			$self->city_cluster( $row->{ "city_cluster" } );
			$self->city_url( $row->{ "city_url" } );
			$self->city_lang( $row->{ "city_lang" } );
			$self->city_flags( $row->{ "city_flags" } );
			$self->city_last_timestamp( $row->{ "city_last_timestamp" } );
			$self->city_path( $row->{ "city_path" } );
		}
	}
	else {
		die "value of $column is not known at the moment";
	}

	#
	# set debug as well here
	#
	if( exists $ENV{ "DEBUG" } ) {
		$self->debug( to_int( $ENV{ "DEBUG" } ) );
	}

}

sub _build_dbh {
	my( $self ) = @_;

	my $fname = ( caller( 0 ) )[ 3 ];

	say STDERR "Calling ". $fname if $self->debug > 3;

	my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::CENTRALSHARED );
	$self->dbh( $dbh ) if $dbh;
}

#
# @todo: cache it in memcache
#
sub _build_namespaces {
	my( $self ) = @_;

	my $fname = ( caller( 0 ) )[ 3 ];

	say STDERR "Calling ". $fname if $self->debug > 3;
	my $url = sprintf( "%s/api.php?action=query&meta=%s&siprop=%s&format=json",
		$self->city_url,
		"siteinfo",
		"namespaces"
	);

	my $response = Wikia::Utils->fetch_json_page( $url );
	my $nms = {};

	if ( $response->{query} ) {
		my $namespaces = $response->{query}->{namespaces};
		if ( scalar( keys %$namespaces) ) {
			foreach my $ns_id ( keys %$namespaces ) {
				$nms->{$ns_id} = $namespaces->{$ns_id}->{'*'} if exists $namespaces->{$ns_id}->{'*'};
				$nms->{$ns_id} = $namespaces->{$ns_id}->{'canonical'} if exists $namespaces->{$ns_id}->{'canonical'} && ! exists $namespaces->{$ns_id}->{'*'};
			}
		}
	}
	$self->namespaces( $nms );
}

sub _build_variables {
	my( $self ) = @_;

	my %vars;
	my $sth = $self->dbh->prepare( qq{SELECT * FROM city_variables, city_variables_pool WHERE cv_city_id = ? AND cv_variable_id = cv_id} );
	$sth->execute( $self->city_id );
	while( my $row = $sth->fetchrow_hashref ) {
		$vars{ $row->{ "cv_name" } } = unserialize( $row->{ "cv_value" } );
	}

	$self->variables( \%vars );
}

sub _build_category {
	my( $self ) = @_;

	my %cats;
	my $sth = $self->dbh->prepare( qq{SELECT city_cat_mapping.cat_id, city_cats.cat_name from city_cat_mapping, city_cats where city_cats.cat_id = city_cat_mapping.cat_id and city_id = ?} );
	$sth->execute( $self->city_id );
	if( my $row = $sth->fetchrow_hashref ) {
		%cats = ( 'id' => $row->{"cat_id"}, 'name' => $row->{ "cat_name" } );
	}

	$self->category( \%cats );
}

=item _build_domains

lazy builder for domains, first domain should be main doman (taken from city_url)

=cut
sub _build_domains {

	my( $self ) = @_;

	my @domains = ();
	my ( $main ) = $self->city_url =~ m!^http://([^/*]+)/*!;

	my $sth = $self->dbh->prepare( "SELECT * FROM city_domains WHERE city_id = ?" );
	$sth->execute( $self->city_id );
	push @domains, $main;
	while( my $row = $sth->fetchrow_hashref ) {
		push @domains, $row->{ "city_domain" } unless $row->{ "city_domain"} eq $main;
	}

	$self->domains( \@domains );
}

=item _build_slot

Laizy builder for slot asset

=cut
sub _build_slot {
	my( $self ) = @_;

	my $city_path = $self->city_path;
	my $slot = 1;

	if( defined $city_path && $city_path =~ /slot(\d+)/ ) {
		$slot = $1;
	}

	$self->slot( $slot );
	return $slot;
}


=item _build_ip

laizy builder fro creating IP variable value

=cut
sub _build_ip {
	my( $self ) = @_;

	#
	# medusa uses slots for their codes
	#
	my $slot = $self->slot;
	my $ip = sprintf( BASEPATH, $slot );

	$self->ip( $ip );

	return $self->ip;
}

=item _build_settings

laizy builder for creating settings

=cut
sub _build_settings {
	my( $self ) = @_;

	#
	# medusa uses slots for their codes
	#
	my $slot = $self->slot;
	my $settings = sprintf( SETTINGS, $slot );

	$self->settings( $settings );

	return $self->settings;
}

=item set_variable

$wf->set_variable( name => "wgReadOnly", value => "new value for variable" );
$wf->set_variable( id => 165, value => "new value for variable" );

=cut
sub set_variable {
	my( $self, %var ) = @_;

	my $cv_id = 0;
	my $cv_value_old = undef;

	#
	# check variable id for $name (use master because we want to set value)
	#
	my $row = undef;
	if( exists $var{ "name" } ) {
		my $sth = $self->dbh->prepare( "SELECT * FROM city_variables_pool WHERE cv_name = ?" );
		$sth->execute( $var{ "name" } );
		$row = $sth->fetchrow_hashref();
		$cv_id = $row->{ "cv_id" };
		$cv_value_old = $row->{ "cv_value" }
	}
	else {
		$cv_id = $var{ "id" };
	}

	#
	# serialize value
	#
	my $cv_value = serialize( $var{ "value" } );

	#
	# set new value in database
	#
	my $sth = $self->dbh->prepare( "DELETE FROM city_variables WHERE cv_variable_id = ? AND cv_city_id = ?" );
	$sth->execute( $cv_id, $self->city_id );
	$sth->finish;

	$sth = $self->dbh->prepare( "INSERT city_variables(cv_city_id, cv_variable_id, cv_value ) VALUES ( ?, ?, ? )" );
	$sth->execute( $self->city_id, $cv_id, $cv_value );

	#
	# @todo update city_list log
	#

	#
	# clear cache
	#
	$self->clear_cache();

	return $row;
}

=item remove_variable

	$wf->remove_variable( name => "wgDBcluster" );
	remove variable value from city_variables for this wiki

=cut
sub remove_variable {
	my( $self, %var ) = @_;

	my $cv_id = 0;

	#
	# check variable id for $name (use master because we want to set value)
	#
	my $row = undef;
	if( exists $var{ "name" } ) {
		my $sth = $self->dbh->prepare( "SELECT * FROM city_variables_pool WHERE cv_name = ?" );
		$sth->execute( $var{ "name" } );
		$row = $sth->fetchrow_hashref();
		$cv_id = $row->{ "cv_id" };
	}
	else {
		$cv_id = $var{ "id" };
	}

	#
	# delete value from database
	#
	my $sth = $self->dbh->prepare( "DELETE FROM city_variables WHERE cv_variable_id = ? AND cv_city_id = ?" );
	$sth->execute( $cv_id, $self->city_id );

	#
	# clear cache
	#
	$self->clear_cache();
}

=item clear_cache

	$wf->clear_cache

	quite stupid way but we actually don't know which memcache server has this key
	so we have to remove key from all caches

=cut
sub clear_cache {
	my ( $self ) = @_;

	my $ws = Wikia::Settings->instance;
	my $servers = $ws->variables()->{ "wgMemCachedServers" } || undef;

	if( defined( $servers ) ) {
		for my $server ( @$servers ) {
			my $mc = Cache::Memcached::libmemcached->new({
				servers => [$server]
			});
			$mc->delete( sprintf( "wikifactory:variables:v5:%d", $self->city_id ) );
		}
	}
}


=item set_city_list

	$wf->set_city_list( name => "city_cluster", value => "c2" )

	Set value in city_list

	@todo change into array of hashes
=cut
sub set_city_list {
	my ( $self, %var ) = @_;

	return unless defined $var{ "name" };

	#
	# do some magic for some names
	#
	if( $var{ "name" } eq "city_cluster" ) {
		if( $var{ "value" } eq "c1" ) {
			$var{ "value" } = undef;
		}
	}

	my $sth = $self->dbh->prepare( sprintf("UPDATE city_list SET %s = ? WHERE city_id = ?", $var{ "name" } ) );
	$sth->execute( $var{"value"}, $self->city_id );
}
1;
