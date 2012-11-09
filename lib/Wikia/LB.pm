package Wikia::LB;

use common::sense;
use strict; # to make perl criticism happy

use Wikia::Settings;
use Wikia::Utils;


use YAML::XS;
use DBD::mysql;
use Data::Dumper;
use IO::File;
use Data::Types        qw(:int);
use PHP::Serialization qw(serialize unserialize);
use Try::Tiny;

use MooseX::Singleton;
use Cache::Memcached::libmemcached;

use List::Util         qw(shuffle);

use constant DB_MASTER      => -2;
use constant DB_SLAVE       => -1;
use constant DEFAULTCFG     => "/usr/wikia/conf/current/DB.yml";
use constant EXTERNALSHARED => "wikicities";
use constant DBSTATSSHARED  => "dbstats";
use constant STATS          => "stats";
use constant METRICS		=> "metrics";
use constant DBSTATSWIKIA   => "wikiastats";
use constant DATAWARESHARED => "dataware";
use constant CENTRALSHARED  => "wikicities";
use constant USER           => 0; # user connection, use regular user & pass
use constant ADMIN          => 1; # admin connection, use admin user & pass
use constant MAILER         => 2; # mailer connection, use mailer user & pass

=head1 NAME

Wikia::LB - MediaWiki load balancer for Wikia scripts

=head1 VERSION

version 0.02

=head1 SYNOPSIS

  use Wikia::LB;
  my $lb = new Wikia::LB;

  # get slave connection to wikicities database
  my $dbh = $lb->getConnection( Wikia::LB::DB_SLAVE, undef, "wikicities" );

  # get master connection to firefly database
  my $dbh = $lb->getConnection( Wikia::LB::DB_MASTER, undef, "firefly" );

  # get slave connection to wikicities database and DO NOT exit if the connection fails, just return undef.
  my $dbh = $lb->getConnection( Wikia::LB::DB_SLAVE, undef, "wikicities" );

=cut

our $VERSION = '0.02';
has "debug"       => ( is => "rw", default => 0 );
has "yml"         => ( is => "rw", default => undef );
has "conf"        => ( is => "rw", isa => "HashRef" );
has "sections"    => ( is => "rw", isa => "HashRef" );
has "externals"   => ( is => "rw", default => undef );
has "info"        => ( is => "rw", isa => "HashRef" );
has "cache"       => ( is => "rw", isa => "HashRef", lazy_build => 1 );
has "admin"       => ( is => "rw", isa => "Int", default => Wikia::LB::USER );
has "specials"    => ( is => "ro", isa => "ArrayRef[Str]", default => sub{ my @s = ("smw+"); \@s },
	documentation => "special entries configuration are used for extinguishing sections which are not normal databases."
);
has "autocharset" => ( is => "rw", default => 0 );


=head1 METHODS

=head2 _build_cache

lazy builder for cache

=cut
sub _build_cache {
	my ( $self ) = @_;

	my %cache;


	my $lb  = __PACKAGE__->instance();
	$lb->yml( yml => $self->yml );
	my $dbh = $lb->getConnection( DB_SLAVE, 'cron', EXTERNALSHARED );
	my $sth = $dbh->prepare(qq{
		SELECT
			city_id, cv_value, cv_variable_id, cv_name
		FROM
			city_variables, city_list, city_variables_pool
		WHERE
			city_public = 1
		AND
			city_list.city_id = city_variables.cv_city_id
		AND
			city_variables.cv_variable_id = city_variables_pool.cv_id
	});

	$sth->execute();
	while( my $row = $sth->fetchrow_hashref ) {
		$row->{ "cv_value" } = unserialize( $row->{ "cv_value" } );
		$cache{ $row->{ "city_id" } } = $row;
	}

	#
	# close connection
	#
	$sth->finish();
	$dbh->disconnect();

}

=head2 readConfig

	read config from Yaml file given as param

=cut
sub readConfig {
	my ( $self ) = @_;

	#
	# set debug as well here
	#
	if( exists $ENV{ "DEBUG" } ) {
		$self->debug( to_int( $ENV{ "DEBUG" } ) );
	}

	my $path = $self->yml;
	unless( $path ) {
		#
		# try to read from env variable first
		#
		$path = $ENV{ "WIKIA_DB_YML" };
		$path = DEFAULTCFG unless( defined $path );
	}

	die "Cannot read configuration from " . $path unless -f $path;
	print STDERR "Reading configuration from $path\n" if $self->debug;
	$self->yml( $path );

	# load file into variable
	my $fh = new IO::File;
	if( $fh->open( $path ) ) {
		my @yml = <$fh>;
		@yml = @{ Load join( "", @yml ) };
		$self->conf( shift @yml );
		$self->sections( shift @yml );
		$self->externals( shift @yml );
	}
	return $self->conf;
}

=head2 getCluster

	get information about cluster stored in database, it's little recursive

=cut
sub getCluster {
	my( $self, $name ) = @_;

	#
	# check cache first, it is always faster and do not use db connection
	#

	my $lb  = __PACKAGE__->instance();
	$lb->yml( yml => $self->yml );
	my $dbh = $lb->getConnection( DB_SLAVE, 'cron', EXTERNALSHARED );
	my $sth = $dbh->prepare(qq{
		SELECT
			cv_value
		FROM
			city_variables
		WHERE
			cv_variable_id = (SELECT cv_id FROM city_variables_pool WHERE cv_name='wgDBcluster' )
		AND
			cv_city_id = ( SELECT city_id FROM city_list WHERE city_dbname = ? order by city_id limit 1 )
	});

	$sth->execute( $name );
	my $value = $sth->fetchrow || undef;

	#
	# close connection
	#
	$sth->finish();
	$dbh->disconnect();

	$value = unserialize( $value ) if $value;
	return $value;
}

=head2 connectDSN
	connect to database using DBI interface

	$db		master or slave
	$group	which group in load balancer
	$name	name of database or connection
=cut
sub connectDSN {
	my ( $self, $db, $group, $name ) = @_;

	my $fname = ( caller( 0 ) )[ 3 ];

	# if connection doesn't exist we have to connect into
	$self->readConfig() unless $self->conf;

	my $sectionsByDB = $self->conf()->{ 'sectionsByDB' };

	# read database sections
	my $section = "DEFAULT";
	#
	# first check if user want to get special section
	#
	my $special = undef;
	for my $s ( @{ $self->specials } ) {
		if( $name =~ /^\Q$s\E/ ) {
			say STDERR "$fname: special section $s found in $name" if $self->debug;
			$special = $s;
			last;
		}
	}
	if( defined $special && exists( $sectionsByDB->{ $special } ) ) {
		$section = $sectionsByDB->{ $special };
		say STDERR "$fname: found special section $special in load balancer configuration" if $self->debug;
	}
	elsif( exists( $sectionsByDB->{ $name } ) ) {
		# checking if name is defined in section
		$section = $sectionsByDB->{ $name };
	}
	else {
		# read which cluster is used for database, cluster for EXTERNALSHARED is
		# always known, it prevent loops
		if( $name ne EXTERNALSHARED ) {
			my $cluster = $self->getCluster( $name );
			if( $cluster && exists( $sectionsByDB->{ $cluster } ) ) {
				$section = $sectionsByDB->{ $cluster }
			}
			else {
				$section = $cluster if $cluster;
			}
		}
	}

	# we have now section name so we getting sectionLoads
	# (and of course we have $self-sections for checking who's master)
	my $sectionLoads = $self->conf()->{'sectionLoads'}->{ $section };
	my $groupLoadsBySection = ();
	if( $group && ref( $self->conf()->{'groupLoadsBySection'} ) eq "HASH" ) {
		$groupLoadsBySection = $self->conf()->{'groupLoadsBySection'}->{ $section }->{ $group };
	}

	my @slaves = @{$self->sections()->{ 'DEFAULT' }};
	if( exists $self->sections()->{ $section } ) {
		@slaves = @{$self->sections()->{ $section }};
	}
	else {
		say STDERR "$fname: $section is not defined, DEFAULT used" if $self->debug > 1;
	}
	my $master = shift @slaves;

	if( scalar keys %$groupLoadsBySection ) {
		@slaves = keys %$groupLoadsBySection;
	}

	my $want = undef;

	# do we want master or slave?
	if( $db == Wikia::LB::DB_MASTER ) {
		$want = $master;
	}
	else {
		if( scalar @slaves ) {
			# random slave if exists
			$want = @{ [ shuffle( @slaves ) ] }[ 0 ];
		}
		else {
			# or master otherwise
			$want = $master;
		}
	}

	#
	# only for not warning about not defined $group entry
	#
	$group = '' unless defined $group;
	say STDERR "$fname: We want type=$db; group=$group; name=$name; have=$want" if $self->debug > 1;

	# so now we need rest of connection variables
	my $serverTemplate = $self->conf()->{ "serverTemplate" };
	my $override = $self->conf()->{ "templateOverridesByServer" }->{ $want };
	$serverTemplate->{ "dbname" } = $name;
	$serverTemplate->{ "host" } = $self->conf()->{ "hostsByName" }->{ $want };

	if( $override ) {
		for my $key ( keys %$override ) {
			$serverTemplate->{ $key } = $override->{$key}
		}
	}

	#
	# for admin connection use other user
	#
	if( $self->admin == Wikia::LB::ADMIN ) {
		my $settings = Wikia::Settings->new();
		$serverTemplate->{ "user" } = $settings->variables()->{ "wgDBadminuser" };
		$serverTemplate->{ "password" } = $settings->variables()->{ "wgDBadminpassword" };
		say STDERR "$fname: Use admin password for admin connection" if $self->debug;
	}

	#
	# for mailer connection use other user
	#
	if( $self->admin == Wikia::LB::MAILER ) {
		my $settings = Wikia::Settings->new();
		my $var = $settings->variables();
		if ( $var->{ "wgEmailSendGridDBGroups" }->{ $group } ) {
			$serverTemplate->{ "user" } = $var->{ "wgEmailSendGridDBGroups" }->{ $group }->{ "username" };
			$serverTemplate->{ "password" } = $var->{ "wgEmailSendGridDBGroups" }->{ $group }->{ "password" };
			$serverTemplate->{ "host" } = $var->{ "wgEmailSendGridDBGroups" }->{ $group }->{ "host" };
			$serverTemplate->{ "dbname" } = $var->{ "wgEmailSendGridDBGroups" }->{ $group }->{ "database" };
			say STDERR "$fname: Use mailer password for mailer ($group) connection" if $self->debug;
		}
	}

	return $serverTemplate;
}

=head2 connect
	connect to database using DBI interface

	$db		master or slave
	$group	which group in load balancer
	$name	name of database or connection
	$failGracefully if set, and non-zero, will return "undef" on failed connection instead of exiting.
=cut
sub connect {
	my ( $self, $db, $group, $name, $failGracefully ) = @_;

	my $fname = ( caller( 0 ) )[ 3 ];

	#
	# store info about last connection
	#
	my %last = ();

	my $template = $self->connectDSN( $db, $group, $name );

	$last{ "name" } = $template->{ "dbname" };
	$last{ "user" } = $template->{ "user" };
	$last{ "pass" } = $template->{ "password" };
	$last{ "host" } = $template->{ "host" };

	my $dbh = undef;
	my $dsn = sprintf( "DBI:mysql:database=%s;host=%s",
			$template->{ "dbname" }, $template->{ "host" },
			{ RaiseError => 0 }
	);

	say STDERR "$fname: Connecting to $dsn" if $self->debug;

	#
	# try to reuse connection for wikicities and other non-wikis databases
	#
	if( $name eq EXTERNALSHARED ) {
		try {
			$dbh = DBI->connect( $dsn,
				$template->{ "user" },
				$template->{ "password" },
				{ 'mysql_auto_reconnect' => 1, 'mysql_connect_timeout' => 60, RaiseError => 1 }
			) or die $DBI::errstr;
		}
		catch {
			say STDERR "$fname: cant connect (external) - ".$DBI::errstr if $self->debug;
			if($failGracefully){
				return;
			} else {
				exit;
			}
		}
	}
	else {
		try {
			$dbh = DBI->connect( $dsn,
				$template->{ "user" },
				$template->{ "password" },
				{ 'mysql_auto_reconnect' => 1, 'mysql_connect_timeout' => 60, RaiseError => 1 }
			) or die $DBI::errstr;
		}
		catch {
			say STDERR "$fname: cant connect - ".$DBI::errstr if $self->debug;
			if($failGracefully){
				return;
			} else {
				exit;
			}
		}
	}

	if ( $self->autocharset ) {
		my $charset = "latin1";
		$charset = $template->{ "charset" } if defined $template->{ "charset" };
		say STDERR "$fname: client encoding = ", $charset if $self->debug;
		my $sth = $dbh->prepare(qq(SET NAMES $charset));
		$sth->execute();
		$sth->finish();
	}

	$self->info( \%last );
	return $dbh;
}

=head1 METHODS

=head2 lightmode

lazy builder for cache

=cut
sub lightmode {
	my ( $self ) = @_;

	my $isLightModeOn = undef;
	my $ws = Wikia::Settings->instance;
	my $servers = $ws->variables->{wgMemCachedServers};
	my $oMemc = Cache::Memcached::libmemcached->new({ servers => $servers, compress_threshold => 10_000 });
	my $memkey = sprintf( "perl:lb:lightmode" );

	# load from memcache
	eval {
		my $res = undef;
		$res = $oMemc->get( $memkey ) if ( $oMemc );
		if ( defined( $res ) ) {
			$isLightModeOn = $res->{lightmode};
		}
	};

	if ( $@ ) {
		print "Memc error: " . $@ . "! \n";
	}

	if ( !defined $isLightModeOn ) {
		my $dbh = $self->connect( DB_SLAVE, 'cron', EXTERNALSHARED );
		my $sth = $dbh->prepare(qq{
			SELECT
				city_id, cv_value, cv_variable_id, cv_name
			FROM
				city_variables, city_list, city_variables_pool
			WHERE
				city_public = 1
			AND
				city_list.city_id = city_variables.cv_city_id
			AND
				city_variables.cv_variable_id = city_variables_pool.cv_id
			AND
				city_variables_pool.cv_name = 'wgDBLightMode'
			AND
				city_list.city_dbname = ?
		});

		$sth->execute( EXTERNALSHARED );
		if( my $row = $sth->fetchrow_hashref ) {
			$isLightModeOn = Wikia::Utils->intval( unserialize( $row->{ "cv_value" } ) );
		} else {
			$isLightModeOn = 0;
		}

		$oMemc->set( $memkey, { 'lightmode' => $isLightModeOn }, 60 * 10 ) if ( $oMemc );

		#
		# close connection
		#
		$sth->finish();
		$dbh->disconnect();
	}

	return $isLightModeOn;
}

=head2 getConnection
	return initialized connection
=cut
sub getConnection {

	my( $self, $db, $group, $name, $admin, $failGracefully ) = @_;
	if ( defined $admin ) {
		$self->admin( $admin );
	}
	else {
		if ( ! $self->isa("Wikia::ExternalLB") ) {
			while ( $self->lightmode() ) {
				say "The database is currently in light mode ( wgDBLightMode = true ) ";
				sleep(10);
			}
		}
	}

	my $conn = $self->connect( $db, $group, $name, $failGracefully );

	return $conn;
}

=head1 AUTHOR

Krzysztof Krzyżaniak (eloy) <eloy@wikia-inc.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2009 by Krzysztof Krzyżaniak.

This is free software; you can redistribute it and/or modify it under
the same terms as perl itself.

=cut

1;
