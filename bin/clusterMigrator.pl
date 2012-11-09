#!/usr/bin/env perl

package Wikia::ClusterMigrator;

use FindBin qw/$Bin/;
use lib "$Bin/../lib";

use feature "say";
use Data::Dump;
use Wikia::WikiFactory;
use Wikia::LB;
use Try::Tiny;

use Moose;

has mysqldump => ( is => "ro", isa => "Str", default => "/usr/bin/mysqldump" );
has mysql     => ( is => "ro", isa => "Str", default => "/usr/bin/mysql" );
has city_id   => ( is => "rw", isa => "Int" );
has cluster   => ( is => "rw", isa => "Str", trigger => sub {
	my ( $self, $cluster ) = @_;

	unless ( $self->clusters->{ $cluster } ) {
		 say( "Unknown cluster $cluster. Known clusters: " .
			join ( " ", sort keys %{ $self->clusters } ) );
		 exit 1;
	}
});
has clusters => ( is => "ro", isa => "HashRef", default => sub {
		my %a = ( "c1" => 1, "c2" => 1, "c3" => 1, "c4" => 1 ); \%a
	}
);



sub migrate {
	my ( $self, $force ) = @_;

	#
	# check to which cluster $city_id belongs
	#
	my $wf = Wikia::WikiFactory->new( city_id => $self->city_id );

	my $current_cluster = defined( $wf->variables()->{ "wgDBcluster" } )
		? $wf->variables()->{ "wgDBcluster" }
		: "c1";
	my $dbname = $wf->variables()->{ "wgDBname" } || undef;

	if( $current_cluster eq $self->cluster ) {
		say( "Current cluster ($current_cluster) and target cluster (${ \$self->cluster() }) are the same.");
		exit 1;
	}

	#
	# get source cluster data
	#
	my $wikicities = "wikicities";
	if( $current_cluster ne "c1" ) {
		$wikicities = $wikicities . "_" . $current_cluster;
	}
	Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, $wikicities, Wikia::LB::ADMIN );
	my $source = Wikia::LB->instance->info();
	$source->{"wikicities"} = $wikicities;

	#
	# get target cluster data
	#
	$wikicities = "wikicities";
	if( $self->cluster ne "c1" ) {
		$wikicities = $wikicities . "_" . $self->cluster;
	}
	my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, $wikicities, Wikia::LB::ADMIN );
	my $target = Wikia::LB->instance->info();
	$target->{"wikicities"} = $wikicities;

	#
	# check if source database exists on target
	#
	my $sth = $dbh->prepare(qq{
		SELECT SCHEMA_NAME as name FROM INFORMATION_SCHEMA.SCHEMATA
		WHERE SCHEMA_NAME = ?
	});
	$sth->execute( $dbname );
	my $row = $sth->fetchrow_hashref;
	if( defined $row->{ "name" } && $row->{ "name" } eq $dbname && ! $force ) {
		say "Database $dbname already exists on cluster ${ \$self->cluster }. Use --force to skip this constraint";
		say "NOTE: if you use --force switch it will erase current data on target database!";
		exit 1;
	}

	#
	# set source database in read only mode
	#
	my $variable = $wf->set_variable( name => "wgReadOnly", value => "Migrating to other database cluster" );

	#
	# then create database on target cluster
	#
	say( "Creating database $dbname on cluster ${ \$self->cluster }" );
	$dbh->func( "createdb", $dbname, "admin" );

	#
	# so far on-fly dump | restore is supported
	# @todo add temp file for dump
	#
	my $cmd = sprintf( "%s -u%s -p%s -h%s %s| %s -u%s -p%s -h%s %s",
		$self->mysqldump,
		$source->{ "user" },
		$source->{ "pass" },
		$source->{ "host" },
		$dbname,
		$self->mysql,
		$target->{ "user" },
		$target->{ "pass" },
		$target->{ "host" },
		$dbname
	);
	say( $cmd );
	qx( $cmd );

	#
	# change city_cluster in city_list
	#
	$wf->set_city_list( name => "city_cluster", value => $self->cluster );

	#
	# change wgCluster if exits (or remove for first cluster)
	#
	if( $self->cluster eq "c1" ) {
		$wf->remove_variable( "name" => "wgDBcluster" )
	}
	else {
		$wf->set_variable( "name" => "wgDBcluster", "value" => $self->cluster );
	}

	$wf->remove_variable( "id" => $variable->{ "cv_id" } );
	say "Done.";
	say "If you are sure that everything went right type in console:";
	say sprintf( "echo \"drop database %s\" | %s -u%s -p%s -h%s %s",
		$dbname,
		$self->mysql,
		$source->{ "user" },
		$source->{ "pass" },
		$source->{ "host" },
		$source->{ "wikicities" }
	);
}
no Moose;
1;

package main;

use Getopt::Long;

sub usage {
	say ( "$0 --city-id=<city id> --cluster=<cluster>" );
	say ( "\twhere <cluster> is c1, c2 or c3" );
	exit 1;
}

my ( $city_id, $cluster, $force ) = undef;

GetOptions( "city-id=i" => \$city_id, "cluster=s" => \$cluster, "force" => \$force );

usage() unless defined $city_id && defined $cluster;

my $migrator = Wikia::ClusterMigrator->new( city_id => $city_id, cluster => lc( $cluster ) );
$migrator->migrate( $force );
