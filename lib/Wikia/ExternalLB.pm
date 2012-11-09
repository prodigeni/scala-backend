package Wikia::ExternalLB;

use Data::Dumper;
use List::Util         qw(shuffle);
use MooseX::Singleton;
use common::sense;
use Wikia::LB;

extends "Wikia::LB";

=head1 NAME

Wikia::ExternalLB - MediaWiki load balancer for Wikia scripts

=head1 VERSION

version 0.02

=head1 SYNOPSIS

  use Wikia::ExternalLB;

  # get slave connection to wikicities database
  my $dbh = Wikia::ExternalLB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, "blobs" );

  # get master connection to firefly database
  my $dbh = Wikia::ExternalLB->instance->getConnection( Wikia::LB::DB_MASTER, undef, "archive1" );

=cut

our $VERSION = '0.01';

=head1 METHODS

=head2 connectDSN
	connect to database using DBI interface

	$db		master or slave
	$group	which group in load balancer
	$name	name of database or connection
=cut
sub connectDSN {

	my $fname = ( caller( 0 ) )[ 3 ];

	my ( $self, $db, $group, $name ) = @_;

	# if connection doesn't exist we have to connect into
	$self->readConfig() unless $self->conf;

	my $want = undef;

	my $serverTemplate = $self->conf()->{ "serverTemplate" };
	$serverTemplate->{ "dbname" } = $name;

	my @slaves = ();
	if( exists $self->externals()->{ $name } ) {
		@slaves = @{ $self->externals()->{ $name } };
	}
	my $master = shift @slaves;
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

	$serverTemplate->{ "host" } = $self->conf()->{ "hostsByName" }->{ $want };;

	say STDERR "$fname: We want type=$db; group=$group; name=$name; want=$want" if $self->debug > 1;

	# template overwrite by cluster
	my $override = $self->conf()->{ "templateOverridesByCluster" }->{ $name };
	if( $override ) {
		for my $key ( keys %$override ) {
			$serverTemplate->{ $key } = $override->{ $key };
		}
	}

	return $serverTemplate;
}

=head1 AUTHOR

Krzysztof Krzyżaniak (eloy) <eloy@wikia-inc.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2009 by Krzysztof Krzyżaniak.

This is free software; you can redistribute it and/or modify it under
the same terms as perl itself.

=cut

1;
