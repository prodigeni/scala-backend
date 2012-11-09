#!/usr/bin/env perl

use common::sense;
use strict;
#
# batch for migrating wikis
#

package Wikia::MW119Migrator;

use FindBin qw/$Bin/;
use lib "$Bin/../lib/";

use Wikia::WikiFactory;
use Wikia::WikiFactory::Iterator;
use IPC::Run qw( run );
use Try::Tiny;
use Data::Types qw(is_int);

use Moose;
with 'MooseX::Getopt';

use constant LIMIT => 200;
use constant UPDATEPHP => "/usr/wikia/slot2/code/maintenance/update.php";
use constant PHPBIN => "/usr/bin/php";
use constant LOCALSETTINGS => "/usr/wikia/slot2/docroot/LocalSettings.php";

$|++;

has id => (
	is => 'rw',
	default => sub{ [] },
	isa => 'ArrayRef[Int]',
	documentation => 'Identifier of wiki (city_id).'
);

has cluster => (
	is            => 'rw',
	isa           => 'ArrayRef[Str]',
	default       => sub{ [] },
	documentation => "Cluster name, possible values: c1, c2, c3, c4."
);

sub execute {
	my( $self ) = @_;
	my $iterator = Wikia::WikiFactory::Iterator->new(
		city_slot => [ 1 ],
		city_cluster => $self->cluster,
		verbose => 1,
		city_id => $self->id,
	);

	my $skip = scalar@{ $self->id } ? 0 : 1;
	my $i = 0;
	my @list = ();
	while(  $i < LIMIT ) {
		my $id = $iterator->next;
		last unless is_int( $id );
		my $wiki = Wikia::WikiFactory->new( city_id => $id );
		my $variables = $wiki->variables();

		#
		# check if is already migrated
		#
		next if $wiki->slot != 1;

		#
		# check if SMW is enabled
		#
		if( $variables->{ "wgEnableSemanticMediaWikiExt" } == 1 && $skip ) {
			say "$id skipped because of wgEnableSemanticMediaWikiExt enabled";
			next;
		}
		#
		# check if wiki is locked for MW 1.16
		#
		if( $variables->{ "wgMediaWiki116Locked" } == 1 && $skip ) {
			say "$id skipped because of wgMediaWiki116Locked enabled";
			next;
		}

		my @cmd = ();
		push @cmd, PHPBIN;
		push @cmd, UPDATEPHP;
		push @cmd, "--quick";
		push @cmd, "--conf";
		push @cmd, LOCALSETTINGS;

		say "Running " . join(' ', @cmd) ." for city_id=$id";
		try {
			$ENV{ "SERVER_ID"} = $id;
			run \@cmd, \undef, \*STDOUT, \*STDERR;
		};

		#
		# update slot for wiki
		#
		$wiki->set_city_list( name=> "city_path", value => "slot2" );
		$wiki->clear_cache;

		$i++;
	}

}
__PACKAGE__->meta->make_immutable;

no Moose;

1;

package main;

my $maintenance = Wikia::MW119Migrator->new_with_options();
$maintenance->execute();

1;
