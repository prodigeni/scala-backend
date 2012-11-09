#
# base class for maintenance scripts
#
package Wikia::Maintenance;

use Moose::Role;
with 'MooseX::Getopt';

requires "execute";


use common::sense;
use Wikia::WikiFactory::Iterator;

has id => (
	is            => 'rw',
	default       => sub{ [] },
	isa           => 'ArrayRef[Int]',
	documentation => 'Identifier of wiki (city_id).'
);

has db => (
	isa           => 'ArrayRef',
	is            => 'rw',
	default       => sub { [] },
	documentation => 'Database name of wiki.'
);

has cluster => (
	is            => 'rw',
	isa           => 'ArrayRef[Str]',
	default       => sub{ [] },
	documentation => "Cluster name, possible values: c1, c2, c3, c4."
);

has slot => (
	isa           => 'ArrayRef',
	is            => 'rw',
	default       => sub { [] },
	documentation => 'Source code slot for wiki. If not defined all slots will be used. Possible values: 1, 2, 3 etc.',
);

has lang => (
	isa           => 'ArrayRef[Str]',
	is            => 'rw',
	default       => sub{ [] },
	documentation => 'Language of wiki (city_lang).'
);

has _procs => (
	isa           => 'Int',
	is            => 'rw',
	default       => 1,
	documentation => "Number of proces to run. Default value is 1"
);

has active => (
	isa           => 'Int',
	is            => 'rw',
	default       => 0,
	documentation => "Run on wikis which are active at least <param> days"
);

has verbose => (
	isa           => "Bool",
	is            => "rw",
	default       => 0,
	documentation => "Be more verbose."
);

has where => (
	isa           => "Str",
	is            => "rw",
	default       => "",
	documentation => "Generic where condition against city_list table. For example --where='city_id > 200' will add 'AND city_id > 200' condition to condition lists."
);

has _iterator => (
    is            => "rw",
    documentation => "Container for WikiFactory iterator with defined parameters",
	accessor      => "iterator",
	lazy_build    => 1
);

has _wiki_id => (
	is            => "rw",
	isa           => "Int",
	documentation => "Containter for current loop iterator item",
	accessor      => "current"
);

#
# laizy builder for iterator
#
sub _build__iterator {
	my( $self ) = @_;
	my $iterator = Wikia::WikiFactory::Iterator->new(
		city_cluster => $self->cluster,
		city_lang    => $self->lang,
		city_id      => $self->id,
		slot         => $self->slot,
		city_dbname  => $self->db,
		active       => $self->active,
		verbose      => $self->verbose,
		slot         => $self->slot,
		where        => $self->where
	);
	$self->iterator( $iterator );
}

#
# main loop
#
sub run {
	my( $self ) = @_;

	while( my $wiki_id = $self->iterator->next ) {
		$self->current( $wiki_id );
		$self->execute;
	}
}

no Moose;
1;
