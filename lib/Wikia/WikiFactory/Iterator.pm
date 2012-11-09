package Wikia::WikiFactory::Iterator;

use common::sense;

use Wikia::LB;
use Wikia::WikiFactory;
use Moose;

use Data::Types qw(is_int);

#
# attributes
#
has verbose => (
	isa           => "Bool",
	is            => "rw",
	default       => 0,
	documentation => q{Be more verbose.}
);

has list => (
	isa           => "ArrayRef",
	is            => "rw",
	lazy_build    => 1,
	documentation => q{Internal container for keeping city_ids}
);

has conditions => (
	isa           => "HashRef",
	is            => "rw",
	lazy_build    => 1,
	documentation => q{Container for conditions}
);

has city_id => (
	isa           => "ArrayRef",
	is            => "rw",
	clearer       => "clear_city_id",
	predicate     => "has_city_id",
	documentation => "Container for keeping requested wiki ids",
	trigger => sub{
		my( $self ) = @_;

		if( scalar@{ $self->city_id } ) {
		   my %cond = %{ $self->conditions };
		   $cond{ city_id } = $self->city_id;
		   $self->conditions( \%cond );
		}
	}
);

has city_lang => (
	isa           => "ArrayRef",
	is            => "rw",
	documentation => "Container for keeping requested languages",
	trigger       => sub {
		my( $self ) = @_;

		if( scalar@{ $self->city_lang } ) {
			my %cond = %{ $self->conditions };
			$cond{ city_lang } = $self->city_lang;
			$self->conditions( \%cond );
		}
	}
);

has city_dbname => (
	isa           => "ArrayRef",
	is            => "rw",
	documentation => "Container for keeping requested databases",
	trigger       => sub {
		my( $self ) = @_;

		if( scalar@{ $self->city_dbname } ) {
			my %cond = %{ $self->conditions };
			$cond{ city_dbname } = $self->city_dbname;
			$self->conditions( \%cond );
		}
	}
);

has city_cluster => (
	isa           => "ArrayRef",
	is            => "rw",
	documentation => "Container for keeping requested clusters",
	trigger       => sub {
		my( $self ) = @_;

		if( scalar @{ $self->city_cluster} ) {
			my %cond = %{ $self->conditions };
			$cond{ city_cluster } = $self->city_cluster;

			#
			# special condition for c1 cluster AKA legacy "no cluster" option
			#
			if( grep( /c1/, @{ $self->city_cluster} ) ) {
				$self->firstcluster( 1 );
			}
			$self->conditions( \%cond );
		}
	}
);

has where => (
	is            => "rw",
	isa           => "Str",
	documentation => "Generic where condition against city_list table. For example --where='city_id > 200' will add 'AND city_id > 200' condition to condition lists.",
	trigger       => sub {
		my( $self ) = @_;

		if( $self->where ne "" ) {
			if( index( $self->where, ";" ) == -1 ) {
				my %cond = %{ $self->conditions };
				push @{ $cond{ quote } }, "AND " . $self->where;
				$self->conditions( \%cond );
			}
			else {
				say "Argument for --where is not safe because contains semicolon ';' (has: " . $self->where . ").";
				exit( 1 );
			}
		}
	}
);

has active => (
	is => "rw",
	isa => "Int",
	default => sub{ 0 },
	trigger => sub{
		my( $self ) = @_;

		if ($self->active != 0) {
			my %cond = %{ $self->conditions };
			push @{ $cond{ quote } },
				sprintf( "AND city_last_timestamp BETWEEN TIMESTAMP( DATE_SUB(CURDATE(), INTERVAL %d DAY) ) AND NOW()",
					$self->active
				);
			$self->conditions( \%cond );
		}
	}
);

has slot => (
	is            => "rw",
	isa           => "ArrayRef",
	documentation => "Container for keeping requested slots",
	trigger       => sub {
		my( $self ) = @_;

		if( scalar @{ $self->slot } ) {
			my %cond = %{ $self->conditions };
			my @slots = map{ is_int( $_ ) ? "slot".$_ : () } @{ $self->slot };
			$cond{ city_path } = \@slots;
			$self->conditions( \%cond );
		}

	}
);

has _firstcluster => (
	is            => "rw",
	isa           => "Bool",
	documentation => "Until we migrate city_cluster from NULL to c1 this is valid",
	default       => sub{ 0 },
	accessor      => "firstcluster"
);

__PACKAGE__->meta->make_immutable;

#
# lazy builders
#
sub _build_list {
	my ( $self ) = @_;

	#
	# initialize database connection
	#
	my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::CENTRALSHARED );

	say "Initialize list of wikis" if $self->verbose;
	my @list = ();

	#
	# this is base query, get all active wikis
	#
	my $query = "SELECT city_id FROM city_list WHERE city_public = 1";

	#
	# and now add all additional conditions
	# @todo: make it simpler
	#
	my %conditions = %{ $self->conditions };
	for my $key ( keys %conditions ) {
		my $firstcluster = 0;
		if( $key eq "quote" ) {
			for my $q ( @{ $conditions{ $key } } ) {
				$query .= " " . $q;
			}
		}
		elsif( ref $conditions{ $key } eq "ARRAY" ) {
			# and same as above, special treat for cluster condition
			if( $key eq "city_cluster" && $self->firstcluster ) {
				my @list = map { is_int( $_) ? $_ : $dbh->quote( $_ ) }  @{ $conditions{ $key } };
				$query .= sprintf( " AND ( %s IN( %s )", $key, join ", ",  @list );
				$query .= " OR city_cluster IS NULL )";
			}
			else {
				my @list = map { is_int( $_) ? $_ : $dbh->quote( $_ ) }  @{ $conditions{ $key } };
				$query .=  sprintf( " AND %s IN( %s )", $key, join ", ",  @list );
			}
		}
	}

	say "Running $query" if $self->verbose;

	my $sth = $dbh->prepare( $query);
	$sth->execute();
	while( my $row = $sth->fetchrow_hashref ) {
		push @list, $row->{ "city_id" };
	}
	$self->list( \@list );
}

sub _build_conditions {
	my( $self ) = @_;

	my %conditions = ();

	#
	# initialize list for quoted conditions
	#
	$conditions{ "quote" } = ();

	$self->conditions( \%conditions );
}

#
# get next item from list, initialize list for first time
#
sub next {
	my ( $self ) = @_;
	say "Getting next wiki" if $self->verbose > 1;

	my @list = @{ $self->list };
	my $item = shift @list;
	$self->list( \@list );

	return $item;
}

no Moose;
1;
