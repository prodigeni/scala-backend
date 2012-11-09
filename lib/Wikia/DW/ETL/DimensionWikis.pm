package Wikia::DW::ETL::DimensionWikis;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::DimensionMerge );

sub config {
    my $self = shift;
    $self->SUPER::config;
    $self->{load_schema} = 'statsdb_mart';
    $self->{schema}      = 'statsdb_mart';
}

sub delete {
    my $self = shift;
    $self->{dbh}->do("UPDATE $self->{schema}.$self->{table} SET deleted = 1 WHERE NOT EXISTS (SELECT 1 FROM $self->{load_schema}.$self->{load_table} l WHERE l.wiki_id = $self->{schema}.$self->{table}.wiki_id)");
}

1;
