package Wikia::DW::ETL::DimensionMerge;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Dimension );

sub update_table {
    my $self = shift;
    $self->{dbh} = Wikia::DW::Common::statsdb($self->{load_schema});

    $self->upsert;
    $self->delete;

    $self->{dbh}->do('COMMIT');
    $self->{dbh}->do("DROP TABLE $self->{load_schema}.$self->{load_table}");
    $self->{dbh}->disconnect;
}

sub upsert {
    my $self = shift;
    $self->{dbh}->do("REPLACE INTO $self->{schema}.$self->{table} SELECT * FROM $self->{load_schema}.$self->{load_table}");
}
    
sub delete {
    my $self = shift;
}
    
1;
