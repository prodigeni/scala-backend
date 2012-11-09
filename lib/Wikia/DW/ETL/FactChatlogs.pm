package Wikia::DW::ETL::FactChatlogs;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Fact );

sub config {
    my $self = shift;
    $self->SUPER::config();
    $self->{last_id_field} = 'log_id';
    $self->{min_ts_col}    = 'event_date';
    $self->{max_ts_col}    = 'event_date';
}

1;
