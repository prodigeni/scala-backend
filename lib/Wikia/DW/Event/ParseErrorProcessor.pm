package Wikia::DW::Event::ParseErrorProcessor;

use strict;

use base qw( Wikia::DW::Event::BaseProcessor );

sub load_config {
    my $self = shift;
    $self->SUPER::load_config();
    $self->{INTERNAL_LOAD_PARAMS}  = [ '_EVENT_ID', '_EVENT_TS' ];  # internal parameters here
    $self->{INTERNAL_LOAD_COLUMNS} = [ 'event_id',   'event_ts' ];  # table columns for internal parameters
}

1;
