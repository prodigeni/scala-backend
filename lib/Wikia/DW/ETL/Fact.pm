package Wikia::DW::ETL::Fact;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Base );

sub config {
    my $self = shift;
    $self->SUPER::config();
    $self->{source}      = 'stats';
    $self->{load_schema} = 'statsdb';
    $self->{load_table}  = $self->{table};
}

sub etl {
    my $self = shift;

    $self->{start_time} = DateTime->now();

    Wikia::DW::Common::log('  etl');

    Wikia::DW::Common::log('    generating load files');
    $self->generate_load_files;

    Wikia::DW::Common::log('    loading table');
    $self->load_table;

    Wikia::DW::Common::log('    logging table update');
    $self->log_table_update;
}

1;
