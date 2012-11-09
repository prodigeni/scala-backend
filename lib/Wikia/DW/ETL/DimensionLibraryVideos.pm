package Wikia::DW::ETL::DimensionLibraryVideos;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Dimension );

sub config {
    my $self = shift;
    $self->SUPER::config;
    $self->{source} = 'video151';
}

sub min_ts_col {
    return "TIMESTAMP('2004-01-01')";
}

sub max_ts_col {
    my $self = shift;
    return "TIMESTAMP('$self->{start_time}->ymd() $self->{start_time}->hms()')";
}

1;
