package Wikia::DW::ETL::RollupHubBeaconPageviews;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Rollup );

sub periods {
    my $self = shift;
    return [1];
}

1;
