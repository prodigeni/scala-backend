package Wikia::DW::ETL::RollupApiEvents;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Rollup );

sub periods {
    return [60,1,3];
}

1;
