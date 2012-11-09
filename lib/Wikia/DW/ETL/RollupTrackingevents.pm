package Wikia::DW::ETL::RollupTrackingevents;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Rollup );

sub periods {
    return [1];
}

1;
