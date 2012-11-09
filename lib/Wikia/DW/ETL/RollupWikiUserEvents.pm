package Wikia::DW::ETL::RollupWikiUserEvents;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Rollup );

sub periods {
    return [1,2,3,15];
}

1;
