package Wikia::DW::ETL::RollupWikiVisits;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Rollup );

sub periods {
    return [2];
}

1;
