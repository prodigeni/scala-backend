package Wikia::DW::ETL::RollupWikiGeoPageviews;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Rollup );

sub periods {
    return [3];
}

1;
