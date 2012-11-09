package Wikia::DW::ETL::RollupWikiUserGeoEvents;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Rollup );

sub periods {
    return [0,2];
}

1;
