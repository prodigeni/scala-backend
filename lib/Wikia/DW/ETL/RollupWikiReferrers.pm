package Wikia::DW::ETL::RollupWikiReferrers;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Rollup );

sub periods {
    return [2,3];
}

sub ts_col {
    return 'pv.event_ts';
}

1;
