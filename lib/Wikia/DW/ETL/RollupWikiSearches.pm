package Wikia::DW::ETL::RollupWikiSearches;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Rollup );

sub periods {
    return [1,2];
}

1;
