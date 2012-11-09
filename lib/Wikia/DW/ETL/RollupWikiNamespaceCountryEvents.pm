package Wikia::DW::ETL::RollupWikiNamespaceCountryEvents;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Rollup );

sub periods {
    return [2,3];
}

1;
