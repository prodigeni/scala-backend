package Wikia::DW::ETL::RollupWikiLangPageviews;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Rollup );

sub periods {
    my $self = shift;
    return [2];
}

1;
