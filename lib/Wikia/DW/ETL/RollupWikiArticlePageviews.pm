package Wikia::DW::ETL::RollupWikiArticlePageviews;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::RollupLoadUpsert );

sub periods {
    my $self = shift;
    return [2];
}

1;
