package Wikia::DW::ETL::RollupWikiVideoViews;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::RollupLoadUpsert );

sub periods {
    my $self = shift;
    return [1,3];
}

1;
