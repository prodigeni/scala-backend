package Wikia::DW::ETL::Job::NoOp;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Job::Base );

sub execute {
    my ($self, $job_config) = @_;
}

1;
