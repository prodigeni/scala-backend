package Wikia::DW::ETL::Job::TimeoutTest;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Job::Base );

sub execute {
    my ($self, $job_config) = @_;
    sleep 60;
}

1;
