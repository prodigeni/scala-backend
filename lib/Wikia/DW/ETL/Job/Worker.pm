package Wikia::DW::ETL::Job::Worker;

use strict;
use warnings;

use base qw( TheSchwartz::Worker );

use TheSchwartz::Job;

sub work {
    my $class = shift;
    my TheSchwartz::Job $job = shift;

    my $job_args = $job->arg;

    my $pkg = $job_args->{jobclass};

    eval "use $pkg";
    if (my $err = $@) {
        $job->failed("Failed to load $pkg: $err\n");
        return;
    }

    eval {
        my $j = $pkg->new();
        $j->process($job_args);
    };
    if (my $err = $@) {
        $job->failed("Failed to process: $err\n");
        return;
    }

    $job->completed();
}

1;
