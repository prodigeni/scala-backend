package Wikia::DW::ETL::Job::Base;

use strict;
use warnings;

use Wikia::DW::Common;

sub new {
    my $class = shift;
    my (%params) = @_; 
    my $self = bless \%params, ref $class || $class;
    return $self;
}

# Perform work
sub process {
    my ($self, $options) = @_;
}

1;
