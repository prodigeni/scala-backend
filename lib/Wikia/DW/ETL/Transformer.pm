package Wikia::DW::ETL::Transformer;

use strict;
use warnings;

sub new {
    my $class = shift;
    my (%params) = @_; 
    my $self = bless \%params, ref $class || $class;
    $self->initialize;
    return $self;
}

sub initialize {
    my $self = shift;
    $self->{row_count} = 0;
}

sub process {
    my ($self, $data) = @_;
    $self->transform($data);
    foreach my $processor (@{ $self->{processors} }) {
        $processor->process($data);
    }
}

sub finalize {
    my $self = shift;
    foreach my $processor (@{ $self->{processors} }) {
        $processor->finalize;
    }
}

sub transform {
    my ($self, $data) = @_;
    $self->{transform}->($data) if $self->{transform};
    return;
}

1;
