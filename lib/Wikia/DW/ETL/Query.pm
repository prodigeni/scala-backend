package Wikia::DW::ETL::Query;

use strict;
use warnings;

use Wikia::DW::ETL::Database;

sub new {
    my $class = shift;
    my (%params) = @_; 
    my $self = bless \%params, ref $class || $class;
    return $self;
}

sub run {
    my $self = shift;

    $self->_execute;

    while (my $result = $self->{sth}->fetchrow_hashref()) {

        $self->{data}->{rows} = [ $result ];
        $self->{data}->{row_count}++;

        foreach my $proc (@{ $self->{processors} }) {
            $proc->process( $self->{data} );
        }
    }

    $self->finalize;
}

sub process {
    my $self = shift;

    if (! exists $self->{sth}) {
        $self->_execute;
    }

    my $result = $self->{sth}->fetchrow_hashref();

    $self->{data}->{rows} = [ $result ];
    $self->{data}->{row_count}++;

    return $self->{data};
}

sub _execute {
    my $self = shift;

    if (! exists $self->{database}) {
        $self->{database} = Wikia::DW::ETL::Database->new( source => $self->{source} ) || die "No valid <source> parameter provided";
    }

    $self->{sth} = $self->{database}->{dbh}->prepare($self->{query});
    $self->{sth}->execute();

    $self->{header} = $self->{sth}->{NAME};
    $self->{data} = { 
                 method    => 'Wikia::DW::ETL::Query',
                 columns   => $self->{header},
                 rows      => [],
                 row_count => 0
               };
}

sub finalize {
    my $self = shift;
    $self->{sth}->finish     if $self->{sth};
    $self->{dbh}->disconnect if $self->{dbh};
    foreach my $processor (@{ $self->{processors} }) {
        $processor->finalize;
    }
}

sub DESTROY {
    my $self = shift;
    $self->finalize();
}

1;
