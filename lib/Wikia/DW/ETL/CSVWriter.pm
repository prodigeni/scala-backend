package Wikia::DW::ETL::CSVWriter;

use strict;
use warnings;

sub new {
    my $class = shift;
    my (%params) = @_; 
    my $self = bless \%params, ref $class || $class;
    $self->{noheader} ||= 0;
    $self->initialize;
    return $self;
}

sub initialize {
    my $self = shift;
    open $self->{FH}, "> $self->{filepath}" || die "Couldn't open file for writing";
    select((select($self->{FH}), $|=1)[0]);
    $self->{row_count} = 0;
}

sub process {
    my ($self, $data) = @_;

    my $header = $data->{columns};

    print { $self->{FH} } join(',', @{$header}), "\n" if $self->{row_count} == 0 && ! $self->{noheader};

    foreach my $row (@{$data->{rows}}) {
        $self->{row_count}++;

        my $quote_regex = qr/[,"]/;
        $quote_regex = qr/[,"\n]/ if ($self->{format} && $self->{format} eq 'Pg');

        foreach (@{$row}{@{$header}}) {
            if (defined) {
                # Escape all backslashes, so they're inserted as the literal character
                $_ =~ s/\\/\\\\/g;

                # Escape all quote characters, since they're used to enclose fields
                $_ =~ s/"/\\"/g;

                # "Quote" the entire value, if the field contains the delimiter or the quoting character
                $_ = "\"$_\"" if ($_ =~ $quote_regex);

                # Escape all newlines, so they don't interfere with the newline (record termination character)
                $_ =~ s/\n/\\n/g if ($self->{format} && $self->{format} eq 'mysql');

                # Double-quote any literal strings that are 'NULL', so they're not interpretted as an actual NULL value
                $_ =~ s/^([Nn][Uu][Ll][Ll])$/"$1"/g;
            } else {
                $_ = '\N';  # MySQL's way of loading a NULL value
            }
        }
        print { $self->{FH} } join(',', @{$row}{@{$header}}), "\n";
    }
}

sub cleanup {
    my $self = shift;
    unlink( $self->{filepath} );
}

sub finalize {
    my $self = shift;
    close $self->{FH} if $self->{FH};
}

1;
