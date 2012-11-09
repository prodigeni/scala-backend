package Wikia::DW::ETL::DimensionWikiContentNamespaces;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Dimension );

use PHP::Serialization qw(serialize unserialize);
use Wikia::DW::ETL::Transformer;
use Wikia::DW::ETL::CSVWriter;
use Wikia::DW::ETL::Database;

sub config {
    my $self = shift;
    $self->SUPER::config;
    $self->{source} = 'statsdb';
}

sub generate_load_files {
    my $self = shift;
    my $sql = Wikia::DW::Common::load_query_file($self->{table}, 'select');
    Wikia::DW::Common::log("[DEBUG]\n$sql\n") if $Wikia::DW::Common::DEBUG;
    $sql =~ s/\s+/ /g;

    # Writer
    my $w = Wikia::DW::ETL::CSVWriter->new( filepath => $self->{tmp_file} );

    # Transformer that outputs to Writer
    my $t = Wikia::DW::ETL::Transformer->new(
        processors => [ $w ],
        transform => sub {
            my $data = shift;
            my $rows = [];
            foreach my $r (@{$data->{rows}}) {
                my $namespaces = unserialize($r->{namespace_id});
                if (ref($namespaces) eq 'HASH') {
                    $namespaces = [ values %$namespaces ];
                }
                foreach my $n (@$namespaces) {
                    push @$rows, { wiki_id => $r->{wiki_id}, namespace_id => $n };
                }
            }
            $data->{rows} = $rows;
            $data->{row_count} = scalar @$rows;
            return;
        }
    );

    # Query that outputs to Transformer
    my $q = Wikia::DW::ETL::Query->new( database   => Wikia::DW::ETL::Database->new( source => $self->{source} ),
                                        query      => $sql,
                                        processors => [ $t ] );

    $q->run;
    $q->finalize;
}

sub min_ts_col {
    return "TIMESTAMP('2004-01-01')";
}

sub max_ts_col {
    my $self = shift;
    return "TIMESTAMP('$self->{start_time}->ymd() $self->{start_time}->hms()')";
}

1;
