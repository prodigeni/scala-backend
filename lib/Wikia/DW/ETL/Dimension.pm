package Wikia::DW::ETL::Dimension;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Base );

use DateTime;
use Wikia::DW::ETL::CSVWriter;
use Wikia::DW::ETL::Query;

sub config {
    my $self = shift;
    $self->SUPER::config;
    $self->{source} = 'wikicities';
    $self->{min_ts_col} = 'created_at';
    $self->{max_ts_col} = 'created_at';
}

sub etl {
    my $self = shift;

    $self->{start_time} = DateTime->now();

    Wikia::DW::Common::log('  etl');

    Wikia::DW::Common::log('    generating load files');
    $self->generate_load_files;

    Wikia::DW::Common::log('    creating tmp table');
    $self->create_tmp_table;

    Wikia::DW::Common::log('    loading tmp table');
    $self->load_table;

    Wikia::DW::Common::log('    adding indexes');
    $self->add_indexes;

    Wikia::DW::Common::log('    dropping old tmp table');
    $self->drop_old_tmp_table;

    Wikia::DW::Common::log('    updating table');
    $self->update_table;

    Wikia::DW::Common::log('    logging table update');
    $self->log_table_update;
}

sub generate_load_files {
    my $self = shift;
    my $sql = Wikia::DW::Common::load_query_file($self->{table}, 'select');
    Wikia::DW::Common::log("[DEBUG]\n$sql\n") if $Wikia::DW::Common::DEBUG;

    my $w = Wikia::DW::ETL::CSVWriter->new( filepath => $self->{tmp_file} );

    my $q = Wikia::DW::ETL::Query->new( database   => Wikia::DW::ETL::Database->new( source => $self->source ),
                                        query      => $sql,
                                        processors => [ $w ] );

    $q->run;
    $q->finalize;
}

sub create_tmp_table {
    my $self = shift;

    my $sql = Wikia::DW::Common::load_query_file( $self->{table},
                                                  'create',
                                                  { schema => $self->{load_schema},
                                                    table  => $self->{load_table}  } );

    my $statsdb = Wikia::DW::Common::statsdb($self->{load_schema});
    $statsdb->do("DROP TABLE IF EXISTS $self->{load_schema}.$self->{load_table}");
    $statsdb->do($sql);
    $statsdb->disconnect;
}

sub add_indexes {
    my $self = shift;

    my $sql = Wikia::DW::Common::load_query_file( $self->{table},
                                                  'index',
                                                  { schema => $self->{load_schema},
                                                    table  => $self->{load_table}  } );

    my $statsdb = Wikia::DW::Common::statsdb($self->{load_schema});

    foreach my $stmt (split(';', $sql)) {
        chomp($stmt);
        $statsdb->do($stmt) if $stmt ne '';
    }

    $statsdb->disconnect;
}

sub drop_old_tmp_table {
    my $self = shift;

    my $statsdb = Wikia::DW::Common::statsdb($self->{load_schema});
    $statsdb->do("DROP TABLE IF EXISTS $self->{load_schema}.$self->{old_table}");
    $statsdb->disconnect;
}

sub update_table {
    my $self = shift;

    my $statsdb = Wikia::DW::Common::statsdb($self->{load_schema});
    $statsdb->do("RENAME TABLE $self->{schema}.$self->{table} TO $self->{load_schema}.$self->{old_table},
                               $self->{load_schema}.$self->{load_table} TO $self->{schema}.$self->{table}");
    $statsdb->disconnect;
}

1;
