package Wikia::DW::ETL::Base;

use strict;
use warnings;

use Wikia::DW::Common;

sub new {
    my $class = shift;
    my (%params) = @_; 
    my $self = bless \%params, ref $class || $class;
    Wikia::DW::Common::log("Initialized $class");
    return $self;
}

sub process {
    my $self = shift;
    $self->config;
    $self->etl;
    $self->cleanup;
    $self->done;
}

sub config {
    my $self = shift;
    Wikia::DW::Common::log('  config');
    $self->{load_schema}       = 'statsdb_tmp';
    $self->{load_table}        = "$self->{table}_new";
    $self->{old_table}         = "$self->{table}_old";
    $self->{schema}            = 'statsdb';
    $self->{tmp_file}          = "/data/tmpdir/$self->{table}." . substr(rand(), 2, 8) . '.csv';
    $self->{replace_or_ignore} = 'REPLACE';
}

sub etl {
    my $self = shift;
    Wikia::DW::Common::log('  etl');
}

sub cleanup {
    my $self = shift;
    Wikia::DW::Common::log('  cleanup');
    unlink $self->{tmp_file} if (-e $self->{tmp_file});
}

sub done {
    my $self = shift;
    Wikia::DW::Common::log('done.');
}

sub log_table_update {
    my $self = shift;
    Wikia::DW::Common::statsdb_do( [
"        REPLACE INTO statsdb_etl.etl_table_updates (
            table_name,
            updated_at,
            period_id,
            file_id,
            first_ts,
            last_ts,
            duration
        )
        SELECT '$self->{table}' AS table_name,
               now() AS updated_at,
               0     AS period_id,
               0     AS file_id,
               MIN(${\( $self->min_ts_col )}) AS first_ts,
               MAX(${\( $self->max_ts_col )}) AS last_ts,
               ${\( DateTime->now()->epoch() - $self->{start_time}->epoch() )} AS duration
          FROM $self->{schema}.$self->{table}",
        'COMMIT'
    ] );
}

sub last_id {
    my $self = shift;

    my $default = 0;
    $default = "'2012-01-01'" if $self->{last_id_field} =~ m/_at$/;

    return Wikia::DW::Common::statsdb_value("SELECT IFNULL(MAX($self->{last_id_field}),$default) AS last_id FROM $self->{schema}.$self->{table}");
}

sub load_table {
    my $self = shift;

    my $statsdb_dbh = Wikia::DW::Common::statsdb($self->{load_schema});

    $statsdb_dbh->do(
"LOAD DATA LOCAL INFILE '$self->{tmp_file}' $self->{replace_or_ignore}
                   INTO TABLE $self->{load_schema}.$self->{load_table}
                 FIELDS TERMINATED BY ','
             OPTIONALLY ENCLOSED BY '\"'
                  LINES TERMINATED BY '\\n'
                 IGNORE 1 LINES"
    );

    $statsdb_dbh->do('COMMIT');
    $statsdb_dbh->disconnect;
}

sub generate_load_files {
    my $self = shift;
    my $sql = Wikia::DW::Common::load_query_file($self->{table}, 'select', { last_id => $self->last_id() });
    Wikia::DW::Common::query2csv($self->source, $sql, $self->{tmp_file});
}

sub source {
    my $self = shift;
    return $self->{source};
}

sub min_ts_col {
    my $self = shift;
    return $self->{min_ts_col};
}

sub max_ts_col {
    my $self = shift;
    return $self->{max_ts_col};
}

1;
