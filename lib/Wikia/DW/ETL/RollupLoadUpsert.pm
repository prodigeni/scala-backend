package Wikia::DW::ETL::RollupLoadUpsert;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Base );

use DateTime;

sub etl {
    my $self = shift;
    Wikia::DW::Common::log('  etl');

    $self->{load_table} = $self->{table};
    $self->{load_table} =~ s/^rollup/load/;

    foreach my $p (@{$self->periods}) {
        $self->etl_period($p);
    }
}

sub etl_period {
    my ($self, $period_id) = @_;
    Wikia::DW::Common::log("    period_id: $period_id");

    $self->{sql} = Wikia::DW::Common::load_query_file($self->{load_table}, 'select') if !defined $self->{sql};

    my $period_info = Wikia::DW::Common::statsdb_row("SELECT * FROM statsdb_etl.etl_periods WHERE period_id = $period_id");

    # Find the last file_id that was upserted for this rollup period
    my $last_file_id = Wikia::DW::Common::statsdb_value(
"        SELECT IFNULL(MAX(up.file_id), 0) AS max_file_id
          FROM statsdb_etl.etl_table_updates up
         WHERE up.table_name = '$self->{table}'
           AND up.period_id  = $period_id"
    );

    # TODO: fix this query to work better with multiple dependencies
    my $files = Wikia::DW::Common::statsdb_arrayref(
"         SELECT DISTINCT up.file_id,
                         up.first_ts,
                         up.last_ts,
                         up.table_name
           FROM statsdb_etl.etl_table_dependencies dep
           JOIN statsdb_etl.etl_table_updates up
             ON up.table_name = dep.depends_on
            AND up.file_id > $last_file_id
            AND up.period_id IN (0,$period_id)
          WHERE dep.table_name = '$self->{table}'
          ORDER BY up.file_id",
         'hash'
    );

    my $dbh = Wikia::DW::Common::statsdb;
    my $mart_dbh = Wikia::DW::Common::statsdb_mart;

    foreach my $f (@$files) {
        my $start_time = DateTime->now();

        $mart_dbh->do("DROP TABLE IF EXISTS $self->{load_table}");

        $mart_dbh->do( Wikia::DW::Common::load_query_file($self->{load_table}, 'create') );

        # Replace all the templatized parameters
        my $sql = $self->{sql};
        $sql =~ s/\[period_id\]/$period_id/g;
        $sql =~ s/\[time_id\]/$period_info->{time_id}/g;
        $sql =~ s/\[ts\]/$self->ts_col/eg;
        $sql =~ s/\[etl_ids\]/$period_info->{etl_ids}/g;  # only used for rolling period rollups
        $sql =~ s/\[file_id\]/$f->{file_id}/g;
        $sql =~ s/\[begin_time\]/$f->{first_ts}/g;
        $sql =~ s/\[end_time\]/$f->{last_ts}/g;
        $sql =~ s/\[from_table\]/statsdb.$f->{table_name}/g;

        my $dt = DateTime->from_epoch( epoch => $f->{file_id} );

        Wikia::DW::Common::log("      file_id: $f->{file_id} (" . $dt->ymd() . ' ' . $dt->hms() . ')');
        
        Wikia::DW::Common::debug("SQL: $sql");

        my $csv_file = "/data/tmpdir/$self->{load_table}.csv";

        Wikia::DW::Common::query2csv( 'statsdb', $sql, $csv_file );

        $mart_dbh->do("LOAD DATA LOCAL INFILE '$csv_file' INTO TABLE $self->{load_table} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES");

        $mart_dbh->do( Wikia::DW::Common::load_query_file($self->{table}, 'upsert') );

        $mart_dbh->do("DROP TABLE IF EXISTS $self->{load_table}");

        $dbh->do(
           "REPLACE INTO statsdb_etl.etl_table_updates (
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
                   $period_id AS period_id,
                   $f->{file_id} AS file_id,
                   TIMESTAMP('$f->{first_ts}') AS first_ts,
                   TIMESTAMP('$f->{last_ts}') AS last_ts,
                   ${\( DateTime->now()->epoch() - $start_time->epoch() )} AS duration"
        );
        $dbh->do('COMMIT');
    }

    $dbh->disconnect;
    $mart_dbh->disconnect;
}

sub periods {
    my $self = shift;
    my @periods = (1,2,3);
    @periods = split(',', $self->{periods}) if defined $self->{periods};
    return \@periods;
}

sub ts_col {
    return 'e.event_ts';
}

1;
