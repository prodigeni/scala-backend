package Wikia::DW::Event::BaseProcessor;

use strict;

use Wikia::DW::Common;

use DBD::mysql;
use URI::Escape;

sub new {
    my $class = shift;
    return bless {} , ref $class || $class;
}

sub init {
    my ($self, $params) = @_;

    # Keep a ref to the Wikia::DW::Event::Fileparser object that instantiated this event processor
    $self->{fileparser} = $params->{fileparser};

    $self->load_config;

    # Initialize anything that might use configurable settings below this line
    $self->{loadfile}   = "$self->{fileparser}->{localbase}.$self->{PROCNAME}.load";
    $self->{rejectfile} = "$self->{fileparser}->{localbase}.$self->{PROCNAME}.reject";

    ($self->{totalcount}, $self->{loadcount}, $self->{rejectcount}, $self->{skipcount}) = (0,0,0,0);

    # Take a timestamp from the beginning of processing
    $self->{load_ts} = `date +"%Y-%m-%d %H:%M:%S"`;

    $self->open_files;

    return $self;
}

sub load_config {
    my $self = shift;
    ($self->{PROCNAME})            = ref($self) =~ m/([^:]+)Processor$/;
    $self->{PROCNAME}              = lc($self->{PROCNAME});
    $self->{REQ_NUMBERS}           = [];
    $self->{OPT_NUMBERS}           = [];
    $self->{TRANSFORM}             = undef;
    $self->{INTERNAL_LOAD_PARAMS}  = [ '_EVENT_ID', '_EVENT_TS', '_EVENT_TYPE' ];  # internal parameters here
    $self->{INTERNAL_LOAD_COLUMNS} = [  'event_id',  'event_ts',  'event_type' ];  # table columns for internal parameters
    $self->{CUSTOM_LOAD_PARAMS}    = [];  # table column names (should be same as the event parameters)
    $self->{CUSTOM_LOAD_COLUMNS}   = $self->{CUSTOM_LOAD_PARAMS};  # expects columns to match parameter names
    $self->{UNESCAPE}              = [];
    $self->{STAGEDB}               = 'statsdb_tmp';
}

sub process_event {
    my ($self, $event) = @_;
    $self->{totalcount}++;

    # These parameters should be run through uri_unescape
    foreach my $k ( @{$self->{UNESCAPE}} ) {
        if (defined $event->{$k} && $event->{$k} ne '') {
            eval {
                $event->{$k} = uri_unescape($event->{$k});
                $event->{$k} =~ s/\+/ /g;
                1;
            };
            if (my $err = $@) {
                $self->reject_event($event, "PROBLEM RUNNING uri_unescape ON PARAM '$k'");
                return;
            }
        }
    }

    # These parameters must exist and be numeric
    foreach my $k ( @{$self->{REQ_NUMBERS}} ) {
        unless (defined $event->{$k} && $event->{$k} =~ /^[+-]?\d+$/) {
            $self->reject_event($event, "MISSING REQUIRED NUMERIC PARAM '$k'");
            return;
        }
    }

    # These parameters must be numeric (or null)
    foreach my $k ( @{$self->{OPT_NUMBERS}} ) {
        if ($event->{$k} && $event->{$k} !~ /^[+-]?\d+$/) {
            $self->reject_event($event, "FOUND NON-NUMERIC PARAM '$k'");
            return;
        }
    }

    # Optionally run a custom transform (and potentially skip the event)
    if (defined $self->{TRANSFORM}) {
        if ( ! $self->{TRANSFORM}->($event) ) {
            $self->{skipcount}++; 
            return;
        }
    }

    # Write to load file
    $self->load_event($event);
}

sub load_event {
    my ($self, $event) = @_;
    $self->{loadcount}++;

    # Escape the event parameters and quote if necessary
    my $quote_regex = qr/[,"]/;

    foreach ( keys %{$event} ) {
        if (defined($event->{$_})) {

            # Escape all backslashes, so they're inserted as the literal character
            $event->{$_} =~ s/\\/\\\\/g;

            # Escape all newlines, so they don't interfere with the newline (record termination character)
            $event->{$_} =~ s/\n/\\n/g;

            # Escape all quote characters, since they're used to enclose fields
            $event->{$_} =~ s/"/\\"/go;

            # "Quote" the entire value, if the field contains the delimiter or the quoting character
            $event->{$_} = "\"$event->{$_}\"" if ($event->{$_} =~ $quote_regex);

            # Double-quote any literal strings that are 'NULL', so they're not interpretted as an actual NULL value
            $event->{$_} =~ s/^(null)$/"$1"/i;
        } else {
            $event->{$_} = '\N';  # MySQL's way of loading a NULL value
        }
    }

    my   @load_params = @{$self->{INTERNAL_LOAD_PARAMS}};
    push @load_params,  @{$self->{CUSTOM_LOAD_PARAMS}};

    print { $self->{loadfile_h} } join(',', map { exists $event->{$_} ? $event->{$_} : '\N' } @load_params) . "\n";
}

sub reject_event {
    my ($self, $event, $reason) = @_;
    $self->{rejectcount}++;
    print { $self->{rejectfile_h} } "$event->{_EVENT_ID},$reason\n";
}

sub finalize {
    my $self = shift;
    my $stat_format = '  %-40s %9d total %9d load %9d reject %9d skip';
    Wikia::DW::Common::log(sprintf($stat_format, ref($self), $self->{totalcount}, $self->{loadcount}, $self->{rejectcount}, $self->{skipcount}));
    $self->close_files;
    $self->load;  # The important part!
    $self->cleanup;
}

sub open_files {
    my $self = shift;
    open $self->{loadfile_h},   "> $self->{loadfile}";
    open $self->{rejectfile_h}, "> $self->{rejectfile}";
}

sub close_files {
    my $self = shift;
    close $self->{loadfile_h}   if exists $self->{loadfile_h};
    close $self->{rejectfile_h} if exists $self->{rejectfile_h};
}

sub prepare_stagedb_table {
    my $self = shift;

    Wikia::DW::Common::statsdb_do(
        "DROP TABLE IF EXISTS $self->{STAGEDB}.${\($self->tablename)}"
    );

    open SQL, "/usr/wikia/backend/lib/Wikia/DW/SQL/load/load_$self->{PROCNAME}_events_create.sql";
        my @sql = <SQL>;
        my $sql = join '', @sql;
    close SQL;

    $sql =~ s/\[TABLENAME\]/$self->{STAGEDB}.${\($self->tablename)}/g;

    Wikia::DW::Common::statsdb_do($sql);
}

sub load_stagedb {
    my $self = shift;
    $self->{dbh}->do( $self->load_stagedb_sql );
}

sub load_stagedb_sql {
    my $self = shift;
    my   @load_columns = @{$self->{INTERNAL_LOAD_COLUMNS}};
    push @load_columns,  @{$self->{CUSTOM_LOAD_COLUMNS}};
    return "LOAD DATA LOCAL INFILE '$self->{loadfile}' INTO TABLE $self->{STAGEDB}.${\($self->tablename)} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' (" . join(',', @load_columns) . ") SET source = '$self->{fileparser}->{source}', file_id = $self->{fileparser}->{file_id}";
}

sub pre_fact_load {
    my $self = shift;
    return;
}

sub load_fact {
    my $self = shift;
    $self->{dbh}->do( $self->load_fact_sql );
}

sub load_fact_sql {
    my $self = shift;
    return "REPLACE INTO fact_$self->{PROCNAME}_events SELECT * FROM $self->{STAGEDB}.${\($self->tablename)}";
}

sub load {
    my $self = shift;

    $self->prepare_stagedb_table;

    # Save a database connection, so the following statements/methods can use a single transaction (if they choose)
    $self->{dbh} = Wikia::DW::Common::statsdb;

    $self->load_stagedb;

    $self->pre_fact_load;

    $self->load_fact;

    $self->{dbh}->do(
        "REPLACE INTO statsdb_etl.etl_file_loads (
            source,
            file_id,
            load_table,
            load_ts,
            loaded,
            rejected,
            rowcount,
            min_event_ts,
            max_event_ts
        )
        SELECT '$self->{fileparser}->{source}',
               $self->{fileparser}->{file_id},
               '${\($self->tablename)}',
               TIMESTAMP('$self->{load_ts}'),
               $self->{loadcount},
               $self->{rejectcount},
               COUNT(1)      AS rowcount,
               MIN(event_ts) AS min_ts,
               MAX(event_ts) AS max_ts
          FROM $self->{STAGEDB}.${\($self->tablename)}"
    );

    $self->{dbh}->do(
       "REPLACE INTO statsdb_etl.etl_table_updates (
            table_name,
            updated_at,
            period_id,
            file_id,
            first_ts,
            last_ts,
            duration
        )
        SELECT 'fact_$self->{PROCNAME}_events' AS table_name,
               now() AS updated_at,
               0     AS period_id,
               $self->{fileparser}->{file_id},
               MIN(event_ts) AS first_ts,
               MAX(event_ts) AS last_ts,
               null AS duration
          FROM $self->{STAGEDB}.${\($self->tablename)}"
    );

    $self->{dbh}->do('COMMIT');

    $self->{dbh}->do("DROP TABLE IF EXISTS $self->{STAGEDB}.${\($self->tablename)}");

    $self->{dbh}->disconnect;
}

sub tablename {
    my $self = shift;
    return "$self->{PROCNAME}_$self->{fileparser}->{year}_$self->{fileparser}->{month}_$self->{fileparser}->{day}_$self->{fileparser}->{hour}_$self->{fileparser}->{minute}_$self->{fileparser}->{file_id}";
}

sub cleanup {
    my $self = shift;
    unlink $self->{loadfile}   if (-e $self->{loadfile});
    unlink $self->{rejectfile} if ($self->{rejectcount} == 0 && (-e $self->{rejectfile}));
}

1;
