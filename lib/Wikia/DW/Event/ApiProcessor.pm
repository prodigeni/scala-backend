package Wikia::DW::Event::ApiProcessor;

use strict;

use base qw( Wikia::DW::Event::DefaultProcessor );

sub load_config {
    my $self = shift;
    $self->SUPER::load_config;
    $self->{REQ_NUMBERS}          = [];
    $self->{OPT_NUMBERS}          = [];
    $self->{CUSTOM_LOAD_PARAMS}   = [ 'host', 'api_type', 'api_function', 'request_method', '@api_key',  '@ip',  'called_by_wikia' ];
    $self->{CUSTOM_LOAD_COLUMNS}  = $self->{CUSTOM_LOAD_PARAMS};
}

sub load_stagedb_sql {
    my $self = shift;
    my   @load_columns = @{$self->{INTERNAL_LOAD_COLUMNS}};
    push @load_columns,  @{$self->{CUSTOM_LOAD_COLUMNS}};
    return
"  LOAD DATA LOCAL INFILE '$self->{loadfile}'
  INTO TABLE $self->{STAGEDB}.${\($self->tablename)}
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' (" . join(',', @load_columns) . ")
   SET source  = '$self->{fileparser}->{source}',
       file_id = $self->{fileparser}->{file_id},
       ip      = INET_ATON(\@ip),
       api_key = UNHEX(\@api_key)"  # Note: api_key and ip are processed on load
}

sub load_fact_sql {
    my $self = shift;
    return
"REPLACE INTO fact_$self->{PROCNAME}_events (
    source,
    file_id,
    event_id,
    event_ts,
    event_type,
    wiki_id,
    api_type,
    api_function,
    request_method,
    api_key,
    ip,
    called_by_wikia
)
SELECT l.source,
       l.file_id,
       l.event_id,
       l.event_ts,
       l.event_type,
       d.wiki_id,
       l.api_type,
       l.api_function,
       l.request_method,
       l.api_key,
       l.ip,
       l.called_by_wikia
  FROM $self->{STAGEDB}.${\($self->tablename)} l
  LEFT JOIN statsdb.dimension_wiki_domains d
    ON d.domain = l.host"  # Note: wiki_id is retrieved on INSERT from load table to fact table
}

1;
