package Wikia::DW::Event::AdDriverProcessor;

use strict;

use base qw( Wikia::DW::Event::BaseProcessor );

use Wikia::DW::Common;

sub load_config {
    my $self = shift;
    $self->SUPER::load_config;
    $self->{CUSTOM_LOAD_PARAMS}  = [ 'beacon', 'position', '@ip' ];
    $self->{CUSTOM_LOAD_COLUMNS} = $self->{CUSTOM_LOAD_PARAMS};
    $self->{PROCNAME}            = 'addriver';
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
       ip      = INET_ATON(\@ip)"
}

1;
