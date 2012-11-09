package Wikia::DW::Event::EmbedChangeProcessor;

use strict;

use base qw( Wikia::DW::Event::DefaultProcessor );

sub load_config {
    my $self = shift;
    $self->SUPER::load_config;
    $self->{CUSTOM_LOAD_PARAMS}  = [ 'beacon', 'wiki_id', 'user_id', 'namespace_id', 'article_id', '@ip' ];
    $self->{CUSTOM_LOAD_COLUMNS} = $self->{CUSTOM_LOAD_PARAMS};
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
1;
