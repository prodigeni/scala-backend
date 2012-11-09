package Wikia::DW::Event::EventProcessor;

use strict;

use base qw( Wikia::DW::Event::DefaultProcessor );

sub load_config {
    my $self = shift;
    $self->SUPER::load_config;
    $self->{CUSTOM_LOAD_PARAMS}  = [ 'beacon', 'wiki_id', 'user_id', 'namespace_id', 'article_id', '@ip', 'is_content', 'is_redirect', 'user_is_bot', 'log_id', 'media_type', 'rev_id', 'rev_size', 'rev_timestamp', 'total_words', 'image_links', 'video_links', 'wiki_cat_id', 'wiki_lang_id' ];
    $self->{CUSTOM_LOAD_COLUMNS} = $self->{CUSTOM_LOAD_PARAMS};
    $self->{UNESCAPE}            = [ 'rev_timestamp' ];
    $self->{TRANSFORMS}          = \&transform;
}

sub transform {
    my $event = shift;

    $event->{image_links}  ||= 0;
    $event->{log_id}       ||= 0;
    $event->{media_type}   ||= 0;
    $event->{rev_size}     ||= 0;
    $event->{total_words}  ||= 0;
    $event->{video_links}  ||= 0;
    $event->{wiki_cat_id}  ||= 0;
    $event->{wiki_lang_id} ||= 0;

    $event->{is_content}  = ($event->{is_content}  || 0) == 1 ? 'Y' : 'N';
    $event->{is_redirect} = ($event->{is_redirect} || 0) == 1 ? 'Y' : 'N';
    $event->{user_is_bot} = ($event->{user_is_bot} || 0) == 1 ? 'Y' : 'N';
    return 1;
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
