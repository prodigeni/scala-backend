package Wikia::DW::Event::TrackingeventProcessor;

use strict;

use base qw( Wikia::DW::Event::DefaultProcessor );

sub load_config {
    my $self = shift;
    $self->SUPER::load_config;
    $self->{CUSTOM_LOAD_PARAMS}  = [ 'beacon', 'wiki_id', 'user_id', 'namespace_id', 'article_id', 'ga_category', 'ga_action', 'ga_label', 'ga_value' ];
    $self->{CUSTOM_LOAD_COLUMNS} = $self->{CUSTOM_LOAD_PARAMS};
    $self->{UNESCAPE} = [ 'ga_category', 'ga_action', 'ga_label', 'ga_value' ];
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
    beacon,
    wiki_id,
    user_id,
    namespace_id,
    article_id,
    ga_category,
    ga_action,
    ga_label,
    ga_value
)
SELECT source,
       file_id,
       event_id,
       event_ts,
       event_type,
       beacon,
       wiki_id,
       user_id,
       namespace_id,
       article_id,
       CASE ga_category WHEN 'Pulse'          THEN 'pulse'
                        WHEN 'FeaturedVideo'  THEN 'featured-video'
                        WHEN 'Tabber'         THEN 'tabber'
                        WHEN 'MosaicSlider'   THEN 'mosaic-slider'
                        WHEN 'TopWikis'       THEN 'top-wikis'
                        WHEN 'Explore'        THEN 'explore'
                        WHEN 'SuggestArticle' THEN 'suggest-article'
                        WHEN 'SuggestVideo'   THEN 'suggest-video'
                                              ELSE ga_category END AS ga_category,
       ga_action,
       ga_label,
       ga_value
  FROM $self->{STAGEDB}.${\($self->tablename)}"
}

1;
