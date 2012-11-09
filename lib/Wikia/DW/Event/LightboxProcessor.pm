package Wikia::DW::Event::LightboxProcessor;

use strict;

use base qw( Wikia::DW::Event::DefaultProcessor );

use URI::Escape;

sub load_config {
    my $self = shift;
    $self->SUPER::load_config;
    $self->{CUSTOM_LOAD_PARAMS}  = [ 'beacon', 'wiki_id', 'user_id', 'namespace_id', 'article_id', 'ga_category', 'ga_action', 'ga_label', 'ga_value', 'title', 'provider', 'click_source' ];
    $self->{CUSTOM_LOAD_COLUMNS} = $self->{CUSTOM_LOAD_PARAMS};
    $self->{UNESCAPE}            = [ 'ga_category', 'ga_action', 'ga_label', 'ga_value', 'title' ];
    $self->{TRANSFORM}           = \&transform;
}

sub transform {
    my $event = shift;
    if ($event->{'ga_category'} && $event->{'ga_category'} eq 'lightbox') {
        if (defined $event->{'title'} && $event->{'title'} ne '') {
            eval {
                $event->{'title'} = uri_unescape($event->{'title'});
            };
            $event->{'title'} =~ s/ /_/g;
        }
        return 1;
    }
    return 0;
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
    ga_value,
    title,
    provider,
    click_source
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
       ga_value,
       title,
       provider,
       click_source
  FROM $self->{STAGEDB}.${\($self->tablename)}"
}

1;
