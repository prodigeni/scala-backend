package Wikia::DW::Event::WikiahubsProcessor;

use strict;

use base qw( Wikia::DW::Event::DefaultProcessor );

sub load_config {
    my $self = shift;
    $self->SUPER::load_config;
    $self->{CUSTOM_LOAD_PARAMS}  = [ 'beacon', 'wiki_id', 'user_id', 'namespace_id', 'article_id', 'page_title', 'event', 'label' ];
    $self->{CUSTOM_LOAD_COLUMNS} = [ 'beacon', 'wiki_id', 'user_id', 'namespace_id', 'article_id', 'page_title', 'event', 'label' ];
    $self->{UNESCAPE} = [ 'page_title', 'label' ];
}

1;
