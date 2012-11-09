package Wikia::DW::Event::SearchProcessor;

use strict;

use base qw( Wikia::DW::Event::DefaultProcessor );

sub load_config {
    my $self = shift;
    $self->SUPER::load_config;
    $self->{CUSTOM_LOAD_PARAMS}  = [ 'beacon', 'wiki_id', 'user_id', 'namespace_id', 'article_id', 'search_term', 'position' ];
    $self->{CUSTOM_LOAD_COLUMNS} = $self->{CUSTOM_LOAD_PARAMS};
    $self->{UNESCAPE}            = [ 'search_term' ];
}

1;
