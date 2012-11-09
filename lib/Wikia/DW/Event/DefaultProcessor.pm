package Wikia::DW::Event::DefaultProcessor;

use strict;

use base qw( Wikia::DW::Event::BaseProcessor );

sub load_config {
    my $self = shift;
    $self->SUPER::load_config;
    $self->{REQ_NUMBERS}         = [ 'wiki_id', 'namespace_id', 'user_id' ];
    $self->{OPT_NUMBERS}         = [ 'article_id' ];
    $self->{CUSTOM_LOAD_PARAMS}  = [ 'beacon', 'wiki_id', 'user_id', 'namespace_id', 'article_id' ];
    $self->{CUSTOM_LOAD_COLUMNS} = $self->{CUSTOM_LOAD_PARAMS};
}

1;
