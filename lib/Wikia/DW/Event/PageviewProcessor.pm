package Wikia::DW::Event::PageviewProcessor;

use strict;

use base qw( Wikia::DW::Event::DefaultProcessor );

use Wikia::DW::ETL::SpecialPage;

sub load_config {
    my $self = shift;
    $self->SUPER::load_config;
    $self->{REQ_NUMBERS}         = [ 'wiki_id', 'namespace_id', 'user_id' ];
    $self->{TRANSFORM}           = \&transform_special_pages;
    $self->{OPT_NUMBERS}         = [ 'article_id' ];
    $self->{CUSTOM_LOAD_PARAMS}  = [ 'beacon', 'wiki_id', 'user_id', 'namespace_id', 'article_id', '@visit_id', '@visitor_id', '@ip' ];
    $self->{CUSTOM_LOAD_COLUMNS} = $self->{CUSTOM_LOAD_PARAMS};
    $self->{UNESCAPE}            = [ 'special_page' ];
}

sub transform_special_pages {
    my ($event) = @_;
    return 1 unless exists $event->{special_page};

    $event->{article_id} = Wikia::DW::ETL::SpecialPage::name_to_id($event->{special_page});
    return 1;
}

sub load_stagedb_sql {
    my $self = shift;
    my   @load_columns = @{$self->{INTERNAL_LOAD_COLUMNS}};
    push @load_columns,  @{$self->{CUSTOM_LOAD_COLUMNS}};
    return "LOAD DATA LOCAL INFILE '$self->{loadfile}' INTO TABLE $self->{STAGEDB}.${\($self->tablename)} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' (" . join(',', @load_columns) . ") SET source = '$self->{fileparser}->{source}', file_id = $self->{fileparser}->{file_id}, visit_id = UNHEX(MD5(\@visit_id)), visitor_id = UNHEX(MD5(\@visitor_id)), ip = INET_ATON(\@ip)";
}

1;
