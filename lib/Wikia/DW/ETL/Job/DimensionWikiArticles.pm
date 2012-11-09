package Wikia::DW::ETL::Job::DimensionWikiArticles;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Job::Base );

use JSON;
use Wikia::DW::Common;

sub execute {
    my ($self, $job_config) = @_;
    my $args = decode_json $job_config->{args};

    my $sql = Wikia::DW::Common::load_query_file('dimension_wiki_articles', 'select', $args);

    my $schema    = 'statsdb_tmp';
    my $tmp_table = "dimension_wiki_articles_$args->{wiki_id}";
    my $tmp_file  = "/data/tmpdir/$tmp_table.csv";

    Wikia::DW::Common::query2csv($args->{dbname}, $sql, $tmp_file);

    if (-e $tmp_file) {
        eval {
            Wikia::DW::Common::statsdb_do( [
                Wikia::DW::Common::load_query_file( 'dimension_wiki_articles',
                                                    'create',
                                                    { schema => $schema, table => $tmp_table } ),
                "LOAD DATA LOCAL INFILE '$tmp_file' REPLACE
                      INTO TABLE $schema.$tmp_table
                    FIELDS TERMINATED BY ','
                OPTIONALLY ENCLOSED BY '\"'
                     LINES TERMINATED BY '\\n' IGNORE 1 LINES",
                "DELETE FROM statsdb.dimension_wiki_articles
                  WHERE wiki_id = $args->{wiki_id}
                    AND NOT EXISTS (SELECT 1
                                      FROM $schema.$tmp_table t
                                     WHERE t.wiki_id      = dimension_wiki_articles.wiki_id
                                       AND t.namespace_id = dimension_wiki_articles.namespace_id
                                       AND t.article_id   = dimension_wiki_articles.article_id)",
                "INSERT INTO statsdb.dimension_wiki_articles
                 SELECT wiki_id,
                        namespace_id,
                        article_id,
                        \@new_title := title
                   FROM $schema.$tmp_table
                     ON DUPLICATE KEY UPDATE title = \@new_title",
                'COMMIT'
            ] );
        };
        if (my $err = $@) {
            unlink($tmp_file);
            die $err;
        }
        unlink($tmp_file);
    }
}

1;
