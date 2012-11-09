package Wikia::DW::ETL::Job::DimensionWikiEmbeds;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Job::Base );

use JSON;
use Wikia::DW::Common;

sub execute {
    my ($self, $job_config) = @_;
    my $args = decode_json $job_config->{args};

    my $sql = Wikia::DW::Common::load_query_file('dimension_wiki_embeds', 'select', $args);

    my $rnd       = substr(rand(), 2, 8);
    my $schema    = 'statsdb_tmp';
    my $tmp_table = "dimension_wiki_embeds_$args->{wiki_id}_$rnd";
    my $tmp_file  = "/data/tmpdir/dimension_wiki_embeds.$args->{wiki_id}.$rnd.csv";

    Wikia::DW::Common::query2csv($args->{dbname}, $sql, $tmp_file);

    if (-e $tmp_file) {
        eval {
            Wikia::DW::Common::statsdb_do( [
                Wikia::DW::Common::load_query_file( 'dimension_wiki_embeds',
                                                    'create',
                                                    { schema => $schema,
                                                      table  => $tmp_table } ),
                "LOAD DATA LOCAL INFILE '$tmp_file' REPLACE
                      INTO TABLE $schema.$tmp_table
                    FIELDS TERMINATED BY ','
                OPTIONALLY ENCLOSED BY '\"'
                     LINES TERMINATED BY '\\n' IGNORE 1 LINES",
                "DELETE FROM statsdb.dimension_wiki_embeds
                  WHERE wiki_id = $args->{wiki_id}
                    AND NOT EXISTS (SELECT 1
                                      FROM $schema.$tmp_table t
                                     WHERE t.wiki_id     = dimension_wiki_embeds.wiki_id
                                       AND t.article_id  = dimension_wiki_embeds.article_id
                                       AND t.video_title = dimension_wiki_embeds.video_title)",
                "REPLACE INTO statsdb.dimension_wiki_embeds SELECT * FROM $schema.$tmp_table",
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
