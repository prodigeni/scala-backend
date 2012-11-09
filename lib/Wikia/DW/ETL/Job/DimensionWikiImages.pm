package Wikia::DW::ETL::Job::DimensionWikiImages;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Job::Base );

use JSON;
use Wikia::DW::Common;

sub execute {
    my ($self, $job_config) = @_;
    my $args = decode_json $job_config->{args};

    my $sql = Wikia::DW::Common::load_query_file('dimension_wiki_images', 'select', $args);

    my $schema    = 'statsdb_tmp';
    my $tmp_table = "dimension_wiki_images_$args->{wiki_id}";
    my $tmp_file  = "/data/tmpdir/dimension_wiki_images.$args->{wiki_id}.csv";

    Wikia::DW::Common::query2csv($args->{dbname}, $sql, $tmp_file);

    if (-e $tmp_file) {
        Wikia::DW::Common::statsdb_do( [
            Wikia::DW::Common::load_query_file( 'dimension_wiki_images',
                                                'create',
                                                { schema => $schema, table => $tmp_table } ),
            "LOAD DATA LOCAL INFILE '$tmp_file' REPLACE
                  INTO TABLE $schema.$tmp_table
                FIELDS TERMINATED BY ','
            OPTIONALLY ENCLOSED BY '\"'
                 LINES TERMINATED BY '\\n' IGNORE 1 LINES",
            "DELETE FROM dimension_wiki_images
              WHERE wiki_id = $args->{wiki_id}
                AND NOT EXISTS (SELECT 1
                                  FROM $schema.$tmp_table t
                                 WHERE t.wiki_id = dimension_wiki_images.wiki_id
                                   AND t.name    = dimension_wiki_images.name)",
            "INSERT IGNORE INTO dimension_wiki_images
             SELECT wiki_id,
                    name,
                    media_type
               FROM $schema.$tmp_table",
            'COMMIT'
        ] );
        unlink($tmp_file);
    }
}

1;
