package Wikia::DW::ETL::Job::WikiDBVideoViews;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Job::Base );

use JSON;
use Wikia::DW::Common;
use Wikia::DW::ETL::CSVWriter;
use Wikia::DW::ETL::Query;

sub execute {
    my ($self, $job_config) = @_;
    my $args = decode_json $job_config->{args};

    my $rnd       = substr(rand(), 2, 8);
    my $tmp_table = "tmp_video_views_$rnd";
    my $tmp_file  = "/data/tmpdir/wikidb_video_views.$args->{wiki_id}.$rnd.csv";

    Wikia::DW::Common::query2csv( 'statsdb',
                                  Wikia::DW::Common::load_query_file( 'wikidb_video_views',
                                                                      'select',
                                                                      { wiki_id => $args->{wiki_id} } ),
                                  $tmp_file );

    if (-e $tmp_file) {
        # Get connection to master
        my $master_db = Wikia::DW::ETL::Database->new( source => $args->{dbname}, master => 1 );

        $master_db->do( [
            "CREATE TEMPORARY TABLE IF NOT EXISTS `$args->{dbname}`.$tmp_table (
                 video_title VARCHAR(255) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT '',
                 views_30day INTEGER UNSIGNED DEFAULT '0',
                 views_total INTEGER UNSIGNED DEFAULT '0',
                 PRIMARY KEY (video_title)
             ) ENGINE=InnoDB",
            "DELETE FROM `$args->{dbname}`.$tmp_table",
            "LOAD DATA LOCAL INFILE '$tmp_file'
                  INTO TABLE `$args->{dbname}`.$tmp_table
                FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'
                 LINES TERMINATED BY '\\n'
                IGNORE 1 LINES
             (video_title, views_30day, views_total)",
            "UPDATE `$args->{dbname}`.video_info, `$args->{dbname}`.$tmp_table
                SET video_info.views_30day = $tmp_table.views_30day,
                    video_info.views_total = $tmp_table.views_total
              WHERE video_info.video_title = $tmp_table.video_title"
        ] );
        unlink($tmp_file);
    }
}

1;
