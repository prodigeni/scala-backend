package Wikia::DW::ETL::Job::DimensionWikiImageCounts;

use strict;
use warnings;

use base qw( Wikia::DW::ETL::Job::Base );

use JSON;
use Wikia::DW::Common;

sub execute {
    my ($self, $job_config) = @_;
    my $args = decode_json $job_config->{args};

    my $sql = Wikia::DW::Common::load_query_file('dimension_wiki_image_counts', 'select');

    for my $k (keys %$args) {
        $sql =~ s/\[$k\]/$args->{$k}/ge;
    }    

    my $tmp_table = "dimension_wiki_image_counts_$args->{wiki_id}";
    my $tmp_file  = "/data/tmpdir/$tmp_table.csv";

    Wikia::DW::Common::query2csv($args->{dbname}, $sql, $tmp_file);

    if (-e $tmp_file) {
        Wikia::DW::Common::statsdb_do( [
            "LOAD DATA LOCAL INFILE '$tmp_file' REPLACE
                  INTO TABLE statsdb.dimension_wiki_image_counts
                FIELDS TERMINATED BY ','
            OPTIONALLY ENCLOSED BY '\"'
                 LINES TERMINATED BY '\\n' IGNORE 1 LINES",
            'COMMIT'
        ] );
        unlink($tmp_file);
    }
}

1;
