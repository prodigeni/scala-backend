#!/usr/bin/perl

use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use Wikia::Search::Indexer;
use Getopt::Long;
use Data::Dumper;

my $city_id              = 0;
my $page_id              = 0;
my $limit                = 0;
my $offset               = 0;
my $worker_id            = '0-0';
my $update_full_index_ts = 0;
my $docs_limit           = 250;
my $to_file              = '';

GetOptions(
    'city_id=i'            => \$city_id,
    'page_id=i'            => \$page_id,
    'limit=i'              => \$limit,
    'offset=i'             => \$offset,
    'worker_id=s'          => \$worker_id,
    'update_full_index_ts' => \$update_full_index_ts,
    'docs_limit=i'         => \$docs_limit,
    'to_file=s'            => \$to_file
);

my %params = (
	'limit'                => $limit,
	'offset'               => $offset,
	'worker_id'            => $worker_id,
	'page_id'              => $page_id,
	'max_index_limit'      => $docs_limit,
	'documents_path'       => $Bin.'/docs_indexed',
	'to_file'              => $to_file
); 

if( defined $city_id ) { 
	$params{"city_id"} = $city_id;
}
else {
	die "city_id required\n\n";
}

my $indexer = new Wikia::Search::Indexer(\%params);

$indexer->index_wiki( $update_full_index_ts );
