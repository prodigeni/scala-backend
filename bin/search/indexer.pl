#!/usr/bin/perl

use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use common::sense;
use Wikia::Search::Indexer;
use Getopt::Long;
use Data::Dumper;
use Thread::Pool::Simple;
use constant MIN_PAGES_PER_WORKER => 250;

my $city_limit = 0;
my $workers    = 20;

GetOptions(
    'city_id=i'    => \my $city_id,
    'city_limit=i' => \$city_limit,
    'workers=i'    => \$workers
);

my %params = (
	'limit'   => $city_limit
); 

my $pool = Thread::Pool::Simple->new(
	min => 10,
	max => $workers,
	load => 1,
	do => [sub {
		worker( @_ );
	}],
	monitor => sub {
		say "done";
	},
	passid => 1,
);

sub worker {
	my( $worker_id, $worker_index, $city_id, $offset, $limit, $is_last ) = @_;

	my $worker_key = $city_id . "-" . $worker_index;

	print( "[Worker:" . $worker_key . ", CityID=" . $city_id . "] -- STARTED -- (LIMIT " .$offset . ", " . $limit . ")\n" );

	my $cmd = '';
	if( $is_last ) {
		$cmd = qq(/usr/bin/perl /usr/wikia/source/backend/bin/search/indexer-worker.pl --city_id=$city_id --offset=$offset --limit=$limit --worker_id=$worker_key);
	}
	else {
		$cmd = qq(/usr/bin/perl /usr/wikia/source/backend/bin/search/indexer-worker.pl --city_id=$city_id --offset=$offset --limit=$limit --worker_id=$worker_key --update_full_index_ts);
	}

	if( system( $cmd ) != 0) {
		say "Failed to run $cmd";
		return 0;
	}

	return 1;
}

my $indexer = new Wikia::Search::Indexer(\%params);

my $wikis;

if( defined $city_id ) { 
	my %wikis_hash = (
		$city_id => { 'full_index_ts' => 0 }
	);
	$wikis = \%wikis_hash;
}
else {
	$wikis = $indexer->get_wikis();
}

foreach (sort ( map { sprintf("%012u",$_) } (keys %$wikis ) )) {
	my $city_id = int $_;

	print "city_id=".$city_id." ";

	my $wiki_indexer = new Wikia::Search::Indexer({ 'city_id' => $city_id });
	my $wiki_pages_num = $wiki_indexer->get_wiki_pages_num();

	if( $wiki_pages_num == 0 ) {
		# wiki db doesn't exists! mark as "indexed" anyway
		$wiki_indexer->update_wiki_full_index_ts();
		next;
	}

	my $pages_per_worker = $wiki_pages_num;
	if( $pages_per_worker > MIN_PAGES_PER_WORKER ) {
		$pages_per_worker = sprintf('%d', ( $wiki_pages_num / $workers ) );
	}

	if( defined $wiki_indexer->city_url ) {
		print "pages=" . $wiki_pages_num . " (per worker: $pages_per_worker)\n";

		my $offset = 0;
		for( my $i = 0; $i <= $workers; $i++ ) {
			$pool->add( $i, $city_id, $offset, $pages_per_worker, ( ( $offset+$pages_per_worker ) > $wiki_pages_num ) );

			$offset += $pages_per_worker;

			if( $offset >= $wiki_pages_num ) { last };
		}
		
	}
	else {
		print "wiki doesn't exists!\n";
	}

	undef $wiki_indexer;
}

$pool->join;

1;

