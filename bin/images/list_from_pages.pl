#!/usr/bin/perl -w

use strict;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";

use Fcntl ":flock";
use Getopt::Long;
use Sys::Hostname;

use Wikia::LB;


my $dbe = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::EXTERNALSHARED );
my $dbd = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::DATAWARESHARED );


my $wikis = get_dict($dbe,qq(SELECT city_id, city_dbname from city_list;),'city_id','city_dbname');

my $processed = 1;
my $lastCityId = 0;
my $lastPageId = 0;
while ($processed > 0) {
	my $stt = $dbd->prepare(qq(SELECT page_wikia_id, page_title from pages where page_namespace = 6 AND ((page_wikia_id = ? and page_id > ? ) or page_wikia_id > ? ) ORDER BY page_wikia_id, page_id LIMIT 1000;));
	$processed = 0;
	if ($stt->execute( $lastCityId, $lastPageId, $lastCityId )) {
		while (my $r = $stt->fetchrow_hashref) {
			printf "%s/%s\n", $wikis->{$r->{page_wikia_id}}, $r->{page_title};
			$lastCityId = $r->{page_wikia_id};
			$lastPageId = $r->{page_id};
			$processed++;
		}
	}
	$stt->finish();
}



sub get_dict {
        my ($db, $q, $key, $vkey) = (shift,shift,shift,shift);
        my $stt = $db->prepare($q);
        return unless $stt->execute();

        my $data = {};
        my $value;
        while (my $r = $stt->fetchrow_hashref) {
                $data->{$r->{$key}} = $vkey ? $r->{$vkey} : $r;
        }
        $stt->finish();
        return $data;
}

