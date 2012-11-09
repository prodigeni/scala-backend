#!/usr/bin/perl -w

use strict;

use Try::Tiny;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";

use Fcntl ":flock";
use Getopt::Long;
use Sys::Hostname;
use Data::Dumper;

use Wikia::LB;
my ($from,$to) = (0, 100000000);
GetOptions('from=i'            => \$from,
                   'to=i'           => \$to,
                  );
;


my $dbe = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::EXTERNALSHARED );
#my $dbd = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::DATAWARESHARED );


my @wikis = get_list($dbe,qq(SELECT city_id, city_dbname, city_lang, city_cluster from city_list where city_public = 1 and city_id >= '$from' and city_id < '$to' order by city_id;));
my $conns = get_clusters($dbe);

foreach my $wiki (@wikis) {
	my $cluster = defined($wiki->{city_cluster}) ? $wiki->{city_cluster} : 'c1';
	printf "# %s %s %s %s\n", $wiki->{city_id}, $wiki->{city_dbname}, $wiki->{city_lang}, $cluster;
	my $dbr = $conns->{$cluster};
#	print "before connect\n";
#	try {
#		$dbr = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, $wiki->{city_dbname} );
#	} catch { next; };
#	print "after connect\n";
	try {
		my $stt = $dbr->prepare(qq(SELECT img_name FROM $wiki->{city_dbname}.image;));
		if ($stt->execute()) {
			while (my $r = $stt->fetchrow_hashref) {
				printf "%s/%s/%s\n", $wiki->{city_lang}, $wiki->{city_dbname}, $r->{img_name};
			}
		}
		$stt->finish;
		$dbr->disconnect;
	}
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

sub get_list {
        my ($db, $q) = (shift,shift);
        my $stt = $db->prepare($q);
        return unless $stt->execute();

        my @data = ();
        my $value;
        while (my $r = $stt->fetchrow_hashref) {
		push @data, $r;
        }
        $stt->finish();
        return @data;
}

sub get_cluster_conn {
	my ($db,$num) = (shift,shift);
	my $stt = $num == 1 
		? $db->prepare(qq(SELECT city_dbname FROM city_list WHERE city_public = 1 AND city_cluster IS NULL LIMIT 1;))
		: $db->prepare(qq(SELECT city_dbname FROM city_list WHERE city_public = 1 AND city_cluster = 'c$num' LIMIT 1;));
	if ($stt->execute()) {
		my $r = $stt->fetchrow_hashref;
		return Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, $r->{city_dbname} );
	}
	$stt->finish();
}

sub get_clusters {
	my $db = shift;
	my $conns = {};
	$conns->{c1} = get_cluster_conn($db,1);
	$conns->{c2} = get_cluster_conn($db,2);
	$conns->{c3} = get_cluster_conn($db,3);
	$conns->{c4} = get_cluster_conn($db,4);

	return $conns;
}
