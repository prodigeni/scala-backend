#!/usr/bin/env perl

use strict;
use warnings;

use FindBin qw/$Bin/;
use lib "$Bin/../../";

use Wikia::LB;
use Wikia::ExternalLB;

use Data::Dumper;

my $lb = Wikia::LB->instance;

#-- connection to firefly/master
my $dbh = $lb->getConnection( Wikia::LB::DB_MASTER, undef, "firefly" );
my $sth = $dbh->prepare( "SHOW TABLES LIKE 'c%'");
$sth->execute();
print "Tables in firefly:\n";
my $cnt = 1;
while( my $row = $sth->fetchrow ) {
	print qq{$cnt\t$row\n};
	$cnt++;
}
$sth->finish();

#-- connection to first cluster, stats group
$dbh = $lb->getConnection( Wikia::LB::DB_SLAVE, "stats", "dbstats" );
$sth = $dbh->prepare( "SHOW TABLES LIKE 'c%'");
$sth->execute();
print "Tables in firefly (stats):\n";
$cnt = 1;
while( my $row = $sth->fetchrow ) {
	print qq{$cnt\t$row\n};
	$cnt++;
}
$sth->finish();

#-- connection to second cluster, stats group
$dbh = $lb->getConnection( Wikia::LB::DB_SLAVE, "stats", "wikicities" );
$sth = $dbh->prepare( "SHOW TABLES LIKE 'c%'");
$sth->execute();
print "Tables in wikicities:\n";
$cnt = 1;
while( my $row = $sth->fetchrow ) {
	print qq{$cnt\t$row\n};
	$cnt++;
}
$sth->finish();


#-- connection to dataware/slave
$dbh = $lb->getConnection( Wikia::LB::DB_SLAVE, undef, "dataware" );
$sth = $dbh->prepare( "SHOW TABLES LIKE 'c%'");
$sth->execute();
print "Tables in dataware:\n";
$cnt = 1;
while( my $row = $sth->fetchrow ) {
	print qq{$cnt\t$row\n};
	$cnt++;
}
$sth->finish();

#
# connection to blobs table defines as archive1
#
$dbh = Wikia::ExternalLB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, "archive1" );
$sth = $dbh->prepare( "SHOW TABLES");
$sth->execute();
print "Tables in archive1:\n";
$cnt = 1;
while( my $row = $sth->fetchrow ) {
	print qq{$cnt\t$row\n};
	$cnt++;
}
$sth->finish();

#
# connection to blobs table defines as archive1
#
$dbh = Wikia::ExternalLB->instance->getConnection( Wikia::LB::DB_MASTER, undef, "archive1" );
$sth = $dbh->prepare( "SHOW TABLES");
$sth->execute();
print "Tables in archive1:\n";
$cnt = 1;
while( my $row = $sth->fetchrow ) {
	print qq{$cnt\t$row\n};
	$cnt++;
}
$sth->finish();


1;
