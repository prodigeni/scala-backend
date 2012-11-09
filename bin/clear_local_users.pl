#!/usr/bin/perl

my $YML = undef;
use FindBin qw/$Bin/;
use lib "$Bin/../lib";

BEGIN {
	$YML = "$Bin/../../wikia-conf/DB.localhost.yml" if ($Bin =~ /moli/) ;
}

use DBI;
use Wikia::LB;
use Wikia::DB;
use Wikia::Config;
use Wikia::Utils;
use Wikia::WikiFactory;

use Getopt::Long;
use Data::Dumper;

#read long options

my $lb = Wikia::LB->instance;
$lb->yml( $YML ) if defined $YML;

my $to_file = 1;
sub usage() {
    my $name = "clear_local_users.pl";
    print "$name [--help] [--skip=db[,db2[,...]]] [--usedb=db[,db2[,...]]] [--all]\n\n";
    print "\thelp\t\t-\tprint this text\n";
    print "\tall\t\t-\tgenerate all reading stats\n";
    print "\tskip\t\t-\tcomma-separated list of dbs to skip\n";
    print "\tusedb\t\t-\tcomma-separated list of dbnames to use\n";
    print "\tfromid\t\t-\twikia ID \n";
    print "\ttoid\t\t-\tto wikia ID \n";
}

GetOptions(	'help' => \$help, 'skip=s' => \$skip_dbs, 'usedb=s' => \$usedbs, 'all' => \$gen_all, 'fromid=s' => \$fromid, 'toid=s' => \$toid );

if ( (!$skip_dbs) && (!$gen_all) && (!$usedbs) && (!$help) ) {
	print STDERR "Use option --help to know how to use script \n";
	exit;
}
my @where = ();
if ($help) {
	&usage(); exit;
}

my $oConf = new Wikia::Config( { logfile => "/tmp/clear_local_users.log" } );
$oConf->log ("Daemon started ...", $to_file);

my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::DATAWARESHARED )} );
my $dbl = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS )} );

$oConf->log ("Get list of wikis", $to_file);

#my @where_db = ("city_public = 1");
my @where_db = (); #"city_public = 1", "city_url not like 'http://techteam-qa%'");
if ($skip_dbs) {
	my @skip_dbs = split /,/,$skip_dbs;
	push @where_db, "city_dbname not in (".join(",", map { $dbs->quote($_) } @skip_dbs).")";
}
if ($usedbs) {
	my @use_dbs = split /,/,$usedbs;
	push @where_db, "city_dbname in (".join(",", map { $dbs->quote($_) } @use_dbs).")";
}
if ( $fromid ) {
	push @where_db, "city_id >= " . $fromid;
}
if ( $toid ) {
	push @where_db, "city_id <= " . $toid;
}
my @fields = ('city_dbname', 'city_public', 'city_url');
my @options = ("order by city_id");
my %databases = ();
my $f = join(",", @fields);
my $sth = $dbs->select_many("city_id, $f", "archive.city_list", \@where_db, \@options);
if ($sth) {
	while (my $row = $sth->fetchrow_hashref()) {
		$databases{$row->{city_id}} = $row;
	}
	$sth->finish();
}

my $process_start_time = time();
my $main_loop = 0;
foreach my $num (sort ( map { sprintf("%012u",$_) } (keys %databases) ))
{
	#--- set city;
	my $city_id = int $num;
	#--- set start time
	my $start_sec = time();
	$oConf->log ($databases{$city_id}->{city_dbname} . " processed (".$city_id.")", $to_file);

	my $wfactory = Wikia::WikiFactory->new( city_id => $city_id );

	next if ( defined( $wfactory ) && $wfactory->city_dbname );

	# delete previous
	my @conditions = ( 'wiki_id = ' . $city_id );
	my $res = $dbl->delete('specials.events_local_users', \@conditions);

	my $end_sec = time();
	my @ts = gmtime($end_sec - $start_sec);
	$oConf->log($databases{$city_id}->{city_dbname} . " processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]), $to_file);

	sleep(1);
	$main_loop++;
}
#---
$dbs->disconnect() if ($dbs);
$dbl->disconnect() if ($dbl);

my $process_end_time = time();
@ts = gmtime($process_end_time - $process_start_time);
$oConf->log ("\nscript processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]));
$oConf->log("done", $to_file);

1;
