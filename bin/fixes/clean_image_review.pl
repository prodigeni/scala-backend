#!/usr/bin/perl

use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use DBI;
use common::sense;
use Cwd;
use Getopt::Long;
use Data::Dumper;
use Time::Local ;
use Encode;
use POSIX qw(setsid uname);
use Date::Manip;

use Wikia::Utils;
use Wikia::WikiFactory;
use Wikia::DB;
use Wikia::LB;

GetOptions(	
	'help' 		=> \(my $help = 0), 
	'cityid=i' 	=> \(my $cityid = 0), 
	'from=i' 	=> \(my $fromId=0),
	'to=i' 		=> \(my $toId = 0) 
);

my $lb = Wikia::LB->instance;
my $dbr_ext = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::DATAWARESHARED )} );
my $dbw_ext = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::DATAWARESHARED )} );

sub do_help()
{
    my $name = "clean_image_review.pl"; 
    print <<EOF
$name [--help] [--cityid=ID] [--from=ID] [--to=ID]

    help\t\t-\tprint this text
    cityid\t\t-\twikia ID;
    from\t\t-\tfrom Wikia ID
    to\t\t-\tto Wikia ID
    
EOF
;
}

sub do_run(;$$$) {
	my ($cityid, $fromid, $toid) = @_;
	my $process_start_time = time();
	my @where_db = ();
	if ($cityid) {
		push @where_db, "city_id = ".$cityid;
	}
	if ($fromid) {
		push @where_db, "city_id >= ".$fromid;
	}
	if ($toid) {
		push @where_db, "city_id <= ".$toid;
	}
	
	say "get list of wikis from city list";
	my @options = ("order by city_id");
	my %databases = ();
	my $sth = $dbr_ext->select_many("city_id, city_dbname", "`archive`.`city_list`", \@where_db, \@options);
	if ($sth) {
		while(my ($city_id,$dbname) = $sth->fetchrow_array()) {
			$databases{$city_id} = $dbname;
		}
		$sth->finish();
	}
	
	my $main_loop = 0;
	foreach my $num (sort ( map { sprintf("%012u",$_) } (keys %databases) )) {
		#--- set city;
		my $city_id = int $num;
		#---
		my $start_sec = time();
		#---
		say $databases{$city_id} . " processed (".$city_id.") \n";

		my $wfactory = Wikia::WikiFactory->new( city_id => $city_id );
		#---
		next if defined $wfactory->city_dbname;

		my @where = ( 'wiki_id = ' . $city_id );
		say "Remove records from image_review table";
		$dbw_ext->delete( "`dataware`.`image_review`", \@where );
		say "Remove records from image_review_stats table";
		$dbw_ext->delete( "`dataware`.`image_review_stats`", \@where );
		say "Remove records from image_review_wikis table";
		$dbw_ext->delete( "`dataware`.`image_review_wikis`", \@where );
		
		my $end_sec = time();
		my @tsCity = gmtime($end_sec - $start_sec);
		say $databases{$city_id} . " processed ".sprintf ("%d hours %d minutes %d seconds\n",@tsCity[2,1,0]);
	}

	my $process_end_time = time();
	my @ts = gmtime($process_end_time - $process_start_time);
		
	say "Script processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);	
	return 1;
}

#############################################################################
################################   main   ###################################

if ($help) {
	do_help();
} else {
	do_run($cityid, $fromId, $toId);
}    
exit(0);
