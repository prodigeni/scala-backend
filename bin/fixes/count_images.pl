#!/usr/bin/perl

my $YML = undef;
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

=global variables
=cut
my ($help, $cityid, $fromId, $toId, $startd, $endd) = ();
GetOptions(	'help' => \$help, 'cityid=s' => \$cityid, 'from=s' => \$fromId, 'to=s' => \$toId );

my $lb = Wikia::LB->instance;
$lb->yml( $YML ) if defined $YML;
my $dbr_ext = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::EXTERNALSHARED )} );

sub do_help()
{
    my $name = "remove_pages.pl"; 
    print <<EOF
$name [--help] [--usedb=db[,db2[,...]]] [--soft]

    help\t\t-\tprint this text
    cityid\t\t-\twikia ID;
    from\t\t-\tfrom Wikia ID
    to\t\t-\tto Wikia ID
EOF
;
}

sub make_last_weeks($) {
	
	my ( $x ) = @_;
	my $one_week = 31;
	#
	my $format = "%04d%02d%02d000000";
	my $ago_new = ($x * $one_week);
	my @ltime_prev = localtime(time - $ago_new * 24 * 60 * 60);
	my ($sec_prev, $min_prev, $hour_prev, $mday_prev, $mon_prev, $year_prev) = @ltime_prev;
	$mon_prev = 12 if ($mon_prev == 0);
	$year_prev = $year_prev - 1 if ($mon_prev == 0);
	my $prev_date = sprintf($format, $year_prev+1900, $mon_prev, $mday_prev);
	return $prev_date;
}

sub do_run(;$$$) {
	my ($cityid, $fromid, $toid) = @_;
	my $process_start_time = time();
	my %result = ();
	my @where_db = ('city_public = 1');
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
	my $sth = $dbr_ext->select_many("city_id, city_dbname", "`city_list`", \@where_db, \@options);
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

		my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, undef, $databases{$city_id} )} );
		@options = ();
		@where_db = ();
		%{$result{ $databases{$city_id} }} = (
			'id' => $city_id,
			'images' => 0,
			'last_week' => 0,
			'last_month' => 0
		);
		
		my $sth = $dbr->select_many("count(*) as cnt", "image", \@where_db, \@options);
		if ($sth) {
			if(my ($cnt) = $sth->fetchrow_array()) {
				$result{ $databases{$city_id} }{ 'images' } = $cnt;
			}
			$sth->finish();
		}
		
		@where_db = ( "img_timestamp >= " . make_last_weeks(4) );
		my $sth = $dbr->select_many("count(*) as cnt", "image", \@where_db, \@options);
		if ($sth) {
			if(my ($cnt) = $sth->fetchrow_array()) {
				$result{ $databases{$city_id} }{ 'last_month' } = $cnt;
			}
			$sth->finish();
		}
		
		@where_db = ( "img_timestamp >= " . make_last_weeks(1) );
		my $sth = $dbr->select_many("count(*) as cnt", "image", \@where_db, \@options);
		if ($sth) {
			if(my ($cnt) = $sth->fetchrow_array()) {
				$result{ $databases{$city_id} }{ 'last_week' } = $cnt;
			}
			$sth->finish();
		}

		my $end_sec = time();
		my @tsCity = gmtime($end_sec - $start_sec);
		say $databases{$city_id} . " processed ".sprintf ("%d hours %d minutes %d seconds\n",@tsCity[2,1,0]);
	}

	if ( scalar keys %result ) {
		open (CSV, '>/tmp/count_images.csv');
		foreach my $dbname ( keys %result ) {
			print CSV "$dbname;" . $result{$dbname}{id} .";" . $result{$dbname}{images} .";" . $result{$dbname}{last_week} . ";" . $result{$dbname}{last_month} . "\n";
		}
		close( CSV );
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
