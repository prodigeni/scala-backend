#!/usr/bin/perl

use strict;
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

use Getopt::Long;
use Data::Dumper;

my $oConf = new Wikia::Config( { logfile => "/tmp/invalid_log_rights.log" } );

#read long options

sub usage()
{
    my $name = "invalid_log_rights.pl"; 
    print "$name [--help] [--skip=db[,db2[,...]]] [--usedb=db[,db2[,...]]] [--all]\n\n";
    print "\thelp\t\t-\tprint this text\n";
    print "\tusedb=X\t\t-\tuse dbname\n";
}

my ( $help, $usedb, $limit ) = ();

GetOptions(	'help' => \$help, 'usedb=s' => \$usedb );

if ( (!$help) && (!$usedb) ) {
	print STDERR "Use option --help to know how to use script \n";
	exit;
}
my @where = ();
if ($help) { &usage(); exit; }

my $log = 1;
my $update = 1;

#----
my $lb = Wikia::LB->instance;
$lb->yml( $YML ) if $YML;
my $dbh = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED )} );
#----

my @where_db = ("city_public=1");
if ($usedb && $usedb ne '*') {
	my @use_dbs = split /,/,$usedb;
	push @where_db, "city_dbname in (".join(",", map { $dbh->quote($_) } @use_dbs).")";
}
my $whereclause = join(" and ", @where_db);

$oConf->log("get list of cities", $log);
my $dbList = $dbh->get_wikis(\@where_db);
my %databases = %{$dbList};
#----
# get data from databases
#----
$dbh->disconnect();

my $process_start_time = time();
my $main_loop = 0;
my %RESULTS = ();
foreach my $num (sort ( map { sprintf("%012u",$_) } (keys %databases) ))
{
	#--- set city;
	my $city_id = int $num;
	#--- set start time
	my $start_sec = time();
	$oConf->log ($databases{$city_id} . " processed (".$city_id.")", $log);
	my $dbr = $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', $databases{$city_id} );
	if ($dbr) 
	{
		my $q = "select count(*) as cnt from logging where log_namespace = 3 and log_type = 'rights'";
		my $sth_w = $dbr->prepare($q);
		my $cnt = 0;
		if($sth_w->execute()) {
			($cnt) = $sth_w->fetchrow_array();
		}
		
		if ( $cnt > 0 ) {
			print $databases{$city_id} . " => " . $cnt . "\n";
			if ( $update ) {
				my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, 'slave', $databases{$city_id} )} );
				if ($dbw) { 
					# update edits count
					my %data = ( "log_namespace" => 2 );
					my @conditions = (
						" log_namespace = 3 ", 
						" log_type = 'rights' "
					);
					my $res = $dbw->update( 'logging', \@conditions, \%data);
					$oConf->log("update $cnt records");
					$dbw->disconnect();
				}
			}
		}
	}
	my $end_sec = time();
	my @ts = gmtime($end_sec - $start_sec);
	$oConf->log ($databases{$city_id} . " processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]), $log);
	$main_loop++;
}

my $process_end_time = time();
my @ts = gmtime($process_end_time - $process_start_time);
$oConf->log ("\nscript processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]), $log);
$oConf->log ("done", $log);

1;
