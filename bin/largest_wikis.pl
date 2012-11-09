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

my $oConf = new Wikia::Config( { logfile => "/tmp/largest_wikis.log" } );

#read long options

sub usage()
{
    my $name = "largest_wikis.pl"; 
    print "$name [--help] [--skip=db[,db2[,...]]] [--usedb=db[,db2[,...]]] [--all]\n\n";
    print "\thelp\t\t-\tprint this text\n";
    print "\tletter=X\t\t-\tbegin from letter X\n";
    print "\tlimit=Y\t\t-\tlimit results to Y\n";
}

my ( $help, $letter, $limit ) = ();

GetOptions(	'help' => \$help, 'letter=s' => \$letter, 'limit=s' => \$limit );

if ( (!$help) && (!$letter) && (!$limit) ) {
	print STDERR "Use option --help to know how to use script \n";
	exit;
}
my @where = ();
if ($help) { &usage(); exit; }

#----
my $lb = Wikia::LB->instance;
$lb->yml( $YML ) if $YML;
my $dbh = $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED );
#----

my @where_db = ("city_public=1");
if ($letter) {
	push @where_db, "city_url like 'http://".$letter."%'";
}
my $whereclause = join(" and ", @where_db);

$oConf->log("get list of cities");
my $q = "select city_id, city_dbname from wikicities.city_list where city_public = 1 and $whereclause order by city_id";
my %databases = ();
my $sth = $dbh->prepare($q);
if($sth->execute()) {
    while(my ($city_id,$dbname) = $sth->fetchrow_array()) {
    	$databases{$city_id} = $dbname;
    }
} 
#----
# get data from databases
#----
$dbh->disconnect();

my $process_start_time = time();
my $main_loop = 0;
#-------------------------------------------------------------
# connect to sayid and start trunsaction 
# run dbh->ping every loop to check if the db is still running
#--------------------------------------------------------------
my %RESULTS = ();
foreach my $num (sort ( map { sprintf("%012u",$_) } (keys %databases) ))
{
	#--- set city;
	my $city_id = int $num;
	#--- set start time
	my $start_sec = time();
	$oConf->log ($databases{$city_id} . " processed (".$city_id.")");
	my $dbr = $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', $databases{$city_id} );
	if ($dbr) 
	{
		#check_lag($dbh);
		$q = "select count(*) as cnt from page";
		my $sth_w = $dbr->prepare($q);
		if($sth_w->execute()) 
		{
			if (my ($cnt) = $sth_w->fetchrow_array())
			{
				$RESULTS{$city_id} = $cnt;
			}
		}
	}
	my $end_sec = time();
	my @ts = gmtime($end_sec - $start_sec);
	$oConf->log ($databases{$city_id} . " processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]));
	$main_loop++;
}

my $i = 0;
foreach (sort { $RESULTS{$b} <=> $RESULTS{$a} || length($b) <=> length($a) || $a cmp $b } keys %RESULTS) {
        print $databases{$_}." ($_)\t".$RESULTS{$_}."\n";
        $i++;
        last if ( $i == $limit ) ;
}

my $process_end_time = time();
my @ts = gmtime($process_end_time - $process_start_time);
$oConf->log ("\nscript processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]));
$oConf->log ("done");

1;
