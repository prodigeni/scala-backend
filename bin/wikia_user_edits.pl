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

my $CREATE_TABLE = <<CREATE_TABLE;
CREATE TABLE `stats`.`city_user_edits` (
  `ue_user_id` int(10) NOT NULL default '',
  `ue_user_text` varchar(255) character set latin1 collate latin1_bin NOT NULL default '',
  `ue_edit_namespace` int(11) NOT NULL default '',
  `ue_edit_count` int(11) NOT NULL default 0,
  PRIMARY KEY  (`ue_user_id`, `ue_edit_namespace`),
  KEY `ue_user_text` (`ue_user_text`),
  KEY `ue_edit_namespace` (`ue_edit_namespace`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
CREATE_TABLE

#read long options

my $oConf = new Wikia::Config( { logfile => "/tmp/wikia_user_edits.log" } );

sub usage()
{
    my $name = "wikia_user_edits.pl"; 
    print "$name [--help] [--skip=db[,db2[,...]]] [--usedb=db[,db2[,...]]] [--all]\n\n";
    print "\thelp\t\t-\tprint this text\n";
    print "\tall\t\t-\tgenerate all reading stats\n";
    print "\tskip\t\t-\tcomma-separated list of dbs to skip\n";
    print "\tusedb\t\t-\tcomma-separated list of dbnames to use\n";
}

my ($skip_dbs, $gen_all, $usedbs, $help) = ();

GetOptions(	'help' => \$help, 'skip=s' => \$skip_dbs, 'usedb=s' => \$usedbs, 'all' => \$gen_all );

if ( (!$skip_dbs) && (!$gen_all) && (!$usedbs) && (!$help) ) {
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

my @where_db = ("1=1");
if ($skip_dbs) {
	my @skip_dbs = split /,/,$skip_dbs;
	push @where_db, "city_dbname not in (".join(",", map { $dbh->quote($_) } @skip_dbs).")";
}
if ($usedbs) {
	my @use_dbs = split /,/,$usedbs;
	push @where_db, "city_dbname in (".join(",", map { $dbh->quote($_) } @use_dbs).")";
}
my $whereclause = join(" and ", @where_db);

$oConf->log("get list of cities", 1);
my $q = "select city_id, city_dbname from wikicities.city_list where city_public = 1 and city_useshared = 1 and $whereclause order by city_id";
my %databases = ();
my $sth = $dbh->prepare($q);
if($sth->execute()) {
    while(my ($city_id,$dbname) = $sth->fetchrow_array()) {
    	$databases{$city_id} = $dbname;
    }
}
#----
$dbh->disconnect();
#----

#----
# get data from databases
#----
my $process_start_time = time();
my $main_loop = 0;
#-------------------------------------------------------------
# connect to sayid and start trunsaction 
# run dbh->ping every loop to check if the db is still running
#--------------------------------------------------------------
my %ACTIVE_USERS = ();
foreach my $num (sort ( map { sprintf("%012u",$_) } (keys %databases) ))
{
	#--- set city;
	my $city_id = int $num;
	#--- set start time
	my $start_sec = time();
	$oConf->log ($databases{$city_id} . " processed (".$city_id.")", 1);
	#----
	$dbh = $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', $databases{$city_id} );
	#----
	if ($dbh) {
		$q = "select rev_user, rev_user_text, page_namespace from `".$databases{$city_id}."`.`revision`, `".$databases{$city_id}."`.`page` where page_id = rev_page and rev_user > 0 ";
		
		my $sth_w = $dbh->prepare($q);
		if($sth_w->execute()) {
			while(my ($rev_user, $user_name, $namespace) = $sth_w->fetchrow_array())
			{
				if ($user_name ne "") {
					%{$ACTIVE_USERS{$rev_user}} = ("user_name" => $user_name) unless ($ACTIVE_USERS{$rev_user});
					$ACTIVE_USERS{$rev_user}{$namespace} = 0 unless ($ACTIVE_USERS{$rev_user}{$namespace});
					$ACTIVE_USERS{$rev_user}{'user_name'} = $user_name;
					$ACTIVE_USERS{$rev_user}{$namespace} = $ACTIVE_USERS{$rev_user}{$namespace} + 1;
				}
			}
		}
		#---
		$dbh->disconnect();
	}
	my $end_sec = time();
	my @ts = gmtime($end_sec - $start_sec);
	$oConf->log ($databases{$city_id} . " processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]), 1);
	$main_loop++;
}
$oConf->log ("save data in database", 1);
my $dbh_w = $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS );
$dbh_w->do("truncate table stats.city_user_edits");
$dbh_w->do("optimize table stats.city_user_edits");
if (scalar(keys %ACTIVE_USERS) > 0) {
	my $dbh_wiki = $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::CENTRALSHARED );
	foreach my $userid (sort keys %ACTIVE_USERS) {
		#my $user_id = $ACTIVE_USERS{$username}{"user_name"};
		my $q = "select user_name from user where user_id = '".$userid."'";
		my $username = "";
		my $sth = $dbh_wiki->prepare($q);
		if ($sth->execute()) {
			($username) = $sth->fetchrow_array();
		}

		if ($ACTIVE_USERS{$userid} && $username) {
			foreach my $namespace (sort keys %{$ACTIVE_USERS{$userid}}) {
				if ($namespace ne "user_name") {
					$dbh_w->do("insert into stats.city_user_edits (ue_user_id, ue_user_text, ue_edit_namespace, ue_edit_count) values (".$dbh_w->quote($userid).", ".$dbh_w->quote($username).", ".$dbh_w->quote($namespace).", ".$ACTIVE_USERS{$userid}{$namespace}.")");
				}
			}
		}
	}
	$dbh_wiki->disconnect();
}
$dbh_w->disconnect();

my $process_end_time = time();
my @ts = gmtime($process_end_time - $process_start_time);
$oConf->log ("\nscript processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]), 1);
$oConf->log("done", 1);

1;
