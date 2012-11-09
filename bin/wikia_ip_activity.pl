#!/usr/bin/perl

use strict;
use FindBin qw/$Bin/;
use lib "$Bin/../lib";

use DBI;
use Wikia::LB;
use Wikia::DB;
use Wikia::Config;

use Getopt::Long;
use Data::Dumper;

my $CREATE_TABLE = <<CREATE_TABLE;
CREATE TABLE `stats`.`city_ip_activity` (
  `ca_id` int(8) NOT NULL auto_increment,
  `ca_ip_text` varchar(255) character set latin1 collate latin1_bin NOT NULL default '',
  `ca_wikis_activity` text default '',
  `ca_latest_activity` text default '',
  PRIMARY KEY  (`ca_id`),
  KEY `ca_ip_text` (`ca_ip_text`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
CREATE_TABLE

my $oConf = new Wikia::Config( { logfile => "/tmp/wikia_ip_activity.log" } );

#read long options

sub usage()
{
    my $name = "wikia_ip_activity.pl";
    print "$name [--help] [--skip=db[,db2[,...]]] [--usedb=db[,db2[,...]]] [--all]\n\n";
    print "\thelp\t\t-\tprint this text\n";
    print "\tall\t\t-\tgenerate all reading stats\n";
    print "\tskip\t\t-\tcomma-separated list of dbs to skip\n";
    print "\tusedb\t\t-\tcomma-separated list of dbnames to use\n";
}

my ( $skip_dbs, $gen_all, $usedbs, $help ) = ();

GetOptions(	'help' => \$help, 'skip=s' => \$skip_dbs, 'usedb=s' => \$usedbs, 'all' => \$gen_all );

if ( (!$skip_dbs) && (!$gen_all) && (!$usedbs) && (!$help) ) {
	print STDERR "Use option --help to know how to use script \n";
	exit;
}
my @where = ();
if ($help) { &usage(); exit; }

#
# get admin connection to database
#
my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED, Wikia::LB::ADMIN );

my @where_db = ("city_public=1");
if ($skip_dbs) {
	my @skip_dbs = split /,/,$skip_dbs;
	push @where_db, "city_dbname not in (".join(",", map { $dbh->quote($_) } @skip_dbs).")";
}
if ($usedbs) {
	my @use_dbs = split /,/,$usedbs;
	push @where_db, "city_dbname in (".join(",", map { $dbh->quote($_) } @use_dbs).")";
}
my $whereclause = join(" and ", @where_db);

my $process_start_time = time();
my $main_loop = 0;

my %ACTIVE_USERS = ();

$oConf->log("get list of cities");
my $sth = $dbh->prepare("select city_id, city_dbname from wikicities.city_list where $whereclause order by city_id");
if ($sth->execute()) {
	while( my $row = $sth->fetchrow_hashref ) {
		my $start_sec = time();
		$oConf->log ($row->{'city_dbname'} . " processed (".$row->{'city_id'}.")");
		$dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, 'stats', $row->{'city_dbname'} );
		if ($dbh) {
			my $dbx = new Wikia::DB( {"dbh" => $dbh} );
			if ( $dbx->table_exists( 'page' ) ) {
				my $sth_w = $dbh->prepare("select rc_ip, count(0) as cnt, max(rc_timestamp) as max_time from recentchanges where rc_ip is not null group by rc_ip");
				if ( $sth_w->execute() ) {
					while(my ($rc_ip, $cnt, $max_time) = $sth_w->fetchrow_array()) {
						if ($rc_ip ne "") {
							%{$ACTIVE_USERS{$rc_ip}} = (
								"dbname" => "",
								"latest" => ""
							) unless ($ACTIVE_USERS{$rc_ip});
							$ACTIVE_USERS{$rc_ip}{"dbname"} .= $row->{'city_dbname'}."<CNT>".$cnt.",";
							$ACTIVE_USERS{$rc_ip}{"latest"} .= $row->{'city_dbname'}."<CNT>".$max_time.",";
						}
					}
					$sth_w->finish();
				}
			} else {
				$oConf->log( "Invalid Wikia: " . $row->{'city_id'} );
			}
		}
		my $end_sec = time();
		my @ts = gmtime($end_sec - $start_sec);
		$oConf->log ($row->{'city_dbname'} . " processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]));
		$main_loop++;

	}
	$sth->finish;
}


$oConf->log ("save data in database");
my $dbh_w = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS );
$dbh_w->do("truncate table `city_ip_activity`");
$dbh_w->do("optimize table `city_ip_activity`");
if( scalar(keys %ACTIVE_USERS) > 0 ) {
	foreach my $ip (sort keys %ACTIVE_USERS) {
		$dbh_w->do("insert into `city_ip_activity` (ca_id, ca_ip_text, ca_wikis_activity, ca_latest_activity) values (null, ".$dbh_w->quote($ip).", ".$dbh_w->quote($ACTIVE_USERS{$ip}{'dbname'}).", ".$dbh_w->quote($ACTIVE_USERS{$ip}{'latest'}).")");
	}
}

my $process_end_time = time();
my @ts = gmtime($process_end_time - $process_start_time);
$oConf->log ("\nscript processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]));
$oConf->log ("done");

1;
