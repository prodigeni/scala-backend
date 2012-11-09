#!/usr/bin/perl

my $YML = undef;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

BEGIN {
	$YML = "$Bin/../../../wikia-conf/DB.localhost.yml" if ($Bin =~ /moli/) ;
}

use DBI;
use Wikia::LB;
use Wikia::DB;
use Wikia::Config;
use Wikia::Utils;

use Getopt::Long;
use Data::Dumper;

#read long options
#$ENV{"WIKIA_DB_YML"} = "$Bin/../../../wikia-conf/DB.moli.yml";
#print $ENV{"WIKIA_DB_YML"} . " \n";

my $lb = Wikia::LB->instance;
$lb->yml( $YML ) if defined $YML;

my $to_file = 1;
sub usage() {
    my $name = "fixEventsLocalUsers.pl";
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

print "Daemon started ... \n";

my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS )} );
my $dbh = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED )} );
my $dbu = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, "stats", Wikia::LB::CENTRALSHARED )} );

print "Get list of wikis \n";

#my @where_db = ("city_public = 1");
my @where_db = (); #"city_public = 1", "city_url not like 'http://techteam-qa%'");
if ($skip_dbs) {
	my @skip_dbs = split /,/,$skip_dbs;
	push @where_db, "city_dbname not in (".join(",", map { $dbh->quote($_) } @skip_dbs).")";
}
if ($usedbs) {
	my @use_dbs = split /,/,$usedbs;
	push @where_db, "city_dbname in (".join(",", map { $dbh->quote($_) } @use_dbs).")";
}
if ( $fromid ) {
	push @where_db, "city_id >= " . $fromid;
}
if ( $toid ) {
	push @where_db, "city_id <= " . $toid;
}
my @fields = ('city_dbname', 'city_public', 'city_url');
my $dbList = $dbh->get_wikis_list(\@where_db, \@fields);
my %databases = %{$dbList};

#--------------
# get central user groups
#--------------
my $start_usr = time();
my @wgWikiaGlobalUserGroups = ('staff', 'helper', 'vstf');
my %CENTRAL_USER_GROUPS = ( 0 => 0 );
$where = [ "ug_user > 0", "ug_group in (".join(",", map { $dbh->quote($_) } @wgWikiaGlobalUserGroups).")" ];
$options = [ "group by ug_user" ];
my $sth_gu = $dbh->select_many("ug_user, group_concat(ug_group) as groups", "user_groups", $where, $options);
if($sth_gu) {
	while(my ($ug_user, $groups) = $sth_gu->fetchrow_array()) {
		$CENTRAL_USER_GROUPS{$ug_user} = $groups;
	}
	$sth_gu->finish();
}

#print Dumper(%CENTRAL_USER_GROUPS);

my $process_start_time = time();
my $main_loop = 0;
my @UNSHARED = ('staff','contractor','uncyclo','oblivion');
foreach my $num (sort ( map { sprintf("%012u",$_) } (keys %databases) ))
{
	#--- set city;
	my $city_id = int $num;
	#--- set start time
	my $start_sec = time();
	print $databases{$city_id}->{city_dbname} . " processed (".$city_id.") \n";
	my %ACTIVE_USERS = ();
	next if (grep /^\Q$databases{$city_id}->{city_dbname}\E$/,@UNSHARED);

	next if ( $databases{$city_id}->{city_public} == 0 );
	next if ( $databases{$city_id}->{city_url} =~ /techteam-qa/i );

	my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, "stats", $databases{$city_id}->{city_dbname} )} );
	if ($dbw) {
		#$dbw->check_lag(50);
		my $start_rev = time();
		my $where = [ "rev_user > 0", 'rev_timestamp < \'20101005090000\'' ];
		my $options = ['group by rev_user'];
		my $sth_w = $dbw->select_many("rev_user, max(date_format('%Y-%m-%d %H:%i:%s', rev_timestamp)), max(rev_id), count(*) as rev_cnt", "revision", $where, $options);
		if ($sth_w) {
			while(my ($rev_user, $max_rev_ts, $max_rev_id, $cnt) = $sth_w->fetchrow_array()) {
				if ($rev_user > 0) {
					%{$ACTIVE_USERS{$rev_user}} = (
						"user_name" => "",
						"lu_numgroups" => 0,
						"lu_singlegroup" => "",
						"lu_allgroups" => "",
						"lu_rev_cnt" => $cnt,
						"lu_blocked" => 0,
						"lu_last_rev" => $max_rev_id,
						"lu_last_ts" => $max_rev_ts,
						"lu_closed" => 0
					) unless ($ACTIVE_USERS{$rev_user});
				}
			}
			$sth_w->finish();
		}
		my $end_rev = time();
		my @ts_rev = gmtime($end_rev - $start_rev);
		print "\tget user's revisions - proceed: ".sprintf ("%d hours %d minutes %d seconds",@ts_rev[2,1,0]) . "\n";

		#--------------
		# get user groups
		#--------------
		my %USER_GROUPS = ( 0 => 0 );
		if ( scalar keys(%CENTRAL_USER_GROUPS) ) {
			foreach my $__uid (keys %CENTRAL_USER_GROUPS) {
				$USER_GROUPS{$__uid} = $CENTRAL_USER_GROUPS{$__uid};
			}
		}
		
		my $start_usr = time();
		$where = [ "ug_user > 0" ];
		$options = [ "group by ug_user" ];
		my $sth_g = $dbw->select_many("ug_user, group_concat(ug_group) as groups", "user_groups", $where, $options);
		if($sth_g) {
		    while(my ($ug_user, $groups) = $sth_g->fetchrow_array()) {
				if ( defined $USER_GROUPS{$ug_user} ) {
					my @skip_groups = split /,/,$groups;
					my @_tmp = ();
					do {
						my $t = $_; next if (grep /^\Q$t\E$/,@wgWikiaGlobalUserGroups);
						push @_tmp, $t;
					} foreach (@skip_groups);
					$USER_GROUPS{$ug_user} = $USER_GROUPS{$ug_user} . "," . join(',', @_tmp);
				} else {
					$USER_GROUPS{$ug_user} = $groups;
				}
		    }
			$sth_g->finish();
		}

		#---------------
		# get user
		#---------------
		$where = [ "user_id in (".join(",", keys %USER_GROUPS).") " ];
		$options = [];
		$dbu->ping();
		my $sth_u = $dbu->select_many("user_name, user_id, user_real_name", "user", $where, $options);
		if ($sth_u) {
			while(my ($user_name, $user_id, $user_real_name) = $sth_u->fetchrow_array()) {
				%{$ACTIVE_USERS{$user_id}} = (
					"user_name" => $user_name,
					"lu_numgroups" => 0,
					"lu_singlegroup" => "",
					"lu_allgroups" => "",
					"lu_rev_cnt" => 0,
					"lu_blocked" => 0,
					"lu_closed" => 0,
					"lu_last_rev" => 0,
					"lu_last_ts" => '0000-00-00 00:00:00'				
				) unless ( $ACTIVE_USERS{$user_id} );

				my @groups = ();
				if ( $USER_GROUPS{$user_id} ) {
					@groups = split(",", $USER_GROUPS{$user_id});
				}
				my $nbr_groups = scalar(@groups);

				if ($ACTIVE_USERS{$user_id}) {
					$ACTIVE_USERS{$user_id}{user_name} = $user_name;
					$ACTIVE_USERS{$user_id}{lu_numgroups} = $nbr_groups;
					$ACTIVE_USERS{$user_id}{lu_singlegroup} = ($nbr_groups > 0) ? $groups[$nbr_groups-1] : "";
					$ACTIVE_USERS{$user_id}{lu_allgroups} = join(";", @groups);

					if ($user_real_name eq "Account Disabled") {
						$ACTIVE_USERS{$user_id}{lu_closed} = 1;
					}
				}
			}
			$sth_u->finish();
		}
		my $end_usr = time();
		my @ts_usr = gmtime($end_usr - $start_usr);
		print "\tget user's groups (join) - proceed: ".sprintf ("%d hours %d minutes %d seconds",@ts_usr[2,1,0]) . " \n";

		#-----------------
		# get ipblocks
		#-----------------
		my $start_ipblck = time();
		$where = [
			"(ipb_deleted IS NULL OR ipb_deleted = 0)",
			"ipb_auto = 0"
		];
		$options = [];
		my $sth_i = $dbw->select_many("ipb_user", "ipblocks", $where, $options);
		if($sth_i) {
			while(my ($ipb_user) = $sth_i->fetchrow_array()) {
				if ($ACTIVE_USERS{$ipb_user}) {
					$ACTIVE_USERS{$ipb_user}{lu_blocked} = 1;
				}
			}
			$sth_i->finish();
		}
		my $end_ipblck = time();
		my @ts_ipblck = gmtime($end_ipblck - $start_ipblck);
		print "\tget user's ipblck - proceed: ".sprintf ("%d hours %d minutes %d seconds",@ts_ipblck[2,1,0]) . "\n";

		$dbw->disconnect() if ($dbw);
	}

	print "save data in table (events_local_users) : " . scalar(keys(%ACTIVE_USERS)). " users \n";
	if ($dbs) {
		if (scalar(keys %ACTIVE_USERS) > 0) {
			foreach my $us_id (sort keys %ACTIVE_USERS) {
				if (!$ACTIVE_USERS{$us_id}{user_name}) {
					$ACTIVE_USERS{$us_id}{user_name} = $dbu->get_user_by_id($us_id);
				}
				if (!$ACTIVE_USERS{$us_id}{user_name}) {
					$ACTIVE_USERS{$us_id}{user_name} = "";
				}

				my %data = (
					'wiki_id' 			=> Wikia::Utils->intval( $city_id ),
					'user_id'			=> Wikia::Utils->intval( $us_id ),
					'user_name'			=> $ACTIVE_USERS{$us_id}{user_name},
					'last_ip'			=> 0,
					'edits'				=> Wikia::Utils->intval( $ACTIVE_USERS{$us_id}{lu_rev_cnt} ),
					'editdate'			=> $ACTIVE_USERS{$us_id}{lu_last_ts},
					'last_revision'		=> $ACTIVE_USERS{$us_id}{lu_last_rev},
					'cnt_groups'		=> Wikia::Utils->intval( $ACTIVE_USERS{$us_id}{lu_numgroups} ),
					'single_group'		=> $ACTIVE_USERS{$us_id}{lu_singlegroup},
					'all_groups'		=> $ACTIVE_USERS{$us_id}{lu_allgroups},
					'user_is_blocked' 	=> Wikia::Utils->intval( $ACTIVE_USERS{$us_id}{lu_blocked} ),
					'user_is_closed'	=> Wikia::Utils->intval( $ACTIVE_USERS{$us_id}{lu_blocked} )
				);
				
				my %update = (
					'last_ip'			=> 'values(last_ip)',
					'edits' 			=> 'edits + values(edits)',
					'single_group'		=> 'values(single_group)',
					'all_groups'		=> 'values(all_groups)',				
					'user_is_blocked'	=> 'values(user_is_blocked)',
					'user_is_closed'	=> 'values(user_is_closed)'
				);	
				
				@options = ();
				my @duplicate = ();
				foreach my $key ( keys %update ) {
					push @duplicate, sprintf("%s = %s", $key, $update{$key});
				}
					
				push @options, ( " ON DUPLICATE KEY UPDATE " . join (",", @duplicate) );
						
				my $res = $dbs->insert( '`specials`.`events_local_users`', "", \%data, \@options, 1 );
			}
		}
	}
	undef(%ACTIVE_USERS);

	my $end_sec = time();
	my @ts = gmtime($end_sec - $start_sec);
	print $databases{$city_id}->{city_dbname} . " processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);

	sleep(1);
	$main_loop++;
}
#---
$dbu->disconnect() if ($dbu);
$dbs->disconnect() if ($dbs);
$dbh->disconnect() if ($dbh);

my $process_end_time = time();
@ts = gmtime($process_end_time - $process_start_time);
print "\nscript processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);
print "done \n";

1;
