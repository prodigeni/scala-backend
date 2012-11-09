#!/usr/bin/perl

my $YML = undef;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

#print "ENV = " . $ENV{'DEVEL'} . "\n";

$YML = "$Bin/../../../wikia-conf/DB.localhost.yml" if ($ENV{'DEVEL'});

#print "YML = $YML \n";
use DBI;
use Wikia::LB;
use Wikia::DB;
use Wikia::Config;
use Wikia::Utils;
use Wikia::User;
use Wikia::Memcached;

use Getopt::Long;
use Data::Dumper;
use Data::Types qw(:all);

#read long options

my $lb = Wikia::LB->instance;
$lb->yml( $YML ) if defined $YML;

my $INSERTS = 250;
my $to_file = 1;
sub usage() {
    my $name = "events_local_users.pl";
    print "$name [--help] [--skip=db[,db2[,...]]] [--usedb=db[,db2[,...]]] [--all]\n\n";
    print "\thelp\t\t-\tprint this text\n";
    print "\tall\t\t-\tgenerate all reading stats\n";
    print "\tskip\t\t-\tcomma-separated list of dbs to skip\n";
    print "\tusedb\t\t-\tcomma-separated list of dbnames to use\n";
    print "\tfromid\t\t-\twikia ID \n";
    print "\ttoid\t\t-\tto wikia ID \n";
    print "\tremove_group\t\t-\tremove users with group X\n";
    print "\tuser\t\t-\tupdate user records\n";
}

GetOptions(	
	'help' => \$help, 
	'skip=s' => \$skip_dbs, 
	'usedb=s' => \$usedbs, 
	'all' => \$gen_all, 
	'fromid=s' => \$fromid, 
	'toid=s' => \$toid, 
	'remove_group=s' => \$remove_group,
	'user=i' => \$use_user
);

if ( ! ($gen_all || $usedbs || $help || $fromid) ) {
	print STDERR "Use option --help to know how to use script \n";
	exit;
}
my @where = ();
if ($help) {
	&usage(); exit;
}

my @columnsTable = ("wiki_id, user_id, user_name, last_ip, edits, editdate, last_revision, cnt_groups, single_group, all_groups, user_is_blocked, user_is_closed");

my $oConf = new Wikia::Config( { logfile => "/tmp/events_local_users.log" } );
$oConf->log ("Daemon started ...", $to_file);

#--------------
# remove user with group X
#--------------
sub removeUnusedGroup {
	my ( $dbl, $dbs, $group ) = @_;

	my $start_usr = time();
	my $where = [ "wiki_id > 0", "user_id > 0", "all_groups like '%" . $group . "%'" ];
	my $options = [ ];
	my $sth = $dbl->select_many("wiki_id, user_id", "specials.events_local_users", $where, $options);
	my $records = 0;
	if ( $sth ) {
		while(my ($wiki_id, $user_id) = $sth->fetchrow_array()) {
			my @conditions = ( 
				'wiki_id = ' . $wiki_id,
				'user_id = ' . $user_id
			);
			my $res = $dbs->delete('specials.events_local_users', \@conditions);
			$records++;
			if ( $records % 1000 == 0 ) {
				$oConf->log ("\tRemoved $records records", $to_file);
			}
		}
		$sth->finish();
	}
	my $end_usr = time();
	my @ts_rev = gmtime($end_usr - $start_usr);
	$oConf->log ("\tremoved $records records: ".sprintf ("%d hours %d minutes %d seconds",@ts_rev[2,1,0]), $to_file);		
}


#--------------
# get central user groups
#--------------
sub getCentralGroups {
	my ( $dbh, $wgWikiaGlobalUserGroups, $user ) = @_;

	my $oMemc = Wikia::Memcached->instance->memc();
	my $memkey = sprintf( "perl:local_users:central:groups:%0d", $user );
	
	my $data = $oMemc->get( $memkey );
	if ( UNIVERSAL::isa( $data, "HASH" ) ) {
		return $data;
	}
	
	my $start_cg = time();
	my %centralUserGroups = ( 0 => 0 );
	
	$where = [ "ug_user > 0", "ug_group in (".join(",", map { $dbh->quote($_) } @$wgWikiaGlobalUserGroups).")" ];
	if ( $user ) {
		push @$where, "ug_user = $user";
	}
	$options = [ "group by ug_user" ];
	my $sth_gu = $dbh->select_many("ug_user, group_concat(ug_group) as groups", "user_groups", $where, $options);
	if ( $sth_gu ) {
		while(my ($ug_user, $groups) = $sth_gu->fetchrow_array()) {
			$centralUserGroups{$ug_user} = $groups;
		}
		$sth_gu->finish();
		$oMemc->set( $memkey, \%centralUserGroups, 60*15 );
	}
	my $end_cg = time();
	my @ts_rev = gmtime($end_cg - $start_cg);
	$oConf->log ("\tuser central rights: ".sprintf ("%d hours %d minutes %d seconds",@ts_rev[2,1,0]), $to_file);
	
	return \%centralUserGroups;
}

#--------------
# get central user groups
#--------------
sub getCurrentLocalUsers {
	my ( $dbl, $wiki_id, $user ) = @_;

	my $oMemc = Wikia::Memcached->instance->memc();
	my $memkey = sprintf( "perl:local_users:user:wikia:%0d:%0d", $wiki_id, $user );
	my $data = $oMemc->get( $memkey );
	if ( UNIVERSAL::isa( $data, "HASH" ) ) {
		return $data;
	}

	my $start_usr = time();
	$where = [ "wiki_id = " . $dbl->quote($wiki_id) ];
	if ( $user ) {
		push @$where, "user_id = $user";
	}
	$options = [ ];
	my %currentUsers = ();
	my $sth = $dbl->select_many("user_id, user_name, edits, single_group, all_groups, user_is_blocked, user_is_closed, editdate, last_revision", "specials.events_local_users", $where, $options);
	if ( $sth ) {
		while(my ($user_id, $user_name, $edits, $single_group, $all_groups, $user_is_blocked, $user_is_closed, $editdate, $last_revision) = $sth->fetchrow_array()) {
			%{$currentUsers{$user_id}} = (
				'user_name'			=> $user_name,
				'edits' 			=> $edits,
				'single_group'		=> $single_group,
				'all_groups'		=> $all_groups,				
				'user_is_blocked'	=> $user_is_blocked,
				'user_is_closed'	=> $user_is_closed,
				'editdate'			=> $editdate,
				'last_revision'		=> $last_revision	
			);
		}
		$sth->finish();
		$oMemc->set( $memkey, \%currentUsers, 60*15 );
	}
	my $end_usr = time();
	my @ts_rev = gmtime($end_usr - $start_usr);
	$oConf->log ("\tcurrent local users: ".sprintf ("%d hours %d minutes %d seconds",@ts_rev[2,1,0]), $to_file);		
	
	return \%currentUsers;
}

sub getLocalUserGroups {
	my ( $dbw, $centralGroups, $wgWikiaGlobalUserGroups, $user ) = @_;

	#--------------
	# get user groups
	#--------------
	my %userGroups = ( 0 => 0 );
	if ( scalar keys %$centralGroups ) {
		foreach my $user_id (keys %$centralGroups) {
			$userGroups{$user_id} = $centralGroups->{$user_id};
		}
	}
	
	my $start_usr = time();
	$where = [ "ug_user > 0" ];
	if ( $user ) {
		push @$where, "ug_user = $user";
	}
	$options = [ "group by ug_user" ];
	my $sth_g = $dbw->select_many("ug_user, group_concat(ug_group) as groups", "user_groups", $where, $options);
	if($sth_g) {
		while(my ($ug_user, $groups) = $sth_g->fetchrow_array()) {
			if ( defined $userGroups{$ug_user} ) {
				my @skip_groups = split /,/,$groups;
				my @ugroups = split /,/, $userGroups{$ug_user};
				my @_tmp = ();
				do {
					my $t = $_; 
					next if (grep /^\Q$t\E$/,@$wgWikiaGlobalUserGroups);
					next if (grep /^\Q$t\E$/,@ugroups);
					push @_tmp, $t;
				} foreach (@skip_groups);
				$userGroups{$ug_user} = $userGroups{$ug_user} . "," . join(',', @_tmp);
			} else {
				$userGroups{$ug_user} = $groups;
			}
		}
		$sth_g->finish();
	}
	
	my $end_usr = time();
	my @ts_rev = gmtime($end_usr - $start_usr);
	$oConf->log ("\tlocal user groups: ".sprintf ("%d hours %d minutes %d seconds",@ts_rev[2,1,0]), $to_file);		
	
	return \%userGroups;
}

sub getUserEdits {
	my ( $dbw, $user, $wikia ) = @_;

	my $start_rev = time();
	
	my $oMemc = Wikia::Memcached->instance->memc();
	my $memkey = sprintf( "perl:local_users:user:edits:%s:%0d", $wikia, $user );
	my $data = $oMemc->get( $memkey );
	if ( UNIVERSAL::isa( $data, "HASH" ) ) {
		return $data;
	}
		
	my $where = [ "rev_user > 0" ];
	if ( $user ) {
		push @$where, "rev_user = $user";
	}
	my $options = ['group by rev_user'];
	my $users = {};
	my $sth_w = $dbw->select_many("rev_user, rev_user_text, count(*) as rev_cnt, max(date_format(rev_timestamp, '%Y-%m-%d %H:%i:%s')) as max_ts, max(rev_id) as rid", "revision", $where, $options);
	if ($sth_w) {
		while(my ($rev_user, $rev_user_text, $cnt, $max_ts, $rid) = $sth_w->fetchrow_array()) {
			if ($rev_user > 0) {
				$users->{$rev_user} = {
					"user_name" => $rev_user_text,
					"last_ip" => 0,
					"editdate" => $max_ts,
					"last_revision" => $rid,
					"cnt_groups" => 0,
					"single_group" => "",
					"all_groups" => "",
					"edits" => $cnt,
					"user_is_blocked" => 0,
					"user_is_closed" => 0
				} unless ( $users->{$rev_user} );
			}
		}
		$sth_w->finish();
		$oMemc->set( $memkey, $users, 60*15 );
	}
	my $end_rev = time();
	my @ts_rev = gmtime($end_rev - $start_rev);
	$oConf->log ("\trevisions: ".sprintf ("%d hours %d minutes %d seconds",@ts_rev[2,1,0]), $to_file);
	
	return $users;
}

sub mergeUserGroupsWithEdits {
	my ( $dbu, $userGroups, $users ) = @_;

	my $start_usr = time();
	$where = [ "user_id in (".join(",", keys %$userGroups).") " ];
	$options = [];
	$dbu->ping();
	my $sth_u = $dbu->select_many("user_name, user_id, user_real_name", "user", $where, $options);
	if ($sth_u) {
		while(my ($user_name, $user_id, $user_real_name) = $sth_u->fetchrow_array()) {
			$users->{$user_id} = {
				"user_name" => $user_name,
				"last_ip" => 0,
				"editdate" => '0000-00-00 00:00:00',
				"last_revision" => 0,
				"cnt_groups" => 0,
				"single_group" => "",
				"all_groups" => "",
				"edits" => 0,
				"user_is_blocked" => 0,
				"user_is_closed" => 0
			} unless ( $users->{$user_id} );

			my @groups = ();
			if ( $userGroups->{$user_id} ) {
				@groups = split( ",", $userGroups->{$user_id} );
			}
			my $nbr_groups = scalar(@groups);

			if ($users->{$user_id}) {
				$users->{$user_id}->{user_name} = $user_name;
				$users->{$user_id}->{cnt_groups} = $nbr_groups;
				$users->{$user_id}->{single_group} = ($nbr_groups > 0) ? $groups[$nbr_groups-1] : "";
				$users->{$user_id}->{all_groups} = join(";", @groups);

				if ($user_real_name eq "Account Disabled") {
					$users->{$user_id}->{user_is_closed} = 1;
				}
			}
		}
		$sth_u->finish();
	}
	
	# check user groups 
	if ( scalar keys %{$userGroups} ) {
		foreach my $user_id ( keys %{$userGroups} ) {
			if ( !defined $users->{$user_id} ) {
				my @groups = split( ",", $userGroups->{$user_id} );
				my $nbr_groups = scalar(@groups);	
				
				my $oUser = new Wikia::User( db => "wikicities", id => $user_id );
								
				if ( $oUser ) {
					$users->{$user_id} = {
						"user_name" => $oUser->name,
						"last_ip" => 0,
						"editdate" => '0000-00-00 00:00:00',
						"last_revision" => 0,
						"cnt_groups" => $nbr_groups,
						"single_group" => ($nbr_groups > 0) ? $groups[$nbr_groups-1] : "",
						"all_groups" => join(";", @groups),
						"edits" => 0,
						"user_is_blocked" => 0,
						"user_is_closed" => 0
					} 
				}
			}		
		}
	}
	
	my $end_usr = time();
	my @ts_usr = gmtime($end_usr - $start_usr);
	$oConf->log ("\tmerge users & groups: ".sprintf ("%d hours %d minutes %d seconds",@ts_usr[2,1,0]), $to_file);
	
	return $users;
}

sub getUserBlocks {
	my ( $dbw, $users, $user ) = @_;

	my $start_ipblck = time();
	$where = [
		"(ipb_deleted IS NULL OR ipb_deleted = 0)",
		"ipb_auto = 0"
	];
	if ( $user ) {
		push @$where, "ipb_user = $user";
	}
	$options = [];
	my $sth_i = $dbw->select_many("ipb_user", "ipblocks", $where, $options);
	if($sth_i) {
		while(my ($user_id) = $sth_i->fetchrow_array()) {
			if ($users->{$user_id}) {
				$users->{$user_id}->{user_is_blocked} = 1;
			}
		}
		$sth_i->finish();
	}
	my $end_ipblck = time();
	my @ts_ipblck = gmtime($end_ipblck - $start_ipblck);
	$oConf->log ("\tblocks: ".sprintf ("%d hours %d minutes %d seconds",@ts_ipblck[2,1,0]), $to_file);
	
	return $users;
}

sub shouldAddUser {
	my ( $update, $data, $currentUser ) = @_;
	
	my $result = 0;
	if ( !defined $currentUser ) {
		# user doesn't exist
		$result = 1;
	} elsif ( scalar keys %$update && scalar keys %$currentUser ) {
		foreach my $inx ( keys %$update ) {
			if ( is_int( $data->{$inx} ) && is_int( $currentUser->{$inx} ) ) {
				if ( $data->{$inx} != $currentUser->{$inx} ) {
					$result = 1;
					last;
				}				
			} else {
				if ( $data->{$inx} ne $currentUser->{$inx} ) {
					$result = 1;
					last;
				}
			}
		}
	}
	
	return $result;
}

my $dbl = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::STATS )} );
my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS )} );
my $dbh = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'cron', Wikia::LB::EXTERNALSHARED )} );
my $dbu = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, "cron", Wikia::LB::CENTRALSHARED )} );

my $process_start_time = time();
	
if ( $remove_group ) {
	$oConf->log("Remove users for group: $remove_group", $to_file);	
	removeUnusedGroup ( $dbl, $dbs, $remove_group);
} else {
	$oConf->log("Get global user groups", $to_file);	

	# central groups
	my $wgWikiaGlobalUserGroups = Wikia::User->_global_groups();
	my $globalUserGroups = getCentralGroups($dbh, $wgWikiaGlobalUserGroups, $use_user);

	$oConf->log ("Get list of wikis", $to_file);

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
	my $databases = $dbh->get_wikis_list(\@where_db, \@fields);

	my $main_loop = 0;
	my @UNSHARED = ('staff','contractor','uncyclo','oblivion');
	foreach my $city_id (sort keys %$databases) {
		my $city_info = $databases->{$city_id};

		#--- set start time
		my $start_sec = time();
		$oConf->log ($city_info->{city_dbname} . " processed (".$city_id.")", $to_file);

		next if (grep /^\Q$city_info->{city_dbname}\E$/,@UNSHARED);

		# delete previous
		if ( $city_info->{city_public} == 0 ) {
			my @conditions = ( 'wiki_id = ' . $city_id );
			my $res = $dbs->delete('specials.events_local_users', \@conditions);
		}
		next if ( $city_info->{city_url} =~ /techteam-qa/i );

		# db connection
		my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, "cron", $city_info->{city_dbname} )} );

		# current list of users
		my $currentUsers = getCurrentLocalUsers($dbl, $city_id, $use_user);

		# local user groups 
		my $userGroups = getLocalUserGroups($dbw, $globalUserGroups, $wgWikiaGlobalUserGroups, $use_user);
		
		# revision 
		my $users = getUserEdits( $dbw, $use_user, $city_info->{city_dbname} );

		# merge edits with groups
		$users = mergeUserGroupsWithEdits( $dbu, $userGroups, $users );

		# check user blocks
		$users = getUserBlocks($dbw, $users, $use_user);
		
		$dbw->disconnect() if ($dbw);

		$oConf->log ("prepare sql inserts : " . scalar(keys(%$users)). " users", $to_file);

		my $insertKeys = ();
		my $index = 0;
		my $added = 0;
		
		my %update = (
			'user_name'			=> 'values(user_name)',
			'edits' 			=> 'values(edits)',
			'single_group'		=> 'values(single_group)',
			'all_groups'		=> 'values(all_groups)',				
			'user_is_blocked'	=> 'values(user_is_blocked)',
			'user_is_closed'	=> 'values(user_is_closed)',
			'editdate'			=> 'values(editdate)',
			'last_revision'		=> 'values(last_revision)',
		);	
		if (scalar(keys %$users) > 0) {
			my $loop = 0;
			foreach my $us_id (sort keys %$users) {
				if (!$users->{$us_id}->{user_name}) {
					$users->{$us_id}->{user_name} = $dbu->get_user_by_id($us_id);
				}
				if (!$users->{$us_id}->{user_name}) {
					$users->{$us_id}->{user_name} = "";
				}

				my %data = (
					"last_ip" 			=> 0,
					"wiki_id"			=> Wikia::Utils->intval( $city_id ),
					"user_id"			=> Wikia::Utils->intval( $us_id ),
					"user_name"			=> $users->{$us_id}->{user_name},
					"cnt_groups"		=> Wikia::Utils->intval( $users->{$us_id}->{cnt_groups} ),
					"single_group"		=> $users->{$us_id}->{single_group},
					"all_groups"		=> $users->{$us_id}->{all_groups},
					"edits"				=> Wikia::Utils->intval( $users->{$us_id}->{edits} ),
					"editdate"			=> $users->{$us_id}->{editdate},
					"last_revision" 	=> Wikia::Utils->intval( $users->{$us_id}->{last_revision} ),
					"user_is_blocked"	=> Wikia::Utils->intval( $users->{$us_id}->{user_is_blocked} ),
					"user_is_closed"	=> Wikia::Utils->intval( $users->{$us_id}->{user_is_closed} )
				);
				
				if ( $loop == 0 ) {
					$insertKeys = join(',', keys %data);
				}
									
				$index++ if ( ( $loop > 0 ) && ( $loop % $INSERTS == 0 ) ) ;
					
				if ( shouldAddUser(\%update, \%data, $currentUsers->{$us_id} ) ) {
					my @conditions = ( 'wiki_id = ' . $city_id, 'user_id = ' . $us_id );
					my $res = $dbs->delete('specials.events_local_users', \@conditions);
				
					my $sql = "INSERT IGNORE INTO specials.events_local_users ( $insertKeys ) VALUES ( " . join(",", map { $dbs->quote($_) } values %data) . " )";
					$dbs->execute($sql);
					$loop++;
					$added++;
				}
				
				delete $currentUsers->{$us_id};
			}
		}
		$oConf->log ("updated:  " . $added. " records", $to_file);

		$oConf->log ("delete " . scalar keys (%$currentUsers). " invalid users", $to_file);
		# remove inactive users;
		if ( scalar keys %$currentUsers ) {
			foreach my $user_id ( keys %$currentUsers ) {
				my @conditions = ( 'wiki_id = ' . $city_id, 'user_id = ' . $user_id );
				my $res = $dbs->delete('specials.events_local_users', \@conditions);
			}
		}
		undef($users);
		undef(%$currentUsers);

		my $end_sec = time();
		my @ts = gmtime($end_sec - $start_sec);
		$oConf->log($city_info->{city_dbname} . " processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]), $to_file);

		$main_loop++;
	}
}
#---
$dbu->disconnect() if ($dbu);
$dbl->disconnect() if ($dbl);
$dbs->disconnect() if ($dbs);
$dbh->disconnect() if ($dbh);

my $process_end_time = time();
@ts = gmtime($process_end_time - $process_start_time);
$oConf->log ("\nscript processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]), $to_file);
$oConf->log("done", $to_file);

1;
