#!/usr/bin/perl
package EventStats;

use strict;
use warnings;
use Data::Dumper;
use FindBin qw/$Bin/;
use lib "$Bin/../lib";

my $YML = undef;
$YML = "$Bin/../../wikia-conf/DB.moli.yml" if -e "$Bin/../../wikia-conf/DB.moli.yml" ;

use Wikia::Scribe;
use Wikia::Utils;
use Wikia::LB;
use Wikia::ExternalLB;
use Wikia::DB;
use Wikia::Settings;
use Wikia::User;

use Switch;
use Getopt::Long;
use Data::Dumper;
use base qw/Class::Accessor::Fast/;

$|++;
my $workers = 10;

#$ENV{"WIKIA_DB_YML"} = "$Bin/../../wikia-conf/DB.moli.yml";

my $user = new Wikia::User( db => "debakugan", id => 1198995 );
print Dumper($user);
print Dumper($user->groups);
exit;
my $res = undef;
if ( $user ) {

	my $exists = $user->user_exists_cluster( 'c3' );
	print "exists = $exists \n";
	if ( !$exists ) {
		print "exists = $exists \n";
		$user->copy_to_cluster( 'c3' );
	}	
	exit;

	my $cnt_groups = ( defined ( $user->groups ) ) ? scalar(@{$user->groups}) : 0;
	my %data = (
		"wiki_id"			=> Wikia::Utils->intval(177),
		"user_id"			=> Wikia::Utils->intval(115748),
		"user_name" 		=> $user->name,
		"edits"				=> ( '1' eq Wikia::Scribe::DELETE_CATEGORY ) ? -1 : 1,
		"-last_ip"			=> 'INET_ATON(\'192.169.0.1\')',
		"editdate"			=> '2010-09-24 03:00:00',
		"last_revision"		=> 1,
		"cnt_groups"		=> $cnt_groups,
		"single_group"		=> ( $cnt_groups > 0 ) ? $user->groups->[$cnt_groups-1] : '',
		"all_groups"			=> ( $cnt_groups > 0 ) ? join(";", @{$user->groups}) : '',
		"user_is_blocked"	=> $user->blocked,
		"user_is_closed"	=> $user->closed
	);
	
	my %update = (
		'edits' 			=> 'edits + values(edits)',
		'last_ip'			=> 'values(last_ip)',
		'editdate'			=> 'values(editdate)',
		'last_revision'		=> 'values(last_revision)',
		'cnt_groups'		=> 'values(cnt_groups)',
		'single_group'		=> 'values(single_group)',
		'all_groups'		=> 'values(all_groups)',
		'user_is_blocked'	=> 'values(user_is_blocked)',
		'user_is_closed'	=> 'values(user_is_closed)'
	);
	#$res = $user->_set_stats( \%data, \%update );

}

print "name = " . $user->name . "\n";
print "edits = " . $user->edits . "\n";
print "groups = " . Dumper($user->groups) . " \n";
print "options = " . Dumper($user->options) . " \n";
