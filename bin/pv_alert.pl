#!/usr/bin/perl

my $YML ;
use FindBin qw/$Bin/;
use lib "$Bin/../lib";

BEGIN {
	$YML = "$Bin/../../wikia-conf/DB.localhost.yml" if ($Bin =~ /moli/);
}

use DBI;
use Wikia::LB;
use Wikia::DB;
use Wikia::Config;
use Wikia::Utils;
use Wikia::Article;

use Getopt::Long;
use Data::Dumper;

use Config::Tiny;
use MIME::Lite;	

my $DEF_MIN_PV = 100;

sub check_pv($) {
	my ($min, $cityid, $dbname) = @_;

	my $lb = Wikia::LB->instance;
	$lb->yml( $YML ) if ($YML);
	my $msg = [];
	my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', $dbname )} );

	if ( $dbw ) {
		my $where = [
			"page_id = article_id",
			"article_id > 0",
			"prev_diff >= '".$min."'"
		];
		my $options = [];
		my $sth = $dbw->select_many("page_title, page_namespace, page_id, prev_diff", "page, page_visited", $where, $options);
		if ($sth) {
			while(my ($page_title, $page_namespace, $page_id, $prev_diff) = $sth->fetchrow_array()) {
				my $oArticle = new Wikia::Article( { 
					'wikia' => $cityid, 
					'title' => $page_title, 
					'ns' => $page_namespace } 
				);
				push @$msg, $oArticle->get_full_url() . " ($prev_diff)";
			}
			$sth->finish();
		}
		$dbw->disconnect() if ($dbw);
	}

	return $msg;
}

my $msg = "";
my $oConf = new Wikia::Config( { logfile => "/tmp/pv_alert.log" } );
$oConf->log ("Daemon started ...");

while (1) {
	my $Config = Config::Tiny->read( "$Bin/../config/pv_alert.cfg" );
	
	my $min_pv = $Config->{alert}->{min} || $DEF_MIN_PV;

	my $mailbody = "";
	if ( keys (%{$Config->{wikia}}) ) {
		foreach my $city_id ( keys %{$Config->{wikia}} ) {
			$oConf->log ("Check  " . $Config->{wikia}->{$city_id} . " ...");
			my $msg = &check_pv($min_pv, $city_id, $Config->{wikia}->{$city_id});
			if ( @$msg ) {
				$oConf->log( sprintf("%d pages found ... ", scalar @$msg) );
				$mailbody .= join("\n", @$msg) . "\n";
			}
		}
	}

	if ( $mailbody ) {
		if ( keys (%{$Config->{emails}}) ) {
			$mailbody = "Most visited pages in last " . $Config->{minutes}->{value} . " minutes: \n" . $mailbody;
			foreach my $nick ( keys %{$Config->{emails}} ) {
				$oConf->log( sprintf("Send email to %s ... ", $Config->{emails}->{$nick} ) );
    			my $msg = MIME::Lite->new(
         			To      => $Config->{emails}->{$nick},
         			Subject => 'PV alert',
         			Type    => 'text/plain',
					Data 	=> $mailbody
				);
				$msg->send("sendmail");
			}
		}
	}
	
	$oConf->log( sprintf("sleep %d seconds ... ", $Config->{minutes}->{value} * 60 ) );
	sleep($Config->{minutes}->{value} * 60);
}

$oConf->log ("Daemon finished ...");

1;
