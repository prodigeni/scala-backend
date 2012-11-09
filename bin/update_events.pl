#!/usr/bin/perl

use FindBin qw/$Bin/;
use lib "$Bin/../lib";

my $YML;
BEGIN {
	$YML = "$Bin/../../wikia-conf/DB.localhost.yml" if ($Bin =~ /moli/) ;
}

use DBI;

use Wikia::Config;
use Wikia::Utils;
use Wikia::DB;
use Wikia::LB;
use Wikia::ExternalLB;
use Wikia::Revision;

use Getopt::Long;
use Data::Dumper;
use CGI::Carp qw(fatalsToBrowser);
use Time::Local ;
use Time::HiRes qw(gettimeofday tv_interval);
use Storable qw(dclone);
use Encode;
use HTML::Entities;
use Compress::Zlib;

=script parameters
=cut
my ($help, $skip_dbs, $dbname, $month) = ();
GetOptions(
	'help' 		=> \$help, 
	'skip=s' 	=> \$skip_dbs,
	'usedb=s' 	=> \$dbname
);

if ( (!$skip_dbs) && (!$dbname) && (!$help) ) {
	print STDERR "Use option --help to know how to use script \n";
	exit;
}

sub usage {
    my $name = __FILE__; 
    print <<EOF
$name [--help] 

	help\t\t-\tprint this text
	skip\t\t-skip databases [skip=db1,db2...dbn]
	usedb\t\t-use databases [skip=db1,db2...dbn]
EOF
;
}


=show help
=cut
if ($help) { 
	&usage(); 
	exit; 
}

=log file
=cut
my $MQ_LOG = 1000;
my $log = 1;
$oConf = new Wikia::Config( { logfile => "/tmp/update_events.log" } );
$oConf->log ("Script started ...", $log);

=load balancer
=cut
my $lb = Wikia::LB->instance; $lb->yml( $YML ) if ($YML);

=db handlers
=cut
my $dbr = new Wikia::DB( 
	{
		"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED )
	} 
);
my $dbr_ext = new Wikia::DB( 
	{
		"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::DATAWARESHARED )
	} 
);
my $dbs = new Wikia::DB( 
	{
		"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::STATS )
	} 
);

my $dbr_local = "";
my @where_db = (
	"city_public = 1", 
	"city_url not like 'http://techteam-qa%'"
);
if ( $dbname && $dbname =~ /\+/ ) {
	$dbname =~ s/\+//i;
	push @where_db, "city_id > " . $dbname;
} elsif ( $dbname && $dbname ne "*" ) {
	my @use_dbs = split /,/,$dbname;
	push @where_db, "city_dbname in (".join(",", map { $dbr->quote($_) } @use_dbs).")";
} 
my $databases = $dbr->get_wikis(\@where_db, 'city_dbname');
foreach my $num (sort ( map { sprintf("%012u",$_) } ( keys %{$databases} ) ) ) {
	#--- set city;
	my $city_id = int $num;
	#--- set start time
	my $start_sec = time();
	#--- wikia object
	$oConf->log( sprintf( "Proceed %s (%d)", $databases->{$city_id}, $city_id ), $log );
	my $wikia = $dbr->id_to_wikia($city_id);
	#--- language id
	my $city_lang = $dbr->get_lang_by_code($wikia->{city_lang});
	#--- namespaces
	my $content_namespaces = $dbr->__content_namespaces($city_id);
	#--- category
	my $city_hub = $dbr->get_wiki_cat($city_id);
	#---
	my $db = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', $databases->{$city_id} )} );
	
	if ( $db ) {
		my @where = ("page_id = rev_page");
		my @options = ();

		if ( $month ) {
			push @where, " date_format(rev_timestamp, '%Y%m') = " . $dbr->quote($month);
		}
		
		my $sth = $db->select_many(
			" SQL_CALC_FOUND_ROWS /*! STRAIGHT_JOIN */ rev_user, rev_user_text, page_namespace, page_id, page_title, rev_timestamp, rev_id, page_is_redirect",
			"page FORCE INDEX (PRIMARY), revision r1 FORCE INDEX (PRIMARY)",
			\@where, 
			\@options
		);
		my $loop = 0;

		my @conditions = (); 
		my $oRow = $db->select("FOUND_ROWS() as cnt", '', \@conditions, \@options);
		my $records = $oRow->{cnt} || 0;
		$oConf->log( sprintf( "Found %d records", $records ), $log );

		my $start_ts = [gettimeofday()];
		if ($sth) {
			while(my ($user_id, $user_name, $page_namespace, $page_id, $page_title, $rev_timestamp, $rev_id, $page_is_redirect) = $sth->fetchrow_array()) {
				# is content?
				print "rev_id = $rev_id \n";
				my $is_content = ( grep /^\Q$page_namespace\E$/, @{$content_namespaces} );
				# get text - to check links, redirect etc 
				my $text = "";
				my $oRevision = new Wikia::Revision( { db => $databases->{$city_id}, id => $rev_id } );
				if ( $oRevision && $oRevision->{text} ) {
					$text = $oRevision->{text};
				}
				# is redirect 
				my $is_redirect = ($text =~ /^\#redirect/i) || Wikia::Utils->intval( $page_is_redirect );
				# text size
				my $size = 0; do { use bytes; $size = length($text); };
				# words
				my $words = $oRevision->count_words( $text );
				# links 
				my ($internallinks, $imagelinks, $videolinks) = $oRevision->parse_links( $wikia );
				# user ip
				my $ip = 0;
				$ip = $user_name if ( Wikia::Utils->is_ip( $user_name ) );
				# date timestamp 
				my $timestamp = Wikia::Utils->datetime_format($rev_timestamp);

				my %data = (
					"wiki_id"		=> Wikia::Utils->intval( $city_id ),
					"page_id"		=> Wikia::Utils->intval( $page_id ),
					"rev_id"		=> Wikia::Utils->intval( $rev_id ),
					"user_id"		=> Wikia::Utils->intval( $user_id ),
					"page_ns"		=> Wikia::Utils->intval( $page_namespace ),
					"is_content"	=> Wikia::Utils->intval( $is_content ),
					"is_redirect"	=> Wikia::Utils->intval( $is_redirect ),
					"-ip"			=> "INET_ATON('$ip')",
					"rev_timestamp"	=> $timestamp,
					"image_links"	=> Wikia::Utils->intval( $imagelinks ),
					"video_links"	=> Wikia::Utils->intval( $videolinks ),
					"total_words"	=> Wikia::Utils->intval( $words ),
					"wiki_lang_id"	=> Wikia::Utils->intval( $city_lang->{lang_id} ),
					"wiki_cat_id"	=> $city_hub,
					"text_size"		=> $size,
					"-event_date"	=> 'now()'
				);

				my $res = $dbs->insert( 'edit_event', "", \%data );

				if ( ( $loop > 0 ) &&  ( $loop % $MQ_LOG ) == 0 ) {
					$oConf->log( sprintf ( "speed: %0d req/%.3f seconds (loop: %d/%d)", $MQ_LOG, tv_interval($start_ts), $loop, $records ), $log );
					$start_ts = [gettimeofday()];
				}
				undef(%data); 
				$loop++;
			}
			$sth->finish();
			undef($sth);
		}
		$db->disconnect() if ($db);
	}
	
	undef($city_hub);
	undef($content_namespaces);
	undef($wikia);
	my $end_sec = time();
	my @ts = gmtime($end_sec - $start_sec);
	$oConf->log($databases->{$city_id} . " processed ".sprintf ("%d hours %d minutes %d seconds",@ts[2,1,0]), $log);
}
$dbr->disconnect() if ($dbr);

1;
