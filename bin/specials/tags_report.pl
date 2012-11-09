#!/usr/bin/perl -w

#
# options
#
use common::sense;
use feature "say";

use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";

#
# private
#
use Wikia::DB;
use Wikia::Settings;
use Wikia::WikiFactory;
use Wikia::Utils;
use Wikia::LB;
use Wikia::Revision;

#
# public
#
use MediaWiki::API;
use Pod::Usage;
use Getopt::Long;
use Thread::Pool::Simple;
use Time::HiRes qw(gettimeofday tv_interval);
use Try::Tiny;
use List::Util qw(shuffle);

package main;

my %TAGS = (
	"dpl" => "\{\{\#dpl|\<dpl\>|\{\{#dplchapter|\<dynamicpagelist\>",
	"youtube" => "\<youtube(.*?)\>|\<gvideo(.*?)\>|\<aovideo(.*?)\>|\<aoaudio(.*?)\>|\<wegame(.*?)\>|\<tangler\>|\<gtrailer\>|\<nicovideo\>|\<ggtube\>",
	"inputbox" => "\<inputbox(.*?)\>",
	"widget" => "\<widget(.*?)\>",
	"googlemap" => "\<googlemap(.*?)\>",
	"imagemap" => "\<imagemap(.*?)\>",
	"poll" => "\<poll(.*?)\>",
	"rss" => "\<rss(.*?)\>",
	"math" => "\<math\>",
	"bloglist" => "\<bloglist(.*?)\>",
	"googlespreadsheet" => "\<googlespreadsheet(.*?)\>",
	"categorytree" => "\<categorytree(.*?)\>",
	"chem" => "\<chem\>",
	"chess" => "\<chess\>",
	"choose" => "\<choose(.*?)\>",
	"listpages" => "\<listpages(.*?)\>",
	"poem" => "\<poem(.*?)\>",
	"tabview" => "\<tabview(.*?)\>",
	"timeline" => "\<timeline(.*?)\>",
	"gallery" => "\<gallery(.*?)\>",
	"slideshow" => "\<gallery(.*?)type=([\" ]*)slideshow([\" ]+.*\>|\>)",
	"slider" => "\<gallery(.*?)type=([\" ]*)slider([\" ]+.*\>|\>)",
	"badge" => "\<badge(.*?)\>",
	"imagelink" => "\<imagelink(.*?)\>",
	"ppch" => "\<ppch\>",
	"tex" => "\<batik\>|\<feyn\>|\<go\>|\<greek\>|\<graph\>|\<ling\>|\<music\>|\<plot\>|\<schem\>|\<teng\>|\<tipa\>",
	"linkedimage" => "\<linkedimage\>",
	"createbox" => "\<createbox(.*?)\>",
	"source" => "\<source(.*?)\>",
	"videogallery" => "\<videogallery(.*?)\>",
	"fb_like" => "\<fb:like-box(.*?)\>",
	"fb_stream" => "\<fb:live-stream(.*?)\>",
	"polldaddy" => "\<polldaddy(.*?)\>",
	"watch" => "\<watch(.*?)\>|\{\\watch",
	"ask" => "\{\{#ask",
	"mainpage" => "<mainpage-leftcolumn-start(.*?)\>|<mainpage-endcolumn(.*?)\>|<mainpage-rightcolumn-start(.*?)\>",
);

my @TAGSLIST = ();
foreach my $key (keys %TAGS) {
	push @TAGSLIST, "(" . $TAGS{$key} . ")" if ($TAGS{$key});
}
my $tagsList = join("|", @TAGSLIST);
	
my $lb = Wikia::LB->instance;

sub worker {
	my( $worker_id, $city_id, $city_dbname ) = @_;

	my $cmd = qq(/usr/bin/perl /usr/wikia/source/backend/bin/specials/tags_report.pl --execute=$city_dbname --wiki=$city_id );
	my $result = 1;
	if (system($cmd) != 0) {
		say "Failed to run $cmd";
		$result = 0;
	}
	return $result;
}

sub make_job {
	my ( $city_id, $dbname, $page_id, $rev_id, $page_namespace ) = @_;
	
	my $dbw = $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS );		
	
	$dbw->do( "delete from `city_used_tags` where ct_wikia_id = " . $dbw->quote($city_id) . " and ct_page_id = " . $dbw->quote($page_id) );
	my $oRevision = new Wikia::Revision( { db => $dbname, id => $rev_id } );
	if ( $oRevision && defined($oRevision->text) ) {
		my $rev_text = $oRevision->text;
		$rev_text =~ s/<!(?:--(?:[^-]*|-[^-]+)*--\s*)>//g;
		if ($rev_text) {
			if ($rev_text =~ /$tagsList/i) {
				foreach my $key (keys %TAGS) {
					if ($rev_text =~ /$TAGS{$key}/i) {
						my $q = "insert into `stats`.`city_used_tags` (ct_wikia_id, ct_page_id, ct_namespace, ct_kind, ct_timestamp) values (";
						$q .= $dbw->quote($city_id) . ", ";
						$q .= $dbw->quote($page_id) . ", ";
						$q .= $dbw->quote($page_namespace) . ", ";
						$q .= $dbw->quote($key) . ", ";
						$q .= "date_format(now(), '%Y%m%d%H%i%s'))";
						$dbw->do($q);
					}
				}
			}
		}
	}	
}

my ( $help, $workers, $wiki, $from, $to, $execute ) = undef;

$|++;        # switch off buffering
$workers     = 7; # by default 10 processes
GetOptions(
	"help|?"    => \$help,
	"workers=i" => \$workers,
	"wiki=i" 	=> \$wiki,
	"from=i"    => \$from,
	"to=i"      => \$to,
	"execute=s" => \$execute,
) or pod2usage( 2 );

pod2usage( 1 ) if $help;

if ( $execute ) {
	#--- set start time
	my $dbname = $execute;
	my $city_id = $wiki;
	my $start_sec = time();
	say "$dbname processed (".$city_id.")";

	my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::STATS )} );
	
	my @options = ();
	my @where = ( "ct_wikia_id = " . $dbr->quote($city_id) );
	my $oRow = $dbr->select( "min(date_format(ct_timestamp, '%Y%m%d000000')) as ts", "city_used_tags", \@where, \@options );
	my $max_ts = $oRow->{ts};

	# first check existing reports
	say "Check existing pages ";
	my @pages = ();
	@options = ();
	@where = ( "ct_wikia_id = " . $dbr->quote($city_id) );
	my $sth = $dbr->select_many( "ct_page_id", "city_used_tags", \@where, \@options );
	if ( $sth ) {
		while( my ( $page_id ) = $sth->fetchrow_array() ) {
			push @pages, $page_id;	
		}
	}	
	
	my $dbh = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'cron', $dbname )} );

	my $loop = 0;
	if ( $dbh ) {
		say "Validate " . scalar(@pages) . " pages ";
		if ( scalar( @pages ) ) {
			foreach ( @pages ) {
				my $page_id = $_;
				@options = ();
				@where = ( "rev_id = page_latest", "page_id = " . $page_id );
				$sth = $dbh->select_many( "page_id, page_namespace, page_latest", "page, revision", \@where, \@options );

				if ( $sth ) {
					while( my ( $page_id, $page_namespace, $rev_id ) = $sth->fetchrow_array() ) {
						make_job( $city_id, $dbname, $page_id, $rev_id, $page_namespace );
						$loop++;
					}
					$sth->finish();
				}
			}
		}
		
		# check last changes
		@options = ();
		@where = ( "rev_id = page_latest", "page_is_redirect = 0" );
		if ( $max_ts ) {
			push @where, "rev_timestamp>=" . $dbh->quote($max_ts);
		}
		$sth = $dbh->select_many( "page_id, page_namespace, page_latest", "page, revision", \@where, \@options );

		if ( $sth ) {
			while( my ( $page_id, $page_namespace, $rev_id ) = $sth->fetchrow_array() ) {
				if ( ! grep( /^$page_id/,@pages ) ) {
					make_job( $city_id, $dbname, $page_id, $rev_id, $page_namespace );
					$loop++;
				}
			}
			$sth->finish();
		}
		$dbh->disconnect();
	}
	my $end_sec = time();
	my @ts = gmtime($end_sec - $start_sec);
	say $dbname . " processed (".$loop." pages) ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]) ;
} else {
	my $process_start_time = time();
	
	my $pool = Thread::Pool::Simple->new(
		min => 1,
		max => $workers,
		load => 4,
		do => [sub {
			worker( @_ );
		}],
		monitor => sub {
			say "done";
		},
		passid => 1,
	);

	my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::EXTERNALSHARED );

	my @where_db = ( "city_public = 1" );
	if ($wiki) {
		push @where_db, "city_id = $wiki";
	}
	if ($from) {
		push @where_db, "city_id >= ".$dbh->quote($from);
	} 
	if ($to) {
		push @where_db, "city_id <= ".$dbh->quote($to);
	}

	my $sth = $dbh->prepare( "SELECT city_id, city_dbname, city_last_timestamp FROM city_list WHERE " . join ( " and ", @where_db ) . " ORDER BY city_last_timestamp DESC " );
	$sth->execute();
	while( my $row = $sth->fetchrow_hashref ) {
		# quick job on local database
		say "Proceed " . $row->{ "city_dbname" } . " (" . $row->{ "city_id" }. ")" ;
		
		$pool->add( $row->{ "city_id" }, $row->{ "city_dbname" } );
	}
	$sth->finish;

	$pool->join;
	
	my $process_end_time = time();
	my @ts = gmtime($process_end_time - $process_start_time);
	say "\nscript processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]) ;
}

1;
__END__

=head1 NAME

tags_report.pl - build stats for Special:TagReport

=head1 SYNOPSIS

tags_report.pl [options]

 Options:
  --help            brief help message
  --workers=<nr>    how many workers should be spawned (default 10)
  --wiki=<nr>		execute script for Wikia
  --from=<nr>		execute script for Wikis with ID>=from
  --to=<nr>			execute script for Wikis with ID<=TO

=head1 OPTIONS

=over 8

=item B<--help>

Print a brief help message and exits.

=head1 DESCRIPTION

B<This programm> iterates through all active databases in city_list and build stats for TagReport special page
=cut
