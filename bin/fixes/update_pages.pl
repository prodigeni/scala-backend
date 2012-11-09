#!/usr/bin/perl

my $YML = undef;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use DBI;
use common::sense;
use Cwd;
use Getopt::Long;
use Data::Dumper;
use Time::Local ;
use Encode;
use POSIX qw(setsid uname);
use Date::Manip;

use Wikia::Config;
use Wikia::Utils qw(strtolower fixutf intval);
use Wikia::DB;
use Wikia::LB;

=global variables
=cut
my ($help, $cityid, $fromId, $toId, $pageid, $silent, $delete) = ();
GetOptions(	'help' => \$help, 'cityid=s' => \$cityid, 'from=s' => \$fromId, 'to=s' => \$toId, 'pageid=i' => \$pageid, 'silent' => \$silent, 'delete' => \$delete);

my $lb = Wikia::LB->instance;
$lb->yml( $YML ) if defined $YML;
my $dbr_ext = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::DATAWARESHARED )} );
my $dbw_ext = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::DATAWARESHARED )} );
my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED )} );

sub do_help()
{
    my $name = "update_pages.pl"; 
    print <<EOF
$name [--help] [--usedb=db[,db2[,...]]] [--soft]

    help\t\t-\tprint this text
    cityid\t\t-\twikia ID;
    from\t\t-\tfrom Wikia ID
    to\t\t-\tto Wikia ID
	pageid\t\t- fix records for page ID
	silent\t\t- don't display any logs
	delete\t\t - mark record as delete
    
EOF
;
}

sub do_run(;$$$$$$) {
	my ($cityid, $fromid, $toid, $pageid, $silent,$delete) = @_;
	my $process_start_time = time();
	my @where_db = ("city_public = 1");
	if ($cityid) {
		push @where_db, "city_id = ".$cityid;
	}
	if ($fromid) {
		push @where_db, "city_id >= ".$fromid;
	}
	if ($toid) {
		push @where_db, "city_id <= ".$toid;
	}
	
	say "get list of wikis from city list" unless ( $silent );
	my ($databases) = $dbr->get_wikis(\@where_db);
	my $main_loop = 0;
	my %ARTICLES = ();
	foreach my $num (sort ( map { sprintf("%012u",$_) } (keys %{$databases}) )) {
		#--- set city;
		my $city_id = int $num;
		#---
		my $start_sec = time();
		#---
		say $databases->{$city_id} . " processed (".$city_id.")" unless ( $silent );
		#---
		my $dbr_wiki = undef;
		eval {
			$dbr_wiki = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, undef, $databases->{$city_id} )} );
		};
		if ($@) {
			$dbr_wiki = undef;
		}
		#---
		next unless $dbr_wiki;
		#--- content_namespace
		my $content_namespaces = $dbr->__content_namespaces($city_id);
		
		#--- image_review
		my @where = ( "wiki_id = $city_id", "top_200 = 1" );
		my @options = ();
		my $oIRow = $dbr_ext->select("top_200", "image_review", \@where, \@options);
		my $top_200 = $oIRow->{top_200} || 0;
		
		#--- pages
		my %PAGES_LOCAL = ();
		my %PAGES_DATAWARE = ();
		
		say "generate list of pages from local database" unless ( $silent );
		@where = ();
		
		my $condition = ( $pageid ) ? "AND page_id = $pageid" : "";
		my $q = "
			SELECT page_id, page_is_redirect, page_latest, page_title, page_namespace, count(rev_id) as edits, max(rev_timestamp) as last_edited, rev_user 
			FROM page p
			JOIN revision r ON rev_page = page_id $condition
			GROUP BY page_id 
			ORDER BY page_id
		";
		my $sth = $dbr_wiki->handler()->prepare($q);
		if ($sth->execute()) {
			while(my ($page_id,$page_is_redirect,$page_latest,$page_title,$page_namespace,$page_edits,$last_edited,$rev_user) = $sth->fetchrow_array()) {
				my $is_content = ( grep /^\Q$page_namespace\E$/, @{$content_namespaces} );
				%{$PAGES_LOCAL{$page_id}} = (
					'latest' => $page_latest,
					'title' => Wikia::Utils->strtolower($page_title),
					'nspace' => $page_namespace,
					'orygtitle' => $page_title,
					'exists' => 1,
					'is_redirect' => $page_is_redirect,
					'is_content' => $is_content,
					'edits' => $page_edits,
					'last_edited' => $last_edited,
					'rev_user' => $rev_user
				);
			}
			$sth->finish();
		}
		say "\tfound: " . scalar(keys(%PAGES_LOCAL)) . " pages in local DB" unless ( $silent );

		say "get pages from dataware" unless ( $silent );
		@where = (
			"page_wikia_id = $city_id"
		);
		
		push @where, "page_id = $pageid" if ( $pageid );
		
		$sth = $dbr_ext->select_many(
			"page_id, page_status, page_latest, page_title, page_title_lower, page_namespace, page_is_redirect, page_is_content, page_edits, page_last_edited", 
			"`dataware`.`pages`", 
			\@where, 
			\@options
		);
		
		if ($sth) {
			while(my ($page_id, $page_status, $page_latest, $page_title, $page_title_lower, $page_namespace, $page_is_redirect, $page_is_content, $page_edits, $page_last_edited) = $sth->fetchrow_array()) {
				my $lctitle =~ s/\s/\_/g;
				$lctitle = Wikia::Utils->strtolower( $lctitle );
				%{$PAGES_DATAWARE{$page_id}} = (
					'title'	 => $page_title,
					'status' => $page_status, 
					'latest' => $page_latest, 
					'lower_title' => $page_title_lower,
					'nspace' => $page_namespace,
					'update' => ( $lctitle eq $page_title_lower ) ? 0 : 1,
					'is_redirect' => $page_is_redirect,
					'is_content' => $page_is_content,
					'edits' => $page_edits,
					'last_edited' => $page_last_edited
				);
			}
			$sth->finish();
		}
		say "\tfound: " . scalar(keys(%PAGES_DATAWARE)) . " pages in dataware" unless ( $silent );

		my $loop = 0;
		foreach my $page_id (sort keys %PAGES_DATAWARE) {
			my $title = $PAGES_DATAWARE{$page_id}{'title'};
			if (!$PAGES_LOCAL{$page_id}) {
				$PAGES_LOCAL{$page_id}{'exists'} = 0;
				$loop++;
			}
		}
		say "\tfound: " . $loop . " pages to update" unless ( $silent );

		say "find non-existent pages on dataware" unless ( $silent );
		$loop = 0;
		my @empty = ();
		foreach my $page_id (sort keys %PAGES_LOCAL) {
			if (
				 ( !$PAGES_DATAWARE{$page_id} ) || 
				 ( $PAGES_DATAWARE{$page_id}{'title'} ne $PAGES_LOCAL{$page_id}{'orygtitle'} ) || 
				 ( $PAGES_DATAWARE{$page_id}{'nspace'} ne $PAGES_LOCAL{$page_id}{'nspace'} ) || 
				 ( $PAGES_DATAWARE{$page_id}{'is_content'} ne $PAGES_LOCAL{$page_id}{'is_content'} ) || 
				 ( $PAGES_DATAWARE{$page_id}{'is_redirect'} ne $PAGES_LOCAL{$page_id}{'is_redirect'} ) 
			) {
				push @empty, $page_id;
				$loop++;				
			}
		}
		say "\tfound: " . $loop . " non-existent or changed pages on dataware." unless ( $silent );
		if ( scalar(@empty) ) {
			#$oConf->log(join(",", @empty));
			foreach (@empty) {
				my $page_id = $_;
				if ( $PAGES_LOCAL{$page_id} ) {
					my %data = (
						"page_wikia_id" => $city_id,
						"page_id" => $page_id,
						"page_namespace" => $PAGES_LOCAL{$page_id}{'nspace'},
						"page_title_lower" => $PAGES_LOCAL{$page_id}{'title'},
						"page_title" => Wikia::Utils->fixutf( $PAGES_LOCAL{$page_id}{'orygtitle'} ),
						"page_status" => 0,
						"page_is_content" => $PAGES_LOCAL{$page_id}{'is_content'},
						"page_is_redirect" => $PAGES_LOCAL{$page_id}{'is_redirect'},
						"page_edits" => $PAGES_LOCAL{$page_id}{'edits'},
						"page_latest" => $PAGES_LOCAL{$page_id}{'latest'},
						"page_last_edited" => $PAGES_LOCAL{$page_id}{'last_edited'},			
					);
					my $update = " ON DUPLICATE KEY UPDATE ";
					$update .= "page_status = values(page_status), ";
					$update .= "page_latest = values(page_latest), ";
					$update .= "page_edits = values(page_edits), ";
					$update .= "page_last_edited = values(page_last_edited), ";
					$update .= "page_is_content = values(page_is_content), ";
					$update .= "page_is_redirect = values(page_is_redirect) ";
					my @ins_options = ( $update );
					my $sql = $dbw_ext->insert( 'pages', "", \%data, \@ins_options, 1 );
				}
			}
		}

		#---
		say "update status of pages or remove non-existent in dataware" unless ( $silent );
		$dbw_ext->ping();
		my $removed = 0;
		foreach my $page_id (sort keys %PAGES_LOCAL) {
			my $status = 0;
			if ( $PAGES_LOCAL{$page_id}{'exists'} == 0 ) {
				$status = 2;
				#print $page_id . "\n";
				$removed++;
			}

			if ( $status == 2 ) {
				my %data = (
					"page_wikia_id" => Wikia::Utils->intval($city_id), 
					"page_id" =>  Wikia::Utils->intval($page_id) 
				);
				my $sql = $dbw_ext->delete("pages", \%data);
				#print $sql . "\n";		
			}
		}
		
		say "\tupdate: " . $removed . " records removed " unless ( $silent );
		
		#--- update image_review table
		say "Update image_review dataware" unless ( $silent );
		my $ir = 0;
		$condition = ( $pageid ) ? "AND dp.page_id = $pageid" : "";
		my $q = "
			SELECT page_wikia_id, dp.page_id, page_latest, page_last_edited FROM pages dp
			LEFT JOIN image_review ir on wiki_id = page_wikia_id AND ir.page_id = dp.page_id
			WHERE page_wikia_id = $city_id AND page_namespace = 6 $condition
			AND page_title_lower REGEXP '.(png|bmp|gif|jpg|ico|svg|jpeg)\$'
		";

		my $sth = $dbr_ext->handler()->prepare($q);
		if ( $sth->execute() ) {
			while(my ( $page_wikia_id, $page_id, $page_latest, $page_last_edited ) = $sth->fetchrow_array()) {
				my $data = {
					wiki_id		=> $city_id,
					page_id		=> $page_id,
					revision_id	=> $page_latest,
					user_id		=> $PAGES_LOCAL{$page_id}{rev_user} || 0,
					last_edited	=> $page_last_edited,
					top_200		=> $top_200,
					state		=> ( $delete ) ? 3 : 0
				};
			
				my $update = " ON DUPLICATE KEY UPDATE ";
				$update .= "last_edited = values(last_edited),";
				$update .= "revision_id = values(revision_id),";
				$update .= "state = values(state)";
			
				my $ins_options = [ $update ];
			
				my $res =$dbw_ext->insert( 'image_review', "", $data, $ins_options, 1 );
				$ir++;
			}
			$sth->finish();
		}
		say "\tmissing $ir records in image_review table" unless ( $silent );
		
		#---
		my $end_sec = time();
		my @tsCity = gmtime($end_sec - $start_sec);
		say $databases->{$city_id} . " processed ".sprintf ("%d hours %d minutes %d seconds\n",@tsCity[2,1,0]) unless ( $silent );
	}

	my $process_end_time = time();
	my @ts = gmtime($process_end_time - $process_start_time);
		
	say "Script processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);	
	return 1;
}


#############################################################################
################################   main   ###################################

if ($help) {
	do_help();
} else {
	do_run($cityid, $fromId, $toId, $pageid, $silent, $delete);
}    
exit(0);
