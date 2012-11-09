#!/usr/bin/perl -w

use common::sense;
use feature "say";
use encoding "UTF-8";

use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";
use Pod::Usage;
use Getopt::Long;
use Time::HiRes qw(gettimeofday tv_interval);
use DateTime;
use Data::Dumper;

use Wikia::Utils;
use Wikia::DB;
use Wikia::LB;
use Wikia::Title;
use Wikia::WikiFactory;
use Wikia::Revision;

$|++;
GetOptions( 
	"insert=i"   => \( my $insert = 50 ), 
	'days=i'     => \( my $days = 7 ),
	'help'	     => \( my $help = 0 ),
	'debug'      => \( my $debug = 0 )
) or pod2usage( 2 );
pod2usage( 1 ) if $help;

=tables
CREATE TABLE `top_blog_comments` (
  `tbc_cat_id` int(5) unsigned NOT NULL,
  `tbc_city_id` int(10) unsigned NOT NULL,
  `tbc_page_id` int(10) unsigned NOT NULL,
  `tbc_user_id` int(10) unsigned NOT NULL,
  `tbc_fedit_date` datetime NOT NULL,
  `tbc_count` int(10) unsigned NOT NULL,
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`tbc_cat_id`, `tbc_city_id`, `tbc_page_id`, `tbc_user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

=cut

=defaults
=cut
my $ns_blog = 500;
my $ns_blog_comments = 501;
my $dt_end = DateTime->now();
my $dt_begin = DateTime->now()->subtract( days => $days );
my $dbw = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS );
my $process_start_time = time();

sub fetch_blogs {
	my $dbh = shift;
	my @res = ();
	#---
	my @db_fields = ( 'page_id', 'rev_id', 'page_title', 'rev_user', 'rev_ts' );
	my $sth = $dbh->prepare( qq{SELECT page_id, rev_id, page_title, rev_user, date_format(rev_timestamp, '%Y-%m-%d %H:%i:%s') as rev_ts FROM page, revision WHERE page_id = rev_page AND rev_timestamp BETWEEN ? AND ? AND page_namespace = ? AND page_is_redirect = 0} );
	if ( $sth->execute( $dt_begin->strftime("%Y%m%d%H%M%S"), $dt_end->strftime("%Y%m%d%H%M%S"), $ns_blog ) ) {
		my %results;
		@results{@db_fields} = ();
		$sth->bind_columns( map { \$results{$_} } @db_fields );

		@res = (\%results, sub {$sth->fetch() }, $sth);
	}

	return @res;
}

sub fetch_comments {
	my ( $dbh, $title ) = @_;
	
	my $count = 0;
	my $sth = $dbh->prepare( qq{SELECT count(page_id) AS cnt FROM page WHERE page_namespace = ? AND page_title LIKE ?} );
	if ( $sth->execute( $ns_blog_comments, $title . '/@comment-%' ) ) {
		if( my $row = $sth->fetchrow_hashref ) {
			$count = $row->{cnt};
		}
		$sth->finish();
	}
		
	return $count;
}

my @wikis = ();
my $dbr = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::STATS );
my $sth = $dbr->prepare( qq{SELECT DISTINCT wiki_id FROM events WHERE rev_timestamp BETWEEN ? AND ? AND page_ns = ?} );
if ( $sth->execute( $dt_begin->strftime("%F %T"), $dt_end->strftime("%F %T"), $ns_blog ) ) {
	while( my $row = $sth->fetchrow_hashref ) {
		push @wikis, $row->{ 'wiki_id' };
	}
	$sth->finish();
}
$dbr->disconnect();

say "Found " . scalar @wikis . " Wikis to proceed";
my @values = ();
my $x = 0; 
my $y = 0;
foreach ( sort @wikis ) {
	my $wiki_id = $_;
	say "Check blog posts for Wiki: " . $wiki_id;	
	my $start_time = time();

	# wiki factory
	my $WF = Wikia::WikiFactory->new( city_id => $wiki_id );
	next unless ( $WF->city_dbname );
	
	# database connect
	my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, 'vslow', $WF->city_dbname );
	my ($row, $fetch, $sth) = fetch_blogs($dbh);
	my $rows = {};
	my $loop = 0;
	if ( defined( $fetch ) && defined( $row ) ) {
		say "Iterate blog posts for Wiki: " . $WF->city_dbname;
		while($fetch->()) {
			# add blog post article to main structure
			$rows->{ $row->{page_id} } = {
				rev_id   => $row->{ rev_id },
				rev_ts   => $row->{ rev_ts },
				user_id  => $row->{ rev_user },
				cat_id   => $WF->category->{id}
			} unless scalar keys %{ $rows->{ $row->{page_id} } };
			
			# set first revision
			if ( $rows->{ $row->{page_id} }->{ rev_id } > $row->{ rev_id } ) {
				$rows->{ $row->{page_id} }->{ rev_id } = $row->{ rev_id };
				$rows->{ $row->{page_id} }->{ rev_ts } = $row->{ rev_ts };
				$rows->{ $row->{page_id} }->{ user_id } = $row->{ rev_user };
			}
			
			# number of comments
			$rows->{ $row->{page_id} }->{ comments } = fetch_comments( $dbh, $row->{ page_title } );
			
			$loop++;
		}
		$sth->finish() if ($sth);
	}
	
	$y++ if ( $x > 0 && $x % $insert == 0 );
	say "Build SQL statement for $loop records";
	# build array with SQL inserts
	if ( scalar keys %{$rows} ) {
		foreach my $page_id ( keys %{$rows} ) {
			my $t = [
				$rows->{$page_id}->{cat_id},
				$wiki_id, 
				$page_id,
				$rows->{$page_id}->{user_id},
				$rows->{$page_id}->{rev_ts},
				$rows->{$page_id}->{comments}
			];
			push @{$values[$y]}, "('" . join( "','", @$t ) . "')";
			$x++;
		}
	}
	$dbh->disconnect() if ( $dbh );
	my $end_time = time();
	my @ts = gmtime($end_time - $start_time);
	say "Wiki ($wiki_id) processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);
}

if ( scalar @values ) {
	$dbw->do("BEGIN");
	say "\nRemove old data";
	my $sth = $dbr->prepare( qq{SELECT DISTINCT tbc_cat_id FROM specials.top_blog_comments} );
	if ( $sth->execute() ) {
		while( my $row = $sth->fetchrow_hashref ) {
			$dbw->do( sprintf("DELETE FROM specials.top_blog_comments WHERE tbc_cat_id = %s", $row->{tbc_cat_id}) );
		}
		$sth->finish();
	}
	say "\nInsert " . scalar @values . " records into db";
	my $x = 1;
	foreach my $k ( @values ) {
		my $data = join(",", map { $_ } @$k);
		if ( $data ) {
			my $sql = "INSERT IGNORE INTO specials.top_blog_comments ( tbc_cat_id, tbc_city_id, tbc_page_id, tbc_user_id, tbc_fedit_date, tbc_count ) ";
			$sql .= "VALUES $data ";
			$dbw->do($sql);
		}
	}
	$dbw->do("COMMIT");					
}
			
$dbw->disconnect();

my $process_end_time = time();
my @ts = gmtime($process_end_time - $process_start_time);
say "\nscript processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);
say "done";

__END__

=head1 NAME

blog_comments.pl - generate blog posts created in the last X days, with the following data:
	- page_id
	- user_id (author of the first revision)
	- city_id
	- date (timestamp of the first edit / blog post creation)
	- comments_count (number of comments for given blog post)
	
=head1 SYNOPSIS

blog_comments.pl [options]

 Options:
  --help                    brief help message
  --insert=<NUM>			number of inserts in SQL statement
  --days=X					generate data for the last X days
  --debug                   enable debug option

=head1 OPTIONS

=over 8

=item B<--help>:
  Brief help message
	
=item B<--insert>:
  Number of elemenets in multi insert SQL statement (insert into ... values (...), (...), ..., (...) )
  
=item B<--days>:
  Number of days when blog post was created (default 7)
  
=head1 DESCRIPTION

B<This programm> collects number of comments of blog posts created in the last X days
=cut
