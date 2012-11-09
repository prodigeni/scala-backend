#!/usr/bin/perl

use strict;
use common::sense;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use DBI;
use DateTime;
use Wikia::LB;
use Wikia::DB;
use Wikia::Utils qw( note );
use Wikia::Config;

use Time::HiRes qw(gettimeofday tv_interval);
use Getopt::Long;
use Data::Dumper;
use Pod::Usage;

$|++;
GetOptions(
	'help'				=> \( my $help    = 0 ),
	'cityid=i' 			=> \( my $city_id = 0 ),
	'from=s'			=> \( my $from    = DateTime->now->subtract( days => 15 )->strftime('%Y%m%d000000') ),
	'to=s'				=> \( my $to      = DateTime->now->strftime('%Y%m%d000000') )
) or pod2usage( 2 );

pod2usage( 1 ) if $help;
pod2usage( 1 ) unless ( $from && $to ); 

sub _revision_by_timestamp {
	my ( $dbh, $date, $order ) = @_;
	
	# get min rev_id for period
	my $rev_id = 0;
	my $q1 = "SELECT rev_id FROM revision ";
	$q1 .= "WHERE rev_timestamp " . ( ( $order == 1 ) ? '>=' : '<=' ) . " '" . $date . "' ";
	$q1 .= "ORDER by rev_id " . ( ( $order == 1 ) ? 'ASC' : 'DESC' ) . " LIMIT 1";
	my $sth = $dbh->prepare( $q1 );
	$sth->execute();
	if ( my $row = $sth->fetchrow_hashref ) {
		$rev_id = $row->{ rev_id };
	}

	return $rev_id;
}

sub _page_last_edited {
	my ( $dbh, $page_id ) = @_;
	
	# get min rev_id for period
	my $rev_ts = 0;
	my $q1 = "SELECT rev_timestamp FROM revision WHERE rev_page = '" . $page_id . "' ORDER by 1 DESC LIMIT 1";
	my $sth = $dbh->prepare( $q1 );
	$sth->execute();
	if ( my $row = $sth->fetchrow_hashref ) {
		$rev_ts = $row->{ rev_ts };
	}

	return $rev_ts;
}

sub _update_dataware_image_review {
	my ( $row, $upstate ) = @_;

	if ( int $row->{ page_namespace } == 6  && $row->{ page_title} =~ /\.png|bmp|gif|jpg|jpeg|ico|svg/i ) { 
		my $dba = new Wikia::DB( {"dbh" => Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::DATAWARESHARED )} );
	
		my $res = undef;
		my $state = ( $row->{state} ) ? $row->{state} : 0;
		my $flags = 0;

		# See if any rows are marked as top_200 and lazily set the remaining rows
		my $wiki_id = $row->{wiki_id};
		my $rowTop = $dba->query("
			SELECT COUNT(*) = 1 AS top_200
			FROM (SELECT *
				  FROM image_review
				  WHERE wiki_id = $wiki_id
				    AND top_200 IS TRUE
				    LIMIT 1
				 ) t1");

		my $data = {
			wiki_id		=> $row->{wiki_id},
			page_id		=> $row->{rev_page},
			revision_id	=> $row->{rev_id},
			user_id		=> $row->{rev_user},
			last_edited	=> $row->{rev_timestamp},
			state		=> $state,
			flags		=> $flags,
			top_200		=> ($rowTop->{top_200} || 0),
		};
	
		my $update = " ON DUPLICATE KEY UPDATE ";
		$update .= "last_edited = values(last_edited), ";
		$update .= "revision_id = values(revision_id), ";
		if ( $upstate ) { 
			$update .= "state = values(state), ";
		}
		$update .= "user_id = values(user_id) ";
	
		my $ins_options = [ $update ];
	
		$res = $dba->insert( 'image_review', "", $data, $ins_options, 1 );
	
		return 1;
	} else {
		return 0;
	}
}

my $process_start_time = time();

my @where_db = ("city_public = 1");
if ($city_id) {
	push @where_db, "city_id = ".$city_id;
}

my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::EXTERNALSHARED );
my $sth = $dbh->prepare( "SELECT city_id, city_dbname FROM city_list WHERE " . join( " AND ", @where_db ) . " ORDER BY city_id" );
$sth->execute();
while( my $row = $sth->fetchrow_hashref ) {
	my $tstart = [ gettimeofday() ];

	print "Proceed " . $row->{ "city_dbname" } . " (" . $row->{ "city_id" }. ") ... ";

	my $dbw = undef;
	eval {
		# connect to Wikia
		$dbw = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, $row->{ "city_dbname" } );
	};
	
	if ( $@ ) {
		print "cannot connect to database - skip \n";
		next;
	}
	
	# get min/max revision for period
	my $rev_id1 = _revision_by_timestamp( $dbw, $from, 1 );
	my $rev_id2 = _revision_by_timestamp( $dbw, $to, -1 );

	if ( !$rev_id1 || !$rev_id2 ) {
		print "cannot find records\n";
		next;
	}

	# get all records for revisions
	my $query = qq{ 
		SELECT * FROM revision 
		JOIN page ON rev_page = page_id 
		WHERE ( rev_id between ? and ? ) AND page_namespace = 6 AND page_is_redirect = 0 
	};
	my $sth = $dbw->prepare( $query );
	$sth->execute( $rev_id1, $rev_id2 );
	my ( $records, $invalid ) = ( 0, 0 );
	my @use_pages = ();
	while( my $rev = $sth->fetchrow_hashref ) {
		next if ( grep /^\Q$rev->{rev_page}\E$/, @use_pages );
		$rev->{wiki_id} = $row->{city_id};
		my $rev_ts = _page_last_edited( $dbw, $rev->{rev_page} );
		$rev->{rev_timestamp} = $rev_ts if( $rev_ts );
		if ( _update_dataware_image_review( $rev, 0 ) ) {
			$records++;
		} else {
			$invalid++;
		}
		push @use_pages, $rev->{rev_page};
	}
	
	# get all records for archive
	$query = qq{ 
		SELECT ar_namespace as page_namespace, ar_title as page_title, ar_page_id as rev_page, ar_timestamp as rev_timestamp, ar_rev_id as rev_id, ar_user as rev_user
		FROM archive 
		WHERE ar_namespace = 6 and ( ar_rev_id between ? and ? ) 
	};
	$sth = $dbw->prepare( $query );
	$sth->execute( $rev_id1, $rev_id2 );
	my $archive = 0;
	while( my $rev = $sth->fetchrow_hashref ) {
		$rev->{wiki_id} = $row->{city_id};
		$rev->{state} = 3;
		if ( _update_dataware_image_review( $rev, 1 ) ) {
			$archive++;
		} else {
			$invalid++;
		}
	}
	
	print "added: $records, invalid: $invalid, archive: $archive, ";
	my $tend = time();
	my @ts = gmtime($tstart - $tend);
	
	my $tend = tv_interval( $tstart, [ gettimeofday() ] ) ;
	print "done: $tend \n";
}

my $process_end_time = time();
my @ts = gmtime($process_end_time - $process_start_time);
note "script processed ".sprintf ("%d hours %d minutes %d seconds",@ts[2,1,0]);
note "done";
1;
__END__

=head1 NAME

fiximagereview.pl - fix image_review table 

=head1 SYNOPSIS

fiximagereview.pl [options]

 Options:
  --help         brief help message
  --cityid=<ID>  run script for Wikia
  --from, --to   run script for time period (MW format YYYYMMDDHHSSII)

=head1 DESCRIPTION

B<This programm> will add missing records to image_review table
=cut
