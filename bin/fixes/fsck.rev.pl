#!/usr/bin/perl


# sync revision table with user table, only contributions

use Modern::Perl;
use Data::Dump;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib";


use Wikia::LB;
use Wikia::ExternalLB;
use Wikia::WikiFactory;

use Getopt::Long;


sub get_revisions {

	my ( $from, $to, $wiki_factory ) = @_;
	my @revisions = ();

	say $wiki_factory->city_id . " => " . $wiki_factory->city_dbname;

	my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, $wiki_factory->city_dbname );
    my $dbc = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::EXTERNALSHARED  );


	my $sth = $dbh->prepare( "SELECT * FROM revision, text WHERE rev_timestamp BETWEEN ? AND ? AND rev_text_id = old_id" );
	my $stc = $dbc->prepare( "SELECT user_id, user_name FROM user WHERE user_id = ?" );
	my $stu = $dbc->prepare( "SELECT user_id, user_name FROM user WHERE user_name = ?" );

	$sth->execute( $from, $to );

	while( my $row = $sth->fetchrow_hashref ) {
		next if $row->{ "rev_user" } == 0;

		#
		# user_id in revisions
		#
		$stc->execute( $row->{ "rev_user" } );
		my $user = $stc->fetchrow_hashref;
		$stc->finish;

		#
		# user_id in user
		#
		$stu->execute( $row->{ "rev_user_text" } );
		my $nuser = $stu->fetchrow_hashref;
		$stu->finish;

		if( $user->{ "user_name"} ne $row->{ "rev_user_text" } ) {
			say "user_id=$nuser->{ user_id } <> rev_user=$row->{ rev_user }, user_name=$user->{ user_name } <> rev_user_text=$row->{ rev_user_text } rev_id=$row->{rev_id}";
			$dbh->do( "UPDATE revision SET rev_user = ? WHERE rev_id = ? LIMIT 1", undef,  $nuser->{ user_id }, $row->{rev_id} );

			#
			# fix blobs table as well
			#

			my ( $store, $cluster, $id ) = $row->{ "old_text" } =~ m|([^/]+)//([^/]+)/(.+)|;
			say "Getting blobs id = $id from $cluster";
			my $exth = Wikia::ExternalLB->instance->getConnection( Wikia::LB::DB_MASTER, undef, $cluster );
			my $exts = $exth->prepare( "SELECT * FROM blobs WHERE blob_id = ?" );
			$exts->execute( $id );
			$exts->finish;
			$exth->do( "UPDATE blobs SET rev_user = ?, rev_wikia_id = ? WHERE blob_id = ? LIMIT 1", undef,  $nuser->{ user_id }, $wiki_factory->city_id, $id );
		}
	}
}

my $city_dbname = undef;
my $city_id = undef;
my $wiki_factory = undef;

GetOptions( "db=s" => \$city_dbname, "id=i" => \$city_id );

if( defined $city_dbname || defined $city_id ) {
	if( defined  $city_dbname ) {
		$wiki_factory = Wikia::WikiFactory->new( city_dbname => $city_dbname );
	}
	else {
		$wiki_factory = Wikia::WikiFactory->new( city_id => $city_id );
	}
	my $revisions = get_revisions( "20100526000000", "20100526163524", $wiki_factory );
}
