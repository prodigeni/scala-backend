#!/usr/bin/env perl

package Wikia::Log;
use POSIX qw(strftime);
use Time::localtime;
use common::sense;
use Moose;

has text => ( is => "rw", isa => "Str" );

__PACKAGE__->meta->make_immutable;

sub add {
	my( $self, $text ) = @_;
	if( defined $self->text ) {
		$self->text( $self->text . "; " . $text );
	}
	else {
		$self->text( $text );
	}
}

sub flush {
	my ( $self ) = @_;

	my $tm = strftime( "%F %T ",
		localtime->sec,
		localtime->min,
		localtime->hour,
		localtime->mday,
		localtime->mon,
		localtime->year );

	say "$tm ${ \$self->text }";
}

no Moose;
1;

package main;

use common::sense;
use strict;

use FindBin qw/$Bin/;
use lib "$Bin/../lib";

#
# public
#
use Getopt::Long;
use Data::Dump;
use Compress::Zlib;
use Data::Types qw(:count);
use PHP::Serialization qw(unserialize);
use Try::Tiny;

#
# private
#
use Wikia::LB;
use Wikia::ExternalLB;
use Wikia::WikiFactory;
use Wikia::Utils;

sub resolveObject {
	my ( $row, $dbh, $log ) = @_;

	#
	# rozkminić!
	#

	my $buffer = undef;

	#
	# try to inflate
	#
	try {
		$buffer = Wikia::Utils->gzinflate( $row->{ "old_text"} );
	}
	catch {
		dd( $row );
	};

	#
	# try to unserialize
	#
	try {
		$buffer = unserialize( $buffer );
	};

	given( ref( $buffer ) ) {
		when( "PHP::Serialization::Object::historyblobstub" ) {
			my $old_id = $buffer->{ "mOldId" };
			my $hash = $buffer->{ "mHash" };
			$log->add( "historyblobstub, hash: $hash, oldid: $old_id");
			#
			# get this blob
			#
#			my $sth = $dbh->prepare( 'SELECT old_flags, old_text FROM text WHERE old_id = ?' );
#			$sth->execute( $old_id );
#			my $text = $sth->fetchrow_hashref( );
#			dd( $text );

		};
		when( "PHP::Serialization::Object::concatenatedgziphistoryblob" ) {
			$log->add( "concatenatedgziphistoryblob" );
		};
		default {
			$log->add( ref( $buffer ) );
		}
	}
}


my( $city_id, $cluster, $city_db, $limit ) = undef;

GetOptions( "city-id=i" => \$city_id, "cluster=s" => \$cluster, "city-db=s" => \$city_db, "limit=i" => \$limit );

unless( ( defined( $city_id ) || defined( $city_db ) ) && defined( $cluster ) ) {
	say "Usage: $0 --city-id=<city id> | --city-db=<city dbname> --cluster=<blobs name> --limit=<max number of revisions>";
	exit(0);
}

#
# we actually don't need this but this is simple checker for existence
#
my $wf = defined( $city_db )
	? Wikia::WikiFactory->new( city_dbname => $city_db )
	: Wikia::WikiFactory->new( city_id => $city_id );

say "Connecting to ${ \$wf->city_dbname }";
my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, $wf->city_dbname );
say "Connecting to external cluster $cluster";
my $dba = Wikia::ExternalLB->instance->getConnection( Wikia::LB::DB_MASTER, undef, $cluster );

#
# find any non-external rows
#
$limit = defined( $limit ) && is_count( $limit ) ? "LIMIT $limit" : "";
say $limit;
my $sth = $dbh->prepare( qq{ SELECT /* blobsMigrator */ * FROM text WHERE old_flags NOT LIKE '%external%' $limit} );
$sth->execute();

my $moved = 0;

while( my $row = $sth->fetchrow_hashref ) {

	my $old_id = $row->{ "old_id" };
	my $log = Wikia::Log->new;

	#
	# check if text is tied to revision
	#
	my $sth = $dbh->prepare( qq{SELECT /* blobsMigrator */ rev_id FROM revision WHERE rev_text_id = ? } );
	$sth->execute( $old_id );
	my $rev = $sth->fetchrow_hashref;
	$sth->finish;
	if( exists $rev->{ "rev_id" } && defined( $rev->{ "rev_id" } ) ) {
		$log->add( "Text $old_id is tied to revision ${ \$rev->{rev_id} }" );
	}
	else {
		#
		# check if text is tied to archive
		#
		my $sth = $dbh->prepare( qq{SELECT /* blobsMigrator */ ar_rev_id FROM archive WHERE ar_text_id = ? } );
		$sth->execute( $old_id );
		my $rev = $sth->fetchrow_hashref;
		$sth->finish;
		if( exists $rev->{ "ar_rev_id" } && defined( $rev->{ "ar_rev_id" } ) ) {
			$log->add( "Text $old_id is tied to archived revision ${ \$rev->{ar_rev_id} }" );
		}
		else {
			$log->add( "Text $old_id is not tied to neither archived revision nor revision" );
		}
	}
	#
	# check if revision is compressed
	#
	my $text = $row->{ "old_text" };
	my $flags = $row->{ "old_flags" };

	if( $flags =~ /object/ ) {
		resolveObject( $row, $dbh, $log );
		$log->flush;
		next;
	}

	if( $flags =~ /gzip/ ) {
		#
		# try to uncompress it, will die if text can't be inflated
		#
		try {
			Wikia::Utils->gzinflate( $text );
		}
		catch {
			$log->add( "text $old_id can't be uncompressed" );
			$log->flush;
			exit;
		}
	}
	$log->add( "Flags: $flags" );
	#
	# update flags
	#
	$flags .= ",external";

	#
	# if text is not gzipped gzip it
	#
	if( $flags != /gzip/ ) {
		$text = Wikia::Utils->gzdeflate( $text );
		$flags .= ",gzip";
		$log->add( "Gzipping revision text before saving in blobs" );
	}

	#
	# insert into blobs
	#
	my $sta = $dba->prepare( qq{INSERT /* blobsMigrator */ INTO blobs(blob_text) VALUES( ? ) } );
	$sta->execute( $row->{ "old_text" } );
	my $last_id = $dba->{ "mysql_insertid" };

	#
	# build URL to blob
	#
	my $uri = sprintf( "DB://%s/%d", $cluster, $last_id );
	$log->add( "Writing to uri $uri" );

	#
	# update current text row
	#
	$sth = $dbh->prepare( qq{ UPDATE /* blobsMigrator */ text SET old_flags = ?, old_text = ? WHERE old_id = ? } );
	$sth->execute( $flags, $uri, $old_id ) ;
	$sth->finish;

	$log->flush;
	$moved++;
}

say "Moved $moved revisions.";
1;
