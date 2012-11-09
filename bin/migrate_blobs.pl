#!/usr/bin/perl -w
package Wikia::Blobs;

use strict;
use common::sense;
use FindBin qw/$Bin/;
use lib "$Bin/../lib/";

use Wikia::LB;
use Wikia::Settings;

use Moose;
use Data::Dumper;

has "id" => ( is => "rw", isa => "Int", required => 1 );
has "dbname" => ( is => "rw", isa => "Str", required => 1 );
has "table"	=> ( is => "rw", isa => "Str" );
has "query" => ( is => "rw", isa => "Str" );
has "count_query" => ( is => "rw", isa => "Str" );
has "blobs_table" => ( is => "rw", isa => "Str" );
has "min_blobs_year" => ( init_arg => undef, is => 'ro', default => 2007 );
has "dbh" => ( is => "rw", lazy_build => 1 );
has "dbw" => ( is => "rw", lazy_build => 0 );
has "dbl" => ( is => "rw", lazy_build => 0 );
has "count"	=> ( is => "rw", isa => "Int", lazy_build => 1 );
has "progress" => ( is => "rw", isa => "Term::ProgressBar", lazy_build => 1 );

sub _build_dbh {
	my ( $self ) = @_;
	$self->dbh( Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, $self->dbname ) );
}

sub _build_dbw {
	my ( $self ) = @_;
	$self->dbw( Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, $self->dbname ) );
}

sub _build_dbl {
	my ( $self ) = @_;
	$self->dbl( Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, $self->dbname ) );
}

sub _build_dbw {
	my ( $self ) = @_;
	$self->dbw( Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::DATAWARESHARED ) );
}

sub _build_count {
	my ( $self ) = @_;
	my $row = $self->dbh->selectrow_hashref( $self->count_query );
	return ( $row ) ? $row->{records} : 0;
}

sub  _build_progress {
	my ( $self ) = @_;
	say "Found " . $self->count . " records to check";
	my $progress = Term::ProgressBar->new( {
		name	=> sprintf( "Parse Wiki: %s (%d), table: %s", $self->dbname, $self->id, $self->table ),
		count	=> $self->count,
		ETA		=> 'linear'
	});
	$self->progress( $progress );
}

sub process {
	my ( $self ) = @_;
	
	if ( $self->count > 0 ) {

		$self->_build_dbl();
		$self->_build_dbw();

		my $sth = $self->dbh->prepare( $self->query );
		if ( $sth->execute() ) {
			while( my $row = $sth->fetchrow_hashref ) {
				$self->progress->update();
				next unless defined $row->{ "rev_text_id" };
				my $blob_id = $self->read_blob_id( $row->{ "rev_text_id" } );
				#print "old_id = " . $row->{"rev_text_id"} . ", blob_id = $blob_id \n";
				if ( $blob_id > 0 ) {
					my $blobs_table = sprintf( "blobs%d1", ( $row->{"year_ts"} < $self->min_blobs_year ) ? $self->min_blobs_year : $row->{"year_ts"} );
					$self->blobs_table( $blobs_table );
					$self->update_blobs( $row, $blob_id );
				}
			}
		}
		$sth->finish;
	}
}

sub read_blob_id {
	my ( $self, $text_id ) = @_;
	
	my $blob_id = 0;
	my $text = $self->dbh->selectrow_hashref( sprintf( "SELECT old_text FROM text WHERE old_id = %d", $text_id ) );
	if ( $text && $text->{"old_text"} =~ /^DB\:\/\/archive1\/(.*)/ ) {
		$blob_id = $1;
	}
	
	return $blob_id;
}

sub update_blobs {
	my ( $self, $row, $blob_id ) = @_;
	
	my $new_blob_id = 0;
	if ( $self->dbw ) {	
		# check if blob is moved
		my $row = $self->dbw->selectrow_hashref( sprintf( "SELECT blob_id, year_ts, new_blob_id FROM dataware.migrate_blobs WHERE blob_id = %d and year_ts = %d", $blob_id, $row->{'year_ts'} ) );
		if ( !defined $row ) {
			my $row2 = $self->dbw->selectrow_hashref( sprintf ( "SELECT blob_id FROM dataware.blobs WHERE blob_id = %d", $blob_id ) );
			if ( defined $row2 ) {
				$self->dbw->{AutoCommit} = 0; 
				$self->dbw->{RaiseError} = 1;
				
				$self->dbl->{AutoCommit} = 0;
				$self->dbl->{RaiseError} = 1;
				eval {
					# update dataware
					my $q1 = sprintf( "INSERT INTO %s.blobs (blob_text) SELECT blob_text FROM dataware.blobs WHERE blob_id = %d", $self->blobs_table, $blob_id );
					$self->dbw->do( $q1 );
					$new_blob_id = $self->dbw->{mysql_insertid};
					
					if ( $new_blob_id > 0 ) {
						my $q2 = qq{ INSERT IGNORE INTO dataware.migrate_blobs ( blob_id, year_ts, new_blob_id ) values (?,?,?) };
						$self->dbw->do( $q2, undef, $blob_id, $row->{ 'year_ts' }, $new_blob_id );
						
						# update local text table
						my $blob_url = sprintf( 'DB://blobs%d1/%d', $row->{ 'year_ts' }, $new_blob_id );
						my $q3 = qq{ UPDATE TABLE text SET old_text = ? WHERE old_id = ? };
						$self->dbl->do( $q3, undef, $blob_url, $row->{ 'rev_text_id' } )
					}
					$self->_debug_log( "Moved $blob_id => $new_blob_id" );
				};
				if ($@) {
					$self->dbw->rollback;
					$self->dbl->rollback;
					$new_blob_id = 0;
				} else {
					$self->dbw->commit;
					$self->dbl->commit;
				}
			}
		} else {
			$new_blob_id = $row->{ 'new_blob_id' };
		}
	}
	
	return $new_blob_id;
}

sub _debug_log {
	my ($self, $text) = @_;

	open ( F, ">>/tmp/migrate_blobs.log" );
	print F $text."\n";
	close ( F );
}

package Wikia::RevisionBlobs;
use strict;
use common::sense;

use Moose;
use Data::Dumper;

extends 'Wikia::Blobs';
override 'table' => sub { return 'revision'; };
override 'query' => sub { return "SELECT rev_id, rev_page, rev_timestamp, rev_text_id, year(rev_timestamp) as year_ts FROM revision ORDER BY rev_page"; };
override 'count_query' => sub { return "SELECT count(rev_id) as records FROM revision"; };

package Wikia::ArchiveBlobs;
use strict;
use common::sense;

use Moose;
use Data::Dumper;
extends 'Wikia::Blobs';

override 'table' => sub { return 'archive'; };
override 'query' => sub { return "SELECT ar_rev_id, ar_page_id, ar_timestamp, ar_text_id, year(ar_timestamp) as year_ts FROM archive"; };
override 'count_query' => sub { return "SELECT count(ar_rev_id) as records FROM archive"; };

package main;

use strict;
use common::sense;
use FindBin qw/$Bin/;
use lib "$Bin/../lib/";

use Pod::Usage;
use Getopt::Long;
use Time::HiRes qw(gettimeofday tv_interval);
use Data::Dumper;
use Term::ProgressBar;

use Wikia::Utils;
use Wikia::LB;

=sql mail table

CREATE TABLE `migrate_blobs` (
  `blob_id` int(11) NOT NULL,
  `year_ts` int(5) NOT NULL,
  `new_blob_id` int(11) NOT NULL,
  PRIMARY KEY (`blob_id`, `year_ts`, `new_blob_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
=cut 

$|++;
GetOptions(
	"help|?"		=> \( my $help = 0 ),
	"wikia=i"		=> \( my $city_id = 0 ),
	"debug"			=> \( my $debug = 0 )
) or pod2usage( 2 );

pod2usage( 1 ) if $help;

=item worker
=cut
say "Script started ...";

my $t_start = [ gettimeofday() ];	
my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::EXTERNALSHARED );
my $q = "SELECT city_id, city_dbname FROM city_list WHERE city_public = 1 ORDER BY city_id";
if ( $city_id ) {
	$q = sprintf( "SELECT city_id, city_dbname FROM city_list WHERE city_id = '%s'", $city_id );
}
my $sth = $dbh->prepare( $q );
$sth->execute();
while( my $row = $sth->fetchrow_hashref ) {
	# check revisions for Wikia
	say sprintf( "Check Wiki: %s (%d) ...", $row->{'city_dbname'}, $row->{'city_id'} );
	
	say "Parse revision";
	my $obj = Wikia::RevisionBlobs->new( "id" => $row->{ "city_id" }, "dbname" => $row->{ "city_dbname" } );
	$obj->process();

	say "Parse archive";
	my $obj = Wikia::ArchiveBlobs->new( "id" => $row->{ "city_id" }, "dbname" => $row->{ "city_dbname" } );
	$obj->process();
}
$sth->finish;
my $t_elapsed = tv_interval( $t_start, [ gettimeofday() ] ) ;
say "Script finished - time $t_elapsed";
1;
__END__

=head1 NAME

migrate_blobs.pl - move blobs from old "dataware" to new "blobsYYYYY" database.

=head1 SYNOPSIS

migrate_blobs.pl [options]

 Options:
  --help            brief help message
  --wikia=<ID>		run script for Wikia ID

=head1 OPTIONS

=over 8

=item B<--help>

Print a brief help message and exits.

=head1 DESCRIPTION

B<This programm> will move all old blobs from dataware to new blobs database.
=cut
