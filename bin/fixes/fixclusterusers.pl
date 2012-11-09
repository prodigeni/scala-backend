#!/usr/bin/perl -w
package Wikia::ClusterUser;

use strict;
use common::sense;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";

use Wikia::LB;
use Wikia::Settings;

use Moose;
use Data::Dumper;

has "dbname" 	=> ( is => "rw", isa => "Str" );
has "cluster"	=> ( is => "rw", isa => "Int" );
has "query_uid" => ( is => "rw", isa => "Str", default => 'SELECT * FROM user WHERE user_id = ?' );
has "query_uname" => ( is => "rw", isa => "Str", default => 'SELECT * FROM user WHERE user_name = ?' );
has "dbh" 		=> ( is => "rw", lazy_build => 0 );
has "dbw" 		=> ( is => "rw", lazy_build => 0 );
has "dbd"		=> ( is => "rw", lazy_build => 0 );
has "user"		=> ( is => "rw", isa => "HashRef" );
has "dry"		=> ( is => "rw", isa => "Int", default => 0  );
#has "progress"	=> ( is => "rw", isa => "Term::ProgressBar", lazy_build => 1 );

sub _build_dbh {
	my ( $self ) = @_;
	$self->dbh( Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, $self->dbname ) );
}

sub _build_dbw {
	my ( $self ) = @_;
	$self->dbw( Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, $self->dbname ) );
}

sub _build_dbd {
	my ( $self ) = @_;
	$self->dbd( Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::DATAWARESHARED ) );
}

sub _build_progress {
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
	
	my $result = 0;

	$self->_build_dbh();
	$self->_build_dbw();

	# check by user_id
	my $sth = $self->dbh->prepare( $self->query_uid );
	if ( $sth->execute( $self->user->{'user_id'} ) ) {
		if ( my $row = $sth->fetchrow_hashref ) {
			#$self->progress->update();
			if ( 
				( $self->user->{user_id} != $row->{user_id} ) ||
				( $self->user->{user_name} ne $row->{user_name} ) || 
				( $self->user->{user_password} ne $row->{user_password} )
			) { 
				if ( $self->dry ) {
					$self->_dry_log( $row->{user_id} );
				} else {
					$result = $self->update_user( $row, 'uid' );
				}
			}
		}
	}
	$sth->finish;
	
	# check by user_name
	$sth = $self->dbh->prepare( $self->query_uname );
	if ( $sth->execute( $self->user->{'user_name'} ) ) {
		if ( my $row = $sth->fetchrow_hashref ) {
			#$self->progress->update();
			if ( 
				( $self->user->{user_id} != $row->{user_id} ) ||
				( $self->user->{user_name} ne $row->{user_name} ) || 
				( $self->user->{user_password} ne $row->{user_password} )
			) { 
				if ( $self->dry ) {
					$self->_dry_log( $row->{user_id} );
				} else {
					$result = $self->update_user( $row, 'uname' );
				}
			}
		}
	}
	$sth->finish;
	
	return $result;
}

sub update_user {
	my ( $self, $row, $with ) = @_;
	my $result = 0;
	if ( $self->dbw ) {	
		# check if blob is moved
		$self->dbw->{AutoCommit} = 0; 
		$self->dbw->{RaiseError} = 1;
		eval {
			# remove invalid record in wikicities_cX table
			if ( $with eq 'uid' ) {
				my $q1 = sprintf( "DELETE FROM %s.user WHERE user_id = %d", $self->dbname, $row->{'user_id'} );
				$self->dbw->do( $q1 );
			} elsif ( $with eq 'uname' ) {
				my $q1 = sprintf( "DELETE FROM %s.user WHERE user_name = %s", $self->dbname, $self->dbh->quote( $row->{'user_name'} ) );
				$self->dbw->do( $q1 );				
			}
		
			# insert fixed record from wikicities table
			my ($names,$values) = ();
			foreach (keys %{ $self->user }) {
				$values .= sprintf( " %s,", $self->dbw->quote( $self->user->{ $_ } ) ) ;
				$names .= sprintf( "%s,", $_ );
			}
			chop($names); chop($values);
			my $q2 = sprintf( "insert into %s.user (%s) values(%s) ", $self->dbname, $names, $values );
			$self->dbw->do( $q2 );

			$self->_debug_log( "Replaced user record (" . $self->dbname . "): " . ( ( $with eq 'uid' ) ? $row->{user_id} : $row->{user_name} ) . " with record: " . $self->user->{'user_id'} . "\n" );
		};
		if ($@) {
			$self->dbw->rollback;
			$result = -1;
		} else {
			$self->dbw->commit;
			
			# update log on dataware
			$self->_build_dbd(); 	
			my ($names,$values);
			foreach (keys %{ $row }) {
				$values .= sprintf( " %s,", $self->dbd->quote( $row->{ $_ } ) ) ;
				$names .= sprintf( "%s,", $_ );
			}
			chop($names); chop($values);
			my $q_dbd = sprintf( "insert into dataware.fixed_user (%s, cluster) values(%s, %d) ", $names, $values, $self->cluster );
			$self->dbd->do( $q_dbd );
			$result = 1;
		}
	}
	
	return $result;
}

sub _debug_log {
	my ($self, $text, $show ) = @_;

	open ( F, ">>/tmp/fixclusterusers.log" );
	print F $text;
	print $text if ( $show );
	close ( F );
}

sub _dry_log {
	my ($self, $text ) = @_;
	open ( F, ">>/tmp/fixclusterusers_dry.log" );
	print F $text . "\n"; 
	close ( F );
}

package Wikia::ClusterUser_C2;
use strict;
use common::sense;

use Moose;
use Data::Dumper;

extends 'Wikia::ClusterUser';
override 'dbname' => sub { return 'wikicities_c2'; };
override 'cluster' => sub { return 2; };

package Wikia::ClusterUser_C3;
use strict;
use common::sense;

use Moose;
use Data::Dumper;
extends 'Wikia::ClusterUser';
override 'dbname' => sub { return 'wikicities_c3'; };
override 'cluster' => sub { return 3; };

package Wikia::ClusterUser_C4;
use strict;
use common::sense;

use Moose;
use Data::Dumper;
extends 'Wikia::ClusterUser';
override 'dbname' => sub { return 'wikicities_c4'; };
override 'cluster' => sub { return 4; };

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

CREATE TABLE `fixed_user` (
  `user_id` int(5) unsigned NOT NULL AUTO_INCREMENT,
  `user_name` varchar(255) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT '',
  `user_real_name` varchar(255) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT '',
  `user_password` tinyblob NOT NULL,
  `user_newpassword` tinyblob NOT NULL,
  `user_email` tinytext NOT NULL,
  `user_touched` char(14) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT '',
  `user_token` char(32) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT '',
  `user_email_authenticated` char(14) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `user_email_token` char(32) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `user_email_token_expires` char(14) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `user_registration` varchar(16) DEFAULT NULL,
  `user_newpass_time` char(14) DEFAULT NULL,
  `user_editcount` int(11) DEFAULT NULL,
  `user_birthdate` date DEFAULT NULL,
  `user_options` blob NOT NULL,
  `cluster` int NOT NULL,
  `ts` timestamp NOT NULL DEFAULT now(),
  KEY (`user_id`),
  KEY `user_name` (`user_name`),
  KEY `user_email` (`user_email`(40)), 
  KEY `cluster` (`cluster`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 
=cut 

$|++;
GetOptions(
	"help|?"		=> \( my $help = 0 ),
	"user=i"		=> \( my $user_id = 0 ),
	"debug"			=> \( my $debug = 0 ),
	"dry"			=> \( my $dry = 0 )
) or pod2usage( 2 );

pod2usage( 1 ) if $help;

=item worker
=cut
say "Script started ...";

my $t_start = [ gettimeofday() ];
my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::EXTERNALSHARED );
my $query = 'SELECT * FROM user';
if ( $user_id ) {
	$query = sprintf( 'SELECT * FROM user WHERE user_id = %s', $user_id );
}
my $sth = $dbh->prepare( $query );
$sth->execute();
while( my $row = $sth->fetchrow_hashref ) {
	# check user 
	my $tuser_start = [ gettimeofday() ];
	my $msg = sprintf( "Check user: %s (%d) ...: ", $row->{'user_name'}, $row->{'user_id'} );
	Wikia::ClusterUser->_debug_log( $msg, 1 );
	
	my $obj = Wikia::ClusterUser_C2->new( "user" => $row, "dry" => $dry );
	my $result = $obj->process();
	$msg = "C2 - " . $result . ", ";
	Wikia::ClusterUser->_debug_log( $msg, 1 );

	my $obj = Wikia::ClusterUser_C3->new( "user" => $row, "dry" => $dry );
	$result = $obj->process();
	$msg = "C3 - " . $result . ", ";
	Wikia::ClusterUser->_debug_log( $msg, 1 );
	
	my $obj = Wikia::ClusterUser_C4->new( "user" => $row, "dry" => $dry );
	$result = $obj->process();
	$msg = "C4 - " . $result . ", ";
	Wikia::ClusterUser->_debug_log( $msg, 1 );
	
	my $tuser_elapsed = tv_interval( $tuser_start, [ gettimeofday() ] ) ;
	$msg = "done: $tuser_elapsed \n";
	Wikia::ClusterUser->_debug_log( $msg, 1 );
}
$sth->finish;
my $t_elapsed = tv_interval( $t_start, [ gettimeofday() ] ) ;
Wikia::ClusterUser->_debug_log( "\nScript finished - time $t_elapsed\n", 1);

1;
__END__

=head1 NAME

fixcluserusers.pl - fix invalid users on C2/C3/C4 clusters

=head1 SYNOPSIS

fixcluserusers.pl [options]

 Options:
  --help            brief help message
  --user=<ID>		run script for USER ID

=head1 OPTIONS

=over 8

=item B<--help>

Print a brief help message and exits.

=head1 DESCRIPTION

B<This programm> will fix invalid records in user table on C2/C3/C4
=cut
