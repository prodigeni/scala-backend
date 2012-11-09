package Wikia::SimpleQueue;

#
# simple naive queue implementation, should be replaced by AMQP or something
# similar
#
use strict;
use common::sense;

use Wikia::LB;
use MooseX::Singleton;
use Data::Types qw(:int);

our %locked = ();

sub handler {
	my( $self ) = @_;
	return Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS );
}

sub push {
	my( $self, $item ) = @_;
	
	my $row = $self->record( $item );
	my $exists = exists $row->{ "city_id"} && is_int( $row->{ "city_id" } );
	
	if ( !$exists ) {	
		$self->add( $item );
	} else {
		$self->unlock( $row );
	}
}

sub add {
	my ( $self, $item ) = @_;
	
	my $dbh = $self->handler();
	my $sth = $dbh->prepare( "INSERT IGNORE INTO specials.jobs_dirty (city_id) VALUES (?)" );
	$sth->execute( $item );
}
		
sub pop {
	my( $self ) = @_;

	my $dbh = $self->handler();
	my $sth = $dbh->prepare( "SELECT * FROM specials.jobs_dirty WHERE locked IS NULL ORDER BY timestamp LIMIT 1" );
	$sth->execute();
	my $row = $sth->fetchrow_hashref();
	$sth->finish;
	my $item = $self->lock( $row );

	return $item;
}

sub exists { 
	my ( $self, $row ) = @_;	
	return exists $row->{ "city_id"} && is_int( $row->{ "city_id" } );
}

sub lock {
	my ( $self, $row ) = @_;
	my $dbh = $self->handler();
	
	my $item = undef;
	if( $self->exists( $row ) ) {
		my $sth = $dbh->prepare( "UPDATE specials.jobs_dirty SET locked = ? WHERE city_id = ?" );
		$sth->execute( $row->{ "timestamp" }, $row->{ "city_id" } );
		$item = $row->{ "city_id" };
	}
	
	return $item;
}
		
sub unlock {
	my ( $self, $row ) = @_;
	my $dbh = $self->handler();
	my $sth = $dbh->prepare( "UPDATE specials.jobs_dirty SET locked = NULL WHERE city_id = ?" );
	$sth->execute( $row->{ "city_id" } );
}

sub unlockAll {
	my ( $self, $row ) = @_;
	my $dbh = $self->handler();
	my $sth = $dbh->prepare( "UPDATE specials.jobs_dirty SET locked = NULL WHERE locked is not null" );
	$sth->execute();
}

sub cleanup {
	my ( $self, $item ) = @_;
	my $dbh = $self->handler();
	my $sth = $dbh->prepare( "DELETE FROM specials.jobs_dirty WHERE city_id = ?" );
	$sth->execute( $item );
}

sub record {
	my ( $self, $city_id ) = @_;
	my $dbh = $self->handler();
	my $sth = $dbh->prepare( qq{SELECT locked, city_id FROM specials.jobs_dirty WHERE city_id = ? LIMIT 1} );
	$sth->execute( $city_id );
	my $row = $sth->fetchrow_hashref();
	$sth->finish;
	
	return $row;
}

__PACKAGE__->meta->make_immutable;
1;
