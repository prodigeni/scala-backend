#!/usr/bin/env perl

package Wikia::DailyStats;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use feature "say";
use Wikia::WikiFactory;
use Wikia::LB;
use Wikia::DB;
use Data::Dumper;

use Moose;

has city_id   => ( is => "rw", isa => "Int" );
has bdate     => ( is => "rw", isa => "Str" );
has edate     => ( is => "rw", isa => "Str" );

sub edits {
	my ( $self, $content ) = @_;

	my $dbx = new Wikia::DB( {"dbh" => Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::STATS )} );
	
	my @options = ();
	my @where = ( 
		"rev_timestamp between '" . $self->bdate . "' and '" . $self->edate . "'",
		"( event_type = 1 or event_type = 2)",
		"is_redirect = 'N'",
		"wiki_id = " . $dbx->quote($self->{city_id})
	);
	
	if ( $content ) {
		push @where, "is_content = 'Y'";
	}

	my $oRow = $dbx->select(
		" count(0) as cnt ",
		" events ",
		\@where,
		\@options
	);
	my $cnt = $oRow->{cnt};
	
	$dbx->disconnect() if ($dbx);
	
	return $cnt;
};

sub editors {
	my ( $self, $content ) = @_;

	my $dbx = new Wikia::DB( {"dbh" => Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::STATS )} );
	
	my @options = ();
	my @where = ( 
		"rev_timestamp between '" . $self->bdate . "' and '" . $self->edate . "'",
		"( event_type = 1 or event_type = 2)",
		"is_redirect = 'N'",
		"wiki_id = " . $dbx->quote($self->{city_id}),
		"user_id != 0"
	);
	
	if ( $content ) {
		push @where, "is_content = 'Y'";
	}

	my $oRow = $dbx->select(
		" count(distinct(user_id)) as cnt ",
		" events ",
		\@where,
		\@options
	);
	my $cnt = $oRow->{cnt};
	
	$dbx->disconnect() if ($dbx);
	
	return $cnt;
}

no Moose;
1;

package main;

use Getopt::Long;
use Data::Dumper;

sub usage {
	say ( "$0 --wikis=<city id> --bdate=<YYYY-MM-DD> --edate=<YYYY-MM-DD>" );
	exit 1;
}

my ( $wikis, $bdate, $edate ) = undef;

GetOptions( "wikis=s" => \$wikis, "bdate=s" => \$bdate, "edate=s" => \$edate );

usage() unless defined $bdate && defined $edate;

$bdate = sprintf( "%s 00:00:00", $bdate );
$edate = sprintf( "%s 23:59:59", $edate );

my @rows = ();
if ( !$wikis ) {
	# connect to wikicitiee
	my $dbr = new Wikia::DB( {"dbh" => Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::EXTERNALSHARED )} );
	my @where = ("city_public = 1", "city_url not like 'http://techteam-qa%'");
	my @options = ("order by city_id");
	my $sth = $dbr->select_many("city_id", "`wikicities`.`city_list`",\@where, \@options);
	if ($sth) {
		while(my ($city_id) = $sth->fetchrow_array()) {
			push @rows, $city_id;
		}
		$sth->finish();
	}
} else {
	@rows = split /,/, $wikis;
}

my @data = ();
foreach ( @rows ) {
	my $oWikia = Wikia::WikiFactory->new( city_id => $_ );
	my $stats = Wikia::DailyStats->new( city_id => $_, bdate => $bdate, edate => $edate );
	my $row = {
		'wikia' => $oWikia->city_dbname,
		'id' => $oWikia->city_id,
		'url' => $oWikia->city_url,
		'edits' => $stats->edits(0),
		'content_edits' => $stats->edits(1),
		'editors' => $stats->editors(0),
		'content_editors' => $stats->editors(1)
	};
	push @data, $row;
}

my $loop = 0;
foreach ( @data ) {
	my $x = $_;
	if ( $loop == 0 ) {
		foreach my $key ( keys %$x ) {
			print "$key;";
		}
		print "\n";
	}
	
	foreach my $key ( keys %$x ) {
		print $x->{$key} . ";";
	}
	print "\n";
	$loop++;
}

1;
