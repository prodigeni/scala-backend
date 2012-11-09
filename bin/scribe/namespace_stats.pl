#!/usr/bin/perl -w

use common::sense;
use feature "say";

use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";
use Getopt::Long;
use Time::HiRes qw(gettimeofday tv_interval);
use Pod::Usage;

use Wikia::Utils;
use Wikia::DB;
use Wikia::LB;

package Wikia::Namespace::Stats;

=pod namespace_monthly_stats
CREATE TABLE `namespace_monthly_stats` (
  `wiki_id` int(8) unsigned NOT NULL,
  `page_ns` int(6) unsigned NOT NULL,
  `wiki_cat_id` int(8) unsigned NOT NULL,
  `wiki_lang_id` int(8) unsigned NOT NULL,
  `stats_date` mediumint(6) unsigned NOT NULL DEFAULT '0',
  `pages_all` int(8) unsigned NOT NULL DEFAULT '0',
  `pages_daily` int(8) unsigned NOT NULL DEFAULT '0',
  `pages_edits` int(8) unsigned NOT NULL DEFAULT '0',
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`wiki_id`,`stats_date`,`page_ns`),
  KEY `stats_date` (`stats_date`,`page_ns`),
  KEY `lang_stats` (`stats_date`, `wiki_lang_id`, `page_ns`),
  KEY `cat_stats` (`stats_date`, `wiki_cat_id`, `page_ns`),
  KEY `lang_cat_stats` (`stats_date`, `wiki_lang_id`, `wiki_cat_id`, `page_ns`)  
) ENGINE=InnoDB DEFAULT CHARSET=latin1
/*!50100 PARTITION BY RANGE (YEAR(stats_date))
(PARTITION nms2002 VALUES LESS THAN (2002) ENGINE = InnoDB,
 PARTITION nms2003 VALUES LESS THAN (2003) ENGINE = InnoDB,
 PARTITION nms2004 VALUES LESS THAN (2004) ENGINE = InnoDB,
 PARTITION nms2005 VALUES LESS THAN (2005) ENGINE = InnoDB,
 PARTITION nms2006 VALUES LESS THAN (2006) ENGINE = InnoDB,
 PARTITION nms2007 VALUES LESS THAN (2007) ENGINE = InnoDB,
 PARTITION nms2008 VALUES LESS THAN (2008) ENGINE = InnoDB,
 PARTITION nms2009 VALUES LESS THAN (2009) ENGINE = InnoDB,
 PARTITION nms2010 VALUES LESS THAN (2010) ENGINE = InnoDB,
 PARTITION nms2011 VALUES LESS THAN (2011) ENGINE = InnoDB,
 PARTITION nms2012 VALUES LESS THAN (2012) ENGINE = InnoDB,
 PARTITION nms2013 VALUES LESS THAN (2013) ENGINE = InnoDB,
 PARTITION nms2014 VALUES LESS THAN (2014) ENGINE = InnoDB,
 PARTITION nms2015 VALUES LESS THAN (2015) ENGINE = InnoDB, 
 PARTITION nms9999 VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */
=cut

use Moose;

use constant EDIT_CATEGORY 			=> 1; 
use constant CREATEPAGE_CATEGORY 	=> 2; 
use constant DELETE_CATEGORY		=> 3;
use constant UNDELETE_CATEGORY		=> 4; 
use constant ENABLE_NEW_CATEGORY 	=> 0;

has "month"	 	=> ( is => "rw", "isa" => "Int",  required => 1 );
has "usedb"	 	=> ( is => "rw", "isa" => "Str",  lazy_build => 0 );
has "dbr" 	 	=> ( is => "rw", lazy_build => 1 ); # Wikia:DB handler
has "dbw"  	 	=> ( is => "rw", lazy_build => 1 ); # Wikia:DB handler
has "dbc"  	 	=> ( is => "rw", lazy_build => 1 ); # Wikia:DB handler
has "params" 	=> ( is => "rw", "isa" => "HashRef",  lazy_build => 0 );
has "databases" => ( is => "rw", "isa" => "HashRef",  lazy_build => 0 );

#__PACKAGE__->meta->make_immutable;

sub  _build_dbr {
	my $self = shift;

	my $lb = Wikia::LB->instance;

	my $dbh = $lb->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::STATS );
	$dbh->{ "mysql_auto_reconnect" } = 1;
	
	my $dbr = new Wikia::DB( { "dbh" => $dbh } );
	$self->dbr( $dbr ) if $dbr;
}

sub  _build_dbw {
	my $self = shift;

	my $lb = Wikia::LB->instance;

	my $dbh = $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS );
	$dbh->{ "mysql_auto_reconnect" } = 1;
	
	my $dbw = new Wikia::DB( { "dbh" => $dbh } );
	$self->dbw( $dbw ) if $dbw;
}

sub  _build_dbc {
	my $self = shift;

	my $lb = Wikia::LB->instance;

	my $dbh = $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED );
	$dbh->{ "mysql_auto_reconnect" } = 1;
	
	my $dbc = new Wikia::DB( { "dbh" => $dbh } );
	$self->dbc( $dbc ) if $dbc;
}

sub _build_databases {
	my ($self) = @_;

	my $where_db = [
		"city_public = 1", 
		"city_url not like 'http://techteam-qa%'",
		"city_useshared = 1"
	];

	if ( $self->usedb && $self->usedb =~ /\+/ ) {
		# usedb=+177
		$self->usedb =~ s/\+//i;
		push @{$where_db}, "city_id > " . Wikia::Utils->intval($self->usedb);
	} elsif ( $self->usedb && $self->usedb ne "*" ) {
		# dbname=wikicities, [ ... ]
		my @use_dbs = split /,/, $self->usedb;
		push @{$where_db}, "city_dbname in (".join(",", map { $self->dbc->quote($_) } @use_dbs).")";
	} elsif ( !$self->usedb ) {
		# all wikis - check last edit timestamp
		push @{$where_db}, "city_last_timestamp > ". $self->dbc->quote($self->params->{start_date})
	}
	
	# database
	my $databases = $self->dbc->get_wikis( $where_db, 'city_dbname' );
	$self->databases($databases);
}	

sub _namespace_all {
	my ( $self, $id, $ns ) = @_;
	
	my $cond = "p1.wiki_id = " . Wikia::Utils->intval($id); 
	my $where = [
		"p2.wiki_id = " . Wikia::Utils->intval($id),
		"p2.rev_timestamp <= " . $self->dbr->quote( $self->params->{end_date} )
	];

	my $options = [
		' GROUP BY p2.wiki_id, p2.page_id ',
		' HAVING ( select count(page_id) from events p3 where p3.wiki_id = p2.wiki_id and p3.page_id = p2.page_id and log_id > 0) = 0 ',
		' ORDER BY p2.wiki_id desc, p2.page_id desc, p2.rev_id desc, p2.event_type desc '
	];

	my $subquery = $self->dbr->sql("p2.page_id, p2.wiki_id, max(p2.rev_id) as max_rev", "events p2", $where, $options);

	my $sql = "select count(page_id) as cnt from ";
	$sql .= "(select p1.page_id, p1.page_ns, p1.is_redirect, p1.rev_timestamp, p1.event_type from events p1 ";
	$sql .= "inner join ( " . $subquery . " ) as c ";
	$sql .= "on c.page_id = p1.page_id and p1.rev_id = c.max_rev and p1.wiki_id = c.wiki_id ";
	$sql .= ( ( $cond ) ? " where " . $cond : "" ) . " ) as d ";
	$sql .= "where d.page_ns = '" . $ns . "' and d.is_redirect = 'N' ";

	my $oRow = $self->dbr->query($sql);
	my $res = $self->__make_value($oRow);
	
	return $res;
}

sub _namespace_new {
	my ( $self, $id, $ns ) = @_;
	
	my $res = 0;
	if ( $self->ENABLE_NEW_CATEGORY == 0 ) {
		my $where = [
			"c1.wiki_id = " . Wikia::Utils->intval($id),
			"c1.page_ns = " . Wikia::Utils->intval($ns),
			"c1.rev_timestamp between " . $self->dbr->quote( $self->params->{start_date} ) . " and " . $self->dbr->quote( $self->params->{end_date} ),
			"c1.is_redirect = 'N'",
			"c1.page_id not in (select distinct c2.page_id from events c2 where c2.rev_timestamp < ". $self->dbr->quote($self->params->{start_date}). " and c2.wiki_id = " . Wikia::Utils->intval($id) . " ) "
		];
		my $options = [];
		my $subquery = $self->dbr->sql("distinct(c1.page_id)", 'events c1', $where, $options);
		my $oRow = $self->dbr->query("select count(1) as cnt from ($subquery) as q");
		$res = $self->__make_value($oRow);
	} else {
		my $where = [
			"wiki_id = " . Wikia::Utils->intval($id),
			"page_ns = " . Wikia::Utils->intval($ns),
			"rev_timestamp between " . $self->dbr->quote( $self->params->{start_date} ) . " and " . $self->dbr->quote( $self->params->{end_date} ),
			"is_redirect = 'N'",
			"event_type = " . $self->CREATEPAGE_CATEGORY
		];
		my $options = [];		
		my $oRow = $self->dbr->select("count(1) as cnt", 'events', $where, $options);
		$res = $self->__make_value($oRow);
	}

	return $res;
}

sub _namespace_edits {
	my ( $self, $id, $ns ) = @_;

	my $where = [
		"wiki_id = " . Wikia::Utils->intval($id),
		"page_ns = " . Wikia::Utils->intval($ns),
		"rev_timestamp between " . $self->dbr->quote( $self->params->{start_date} ) . " and " . $self->dbr->quote( $self->params->{end_date} ),
		"( event_type = " . $self->EDIT_CATEGORY . " or event_type = " . $self->CREATEPAGE_CATEGORY . ") ",
		"is_redirect = 'N'"
	];
	my $options = [];
	my $oRow = $self->dbr->select("count(1) as cnt", 'events', $where, $options);
	return $self->__make_value($oRow);	
}

sub _wikia_stats {
	my ( $self, $id ) = @_;
	
	my $start_sec = time();
	say sprintf( "Proceed %s (%d) (%d) \n", $self->databases->{$id}, $id, $self->month );	
	
	# Wikia category && language
	my $cat = $self->dbc->get_wiki_cat($id);
	# language
	my $lang = $self->dbc->get_wiki_lang($id);
				
	# all namespaces for month and Wikia
	my $where = [ 
		"wiki_id = " . Wikia::Utils->intval($id),
		"rev_timestamp between " . $self->dbr->quote($self->params->{start_date}) . " and " . $self->dbr->quote($self->params->{end_date}),
		"is_redirect = 'N'" 
	]; 
	my $options = [];
	my $sth = $self->dbr->select_many("distinct page_ns", "events", \@$where, \@$options);
	if ($sth) {
		while( my $row = $sth->fetchrow_hashref ) {
			say sprintf( "\t Namespace: %d", Wikia::Utils->intval($row->{'page_ns'}) );	
			# 'namespaces' => 'all'
			my $all = $self->_namespace_all( $id, $row->{'page_ns'} );

			# 'namespaces' => 'newday'
			my $new = $self->_namespace_new( $id, $row->{'page_ns'} );

			# 'namespaces' => 'edits'
			my $edits = $self->_namespace_edits( $id, $row->{'page_ns'} );		
			
			# update stats;
			my @conditions = (
				"stats_date = " . $self->dbw->quote( $self->month ),
				"wiki_id = " . Wikia::Utils->intval( $id ),
				"page_ns = " . Wikia::Utils->intval( $row->{'page_ns'} )
			);
			my $q = $self->dbw->delete( 'namespace_monthly_stats', \@conditions );
						
			my %data = (
				"wiki_id" 		=> Wikia::Utils->intval( $id ),
				"page_ns"		=> Wikia::Utils->intval( $row->{'page_ns'} ),
				"wiki_cat_id"	=> Wikia::Utils->intval( $cat ),
				"wiki_lang_id"	=> Wikia::Utils->intval( $lang ),
				"stats_date"	=> Wikia::Utils->intval( $self->month ),

				"pages_all"		=> Wikia::Utils->intval( $all ),
				"pages_daily"	=> Wikia::Utils->intval( $new ),
				"pages_edits"	=> Wikia::Utils->intval( $edits )
			);
			my $ins = $self->dbw->insert( 'namespace_monthly_stats', "", \%data )
		}
		$sth->finish();
	}	
	
	my $end_sec = time();
	my @ts = gmtime($end_sec - $start_sec);
	say $self->databases->{$id} . " processed " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);	
}

sub calculate {
	my ( $self ) = @_;
	
	my $params = {
		'start_date'	=> Wikia::Utils->first_datetime($self->month),
		'end_date'		=> Wikia::Utils->last_datetime($self->month)
	};
	$self->params($params);
	$self->_build_databases();

	if ( !$self->databases ) {
		return;
	} 

	foreach my $num ( sort ( map { sprintf("%012u",$_) } ( keys %{$self->databases} ) ) ) {
		my $id = int $num;
		$self->_wikia_stats($id);
	}
}

sub __make_value {
	my ($self, $row, $key) = @_;
	$key = 'cnt' unless $key;
	return ( ( ref($row) eq "HASH" ) && ( keys %$row ) ) ? Wikia::Utils->intval($row->{$key}) : 0;
}

no Moose;
1;

package main;

use DateTime;

$|++;
my $help = undef;
my $month = DateTime->now()->strftime('%Y%m');
my $usedb = '*';
GetOptions( "help|?" => \$help, "usedb=s" => \$usedb, "month=i" => \$month ) or pod2usage(2);
pod2usage(1) if ( $help );

say "Process (with --month=" . $month . " and --usedb= " . $usedb . " ) . option) started";
my $start_sec = time();				
my $oStats = new Wikia::Namespace::Stats( { 'month' => $month, 'usedb' => $usedb } );
$oStats->calculate();
my $end_sec = time();
my @ts = gmtime($end_sec - $start_sec);
say "Finish after " .  sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);	

1;
__END__

=head1 NAME

namespace_stats.pl - calculate summary statistics per namespace for Wikia/Language/Category

=head1 SYNOPSIS

namespace_stats.pl [options]

 Options:
  --help            brief help message
  --usedb=<s>      	run script for selected Wikis
  --month=<nr>    	calculate stats for YYYYMM month

=head1 OPTIONS

=over 8

=item B<--help>

Print a brief help message.

=item B<--usedb=<s>>

Allowed options: 
	- usedb=177 - for Wikia with id = 177
	- usedb=+177 - for all Wikis with id > 177
	- usedb=wikicities,wowiki - for comma separated dbname of Wikis

=head1 DESCRIPTION

B<This programm> will calculate statistics per namespace for selected Wikis.
=cut
