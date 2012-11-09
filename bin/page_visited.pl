#!/usr/bin/perl -w

use common::sense;
use feature "say";

use FindBin qw/$Bin/;
use lib "$Bin/../lib/";
use Getopt::Long;
use Time::HiRes qw(gettimeofday tv_interval);
use Pod::Usage;
use DateTime;

use Wikia::Utils;
use Wikia::DB;
use Wikia::LB;

package Wikia::PageVisited;

=pod page_visited
CREATE TABLE `page_visited` (
  `article_id` int(9) NOT NULL,
  `count` int(8) NOT NULL,
  `prev_diff` int(8) NOT NULL default '0',
  PRIMARY KEY  (`article_id`),
  KEY `page_visited_cnt_inx` (`count`),
  KEY `pv_changes` (`prev_diff`,`article_id`)
) ENGINE=InnoDB */
=cut

use Moose;

has "dbname"	=> ( is => "rw", "isa" => "Str", required => 1 );
has "cityid"	=> ( is => "rw", "isa" => "Int", required => 1 );
has "days"	 	=> ( is => "rw", "isa" => "Int", required => 1 );
has "insert"	=> ( is => "rw", "isa" => "Int", required => 1 );
has "dbr" 	 	=> ( is => "rw", lazy_build => 1 ); 
has "dbw"  	 	=> ( is => "rw", lazy_build => 0 ); 

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

	my $dbh = $lb->getConnection( Wikia::LB::DB_MASTER, undef, $self->dbname );
	$dbh->{ "mysql_auto_reconnect" } = 1;
	
	my $dbw = new Wikia::DB( { "dbh" => $dbh } );
	$self->dbw( $dbw ) if $dbw;
}

sub run {
	my ( $self ) = @_;

	my $days = sprintf("%s", DateTime->now()->subtract( days => $self->days )->strftime('%Y%m%d'));

	if ( !$self->dbr ) {
		say "Cannot connect to db ";
		return 0;
	}
	
	my $data = [];
	my $y = 0;
	my $x = 0;

	my @where = (
		'pv_city_id = ' . Wikia::Utils->intval($self->cityid),
		'pv_use_date = ' . Wikia::Utils->intval($days)
	);
	my @options = ('GROUP BY pv_page_id');
	
	my $sth = $self->dbr->select_many(
		"pv_page_id, sum(pv_views) as cnt",
		"page_views_articles",
		\@where, 
		\@options
	);
	if ($sth) {
		while(my ($page_id, $cnt) = $sth->fetchrow_array()) {
			my $page = [ $page_id, $cnt, 0 ];
			$data->[$y] = [] unless ( $data->[$y] );
			$y++ if ( ( $x > 0 ) && ( scalar ( @{$data->[$y]} )  % $self->insert ) == 0 ) ;
			
			# push in data array
			my $row = join( ',', map { $self->dbr->quote($_) } @$page );
			if ( $row ) {
				push @{$data->[$y]} , "(" . $row . ")";
				$x++;
			}				
		}
		$sth->finish();
	}

	$self->_build_dbw();	
	
	if ( !$self->dbw ) {
		say "Cannot connect to Wiki database \n";
		return 0;
	}
	
	say "Found " . scalar @{$data} . " records to copy to local database ";
	if ( scalar @{$data} ) {
		# insert
		foreach my $k ( @{$data} ) {
			my $values = join(",", map { $_ } @$k);
			if ( $values ) {
				my $sql = "INSERT IGNORE INTO page_visited ( article_id, count, prev_diff ) values ";
				$sql .= $values;
				$sql .= " ON DUPLICATE KEY UPDATE count = count + values(count), prev_diff = values(count) ";
				$sql = $self->dbw->execute($sql);
			}
		}
	}

	return 1;
}

no Moose;
1;

package main;

$|++;
my $help = undef;
my $usedb = '*';
my $days = 1;
my $insert = 50;
GetOptions( "help|?" => \$help, "usedb=s" => \$usedb, "days=i" => \$days, 'insert=i' ) or pod2usage(2);
pod2usage(1) if ( $help );

say "Process (with --days=" . $days . " and --usedb= " . $usedb . " ) . option) started";
my $start_sec = time();				

my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED );
my $where_db = [
	"city_public = 1", 
	"city_url not like 'http://techteam-qa%'"
];

if ( $usedb && $usedb =~ /\+/ ) {
	# usedb=+177
	$usedb =~ s/\+//i;
	push @{$where_db}, "city_id > " . Wikia::Utils->intval($usedb);
} elsif ( $usedb && $usedb ne "*" ) {
	# dbname=wikicities, [ ... ]
	my @use_dbs = split /,/, $usedb;
	push @{$where_db}, "city_dbname in (".join(",", map { $dbh->quote($_) } @use_dbs).")";
} 


my $sth = $dbh->prepare( "SELECT city_id, city_dbname FROM city_list WHERE " . join (' AND ', @$where_db ). " ORDER BY city_id" );
if ( $sth->execute() ) {
	while( my $row = $sth->fetchrow_hashref ) {
		my $s = time();	
		my $oPV = new Wikia::PageVisited( { 'days' => $days, 'insert' => $insert, 'dbname' => $row->{'city_dbname'}, 'cityid' => $row->{'city_id'} } );
		$oPV->run();
		my $e = time();
		my @ts = gmtime($e - $s);
		say $row->{'city_dbname'} . " finishes after " .  sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);	
	}
	$sth->finish;
}

my $end_sec = time();
my @ts = gmtime($end_sec - $start_sec);
say "Finish after " .  sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);	

1;
__END__

=head1 NAME

page_visited.pl - copy daily PV to the page_visited table on local database

=head1 SYNOPSIS

page_visited.pl [options]

 Options:
  --help            brief help message
  --usedb=<s>      	run script for selected Wikis
  --days=<nr>    	number of days to move
  --insert=<nr>		number of multi-insert sql packages

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

B<This programm> will copy summary pageviews for selected Wikis.
=cut
