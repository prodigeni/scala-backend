package Wikia::Log;

use Wikia::LB;
use Wikia::DB;
use Wikia::ExternalLB;
use Wikia::Utils;

use Data::Dumper;
use Compress::Zlib;
use PHP::Serialization qw/serialize unserialize/;
use DateTime;
use Moose;

use Scalar::Util 'looks_like_number';

=head1 NAME

Wikia::Log - log for Wikia scripts

=head1 VERSION

version 0.01

=head1 SYNOPSIS

  use Wikia::Log;

  #
  # get title from firefly database and id = 1
  #
  my $log = new Wikia::Log( name => "event" );

  #
  # set log timestamp
  #
  $log->update();
  #
  # used table:
CREATE TABLE `script_log` (
  `logname` varchar(50) NOT NULL,
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (logname)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

=cut

has "dbh" => (
	is            => "rw",
	lazy_build    => 1,
	documentation => "DBI database handler",
);

has "dbs" => (
	is	=> "rw",
	isa => "Wikia::DB",
	lazy_build => 1
);

has "name" => ( is => "rw", isa => "Str", required => 1 );

=head1 METHODS
=cut

sub _build_dbh {
	my ( $self ) = @_;
	my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS );

	$self->dbh( $dbh ) if $dbh;
}

sub _build_dbs {
	my ( $self ) = @_;

	my $dbs = Wikia::DB->new( {"dbh" => $self->dbh } );
	$self->dbs( $dbs );
}

sub update {
	my $self = shift;

	if ( !$self->name ) {
		return 0;
	}

	my $oRow = $self->select();

	my $where = [ "logname = " . $self->dbs->quote( $self->name ) ];

	my $res = 0;
	if ( defined ( $oRow->{logname} ) ) {
		my %data = ( '-ts' => 'now()' );
		$res = $self->dbs->update( '`noreptemp`.`script_log`', $where, \%data );
	} else {
		my %data = ( 'logname' => $self->name, '-ts' => 'now()' );
		my @options = ();
		$res = $self->dbs->insert( '`noreptemp`.`script_log`', "", \%data, \@options, 1 );
	}

	return $res;
}

sub select {
	my ( $self ) = @_;
	my $where = [ "logname = " . $self->dbs->quote( $self->name ) ];
	my $options = [ ' LIMIT 1 ' ];

	return $self->dbs->select("logname, unix_timestamp(ts) as ts_log", '`noreptemp`.`script_log`', $where, $options);
}

1;
