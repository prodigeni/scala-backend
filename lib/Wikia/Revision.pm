package Wikia::Revision;

use Wikia::LB;
use Wikia::ExternalLB;
use Wikia::Utils;
use Data::Dumper;
use Compress::Zlib;
use PHP::Serialization qw/serialize unserialize/;
use DateTime;
use Data::Types        qw(:int);
use Moose;

=head1 NAME

Wikia::Revision - MediaWiki Revision class for Wikia scripts

=head1 VERSION

version 0.01

=head1 SYNOPSIS

  use Wikia::Revision;

  #
  # get first revision from firefly database
  #
  my $rev = new Wikia::Revision( db => "firefly", id => 1 );

  #
  # get timestamp of revision
  #
  my $timestamp = $rev->timestamp;

  #
  # get text of revision
  #
  my $text = $rev->text;

=cut


has "db"                => ( is => "rw", "isa" => "Str", required => 1 );
has "id"                => ( is => "rw", "isa" => "Int", required => 1 );
has "user_text"         => ( is => "rw", "isa" => "Str", lazy_build => 1 );
has "user_id"           => ( is => "rw", "isa" => "Int", lazy_build => 1 );
has "timestamp"         => ( is => "rw", "isa" => "Str", lazy_build => 1 );
has "timestamp_iso8601" => ( is => "rw", "isa" => "Str", lazy_build => 1 );
has "deleted"           => ( is => "rw", "isa" => "Int", lazy_build => 1 );
has "minor_edit"        => ( is => "rw", "isa" => "Bool", lazy_build => 1 );
has "comment"           => ( is => "rw", "isa" => "Str", lazy_build => 1 );
has "text"              => ( is => "rw", "isa" => "Str", lazy_build => 1 );
has "flags"             => ( is => "rw", "isa" => "HashRef", lazy_build => 1 );
has "debug"             => ( is => "rw", lazy_build => 1 );


has "dbh" => (
	is            => "rw",
	lazy_build    => 1,
	documentation => "DBI database handler"
);

has "row" => (
	is            => "rw",
	default       => undef,
	documentation => "database row with revision definition"
);

has "master" => (
	is            => "rw",
	isa           => "Bool",
	default       => 0,
	documentation => "set to true/1 if master connection is used for reading revision data"
);


=head1 METHODS

=head2 _load_revision

	load revision from database

=cut
sub _load_revision {
	my ( $self ) = @_;

	unless( defined $self->row ) {
		my $dbh = $self->dbh;
		my $sth = $dbh->prepare( qq{SELECT * FROM revision, text WHERE rev_id = ? AND rev_text_id = old_id LIMIT 1} );
		$sth->execute( $self->id );
		my $row = $sth->fetchrow_hashref;

		#
		# if row doesn't exists use master connection
		#
		if( !exists( $row->{"rev_id" } ) && ! $self->master ) {
			$self->master( 1 );
			$self->_build_dbh();
			my $sth = $dbh->prepare( qq{SELECT * FROM revision, text WHERE rev_id = ? AND rev_text_id = old_id LIMIT 1} );
			$sth->execute( $self->id );
			my $row = $sth->fetchrow_hashref;
		}
		$self->row( $row );
	}
}

=head2 _build_user_id

	lazy builder for $rev->user_id -- user id of contributor

=cut
sub _build_user_id {
	my ( $self ) = @_;

	$self->_load_revision;
	$self->user_id( $self->row->{ 'rev_user' } );
}

=head2 _build_minor_edit

	lazy builder for $rev->minor_edit -- is edit is minor edit or not

=cut
sub _build_minor_edit {
	my ( $self ) = @_;

	$self->_load_revision;
	$self->minor_edit( $self->row->{ 'rev_minor_edit' } );
}

=head2 _build_comment

	lazy builder for $rev->comment -- comment for edit

=cut
sub _build_comment {
	my ( $self ) = @_;

	$self->_load_revision;
	$self->comment( exists $self->row->{ 'rev_comment' } ? $self->row->{ 'rev_comment' } : undef );
}

=head2 _build_user_text

	lazy builder for $rev->user_text -- user name of contributor

=cut
sub _build_user_text {
	my ( $self ) = @_;

	$self->_load_revision;
	$self->user_text( exists $self->row->{ 'rev_user_text' } ? $self->row->{ 'rev_user_text' } : undef );
}

=head2 _build_timestamp

	lazy builder for $rev->timestamp -- timestamp in mediawiki format

=cut
sub _build_timestamp {
	my ( $self ) = @_;

	$self->_load_revision;
	$self->timestamp( exists $self->row->{ 'rev_timestamp' } ? $self->row->{ 'rev_timestamp' } : undef );
}

=head2 _build_debug

	lazy builder for $class->debug -- set/unset debug variable

=cut
sub _build_debug {
	my( $self ) = @_;
	if( exists $ENV{ "DEBUG" } ) {
		$self->debug( to_int( $ENV{ "DEBUG" } ) );
	}
	else {
		$self->debug( 0 );
	}
}

=head2 _build_timestamp_iso8601

	lazy builder for $rev->timestamp_iso8601 -- timestamp in iso8601 format

=cut
sub _build_timestamp_iso8601 {
	my ( $self ) = @_;

	my $t = $self->timestamp;
	my( $y, $m, $d, $h, $n, $s ) = $t =~ /(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})/;
	my $dt = DateTime->new(
		year       => $y,
		month      => $m,
		hour       => $h,
		minute     => $n,
		second     => $s,
		nanosecond => 0,
		time_zone  => "UTC"
	);
	$self->timestamp_iso8601( $dt->iso8601 );
}

=head2 _build_flags

	lazy builder for $rev->flags -- revision flags

=cut
sub _build_flags {
	my ( $self ) = @_;

	$self->_load_revision;

	my %flags = ();
	for my $flag ( split( ",", $self->row->{ 'old_flags' } ) ) {
		$flags{ $flag } = 1;
	}
	$self->flags( \%flags );
}

sub _build_text {
	my ( $self ) = @_;
	$self->_load_revision;

	#
	# check in flags if it is local or remote storage
	#
	if( exists $self->row->{ "old_text" } ) {
		my $text = $self->row->{ "old_text" };

		if( exists $self->flags->{ "external" } ) {
			#
			# get revision text from external blobs database
			#
			my ( $store, $cluster, $id ) = $self->row->{ "old_text" } =~ m|([^/]+)//([^/]+)/(.+)|;
			if ( $cluster && $cluster =~ m/^blobs/ ) {
				$text = $self->_load_text( $store, $cluster, $id );
			} else {
				$text = '';
			}
		}

		#
		# handle object revisions
		#
		if( exists $self->flags->{ "object" } ) {
			my $obj = unserialize( $text );
			$text = $obj->{ "mItems" };
			if( $obj->{ "mCompressed" } ) {
				$self->flags->{ "gzip" } = 1;
			}
		}

		#
		# handle gzipped revisions
		#
		if( exists $self->flags->{ "gzip" } ) {
			my( $inf, $status ) = inflateInit( -WindowBits => 0 - MAX_WBITS );
			( $text, $status ) = $inf->inflate( $text );
			if( ( $status != Z_OK ) && ( $status != Z_STREAM_END ) ) {
				$text = "";
			}
		}

		$self->text( $text );
	}
	else {
		$self->text( '' );
	}
}

sub _build_dbh {
	my ( $self ) = @_;
	my $dbh = undef;
	if( $self->master ) {
		$dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, $self->db );
	}
	else {
		$dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, $self->db );
	}
	$self->dbh( $dbh ) if $dbh;
}

sub _load_text {
	my ( $self, $store, $cluster, $id ) = @_;

	my $fname = ( caller( 0 ) )[ 3 ];

	say STDERR "$fname: connecting to external cluster $cluster" if $self->debug > 2;
	my $dbh = Wikia::ExternalLB->instance->getConnection( Wikia::LB::DB_SLAVE, "blobs", $cluster );
	say STDERR "$fname: getting blob from cluster: $cluster with id $id" if $self->debug > 2;

	my $sth = $dbh->prepare( "SELECT * FROM blobs WHERE blob_id = ? LIMIT 1" );
	$sth->execute( $id );
	my $row = $sth->fetchrow_hashref;
	return exists $row->{ "blob_text" } ? $row->{ "blob_text" } : "";
}

sub count_words($;$) {
	my ($self, $text) = @_;
	my $words = 0;

	return $words unless ($text);

	# clear text
	my $parse_text = Wikia::Utils->parse_article($text);
	$parse_text =~ s/\{x\}/x/g;

	# count number as one word
	$parse_text =~ s/\d+[,.]\d+/number/g ;
	# links -> text + strip hidden part of links

	$parse_text =~ s/\[\[ (?:[^|\]]* \|)? ([^\]]*) \]\]/$1/gxo;
	while ( $parse_text =~ m/([A-Za-z\xC0-\xFF0-9]+)/g ) {
		$words++;
	}

	undef($parse_text);
	return $words;
}

sub parse_links($;$$$) {
	my ($self, $wikia) = @_;

	my $dbr = new Wikia::DB( {"dbh" => $self->dbh } );

	my ($pagelinks, $imagelinks, $videolinks) = ();
	$pagelinks = $imagelinks = $videolinks = 0;

	if ( $wikia ) {
		my $imageTag = Wikia::Utils->getImagetag($wikia->{city_lang});
		# use api to get namespace aliases
		if ( $wikia->{server} ) {
			my $namespaces = Wikia::Utils->get_namespace_by_server($wikia->{server});
			my $image_regex = "(" . join("|", @{$namespaces->{Wikia::Utils::NS_IMAGE} || ['Image']}) . ")";
			my $video_regex = "(" . join("|", @{$namespaces->{Wikia::Utils::NS_VIDEO} || ['Video']}) . ")";
			my $links = {};
			while ($self->text =~ /\[\[([^\]]*)\]\]/go) {
				my $a = $1;
				#---
				if ($a =~ /^[^\:]+$/) {
					$links->{uc($a)} = 1 unless($links->{uc($a)});
				}
				# check video objects
				if ($a =~ /^$video_regex\:((.+)(.*))/gio) {
					my $imageRow = $dbr->get_image_by_name([split(/\|/, $2)]->[0]);
					$videolinks++ if ( $imageRow );
					$videolinks++ if ( $2 =~ /commons\:/gio );
				}

				if ($a =~ /^$image_regex\:((.+\.[a-z]{3})(.*))/gio) {
					my $imageRow = $dbr->get_image_by_name($2);
					$imagelinks++ if ( $imageRow );
					$imagelinks++ if ( $2 =~ /commons\:/gio );
				}
			}
			my $lprev = "!@#$%^&*" ;
			foreach my $lcurr (sort keys %$links) {
				$pagelinks++  if ($lcurr ne $lprev);
				$lprev = $lcurr;
			}
		}
	}

	return ($pagelinks, $imagelinks, $videolinks);
}

1;
