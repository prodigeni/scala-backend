package Wikia::Title;

use Wikia::LB;
use Wikia::ExternalLB;
use Wikia::Utils;
use Wikia::WikiFactory;
use Data::Dumper;
use Compress::Zlib;
use PHP::Serialization qw/serialize unserialize/;
use DateTime;
use Moose;

use Scalar::Util 'looks_like_number';

=head1 NAME

Wikia::Revision - MediaWiki Title class for Wikia scripts

=head1 VERSION

version 0.01

=head1 SYNOPSIS

  use Wikia::Title;

  #
  # get title from firefly database and id = 1
  #
  my $page = new Wikia::Title( db => "firefly", id => 1 );

  #
  # get name revision
  #
  my $timestamp = $page->name;


=cut


has "db" => ( is => "rw", "isa" => "Str", required => 1 );
has "from_id" => ( is => "rw", isa => "Str", predicate => "has_id", "trigger" => sub {
	my ( $self, $id ) = @_;
	$self->_load_page( {"page_id" => $id});
} );

has "from_title" => ( is => "rw", isa => "HashRef", predicate => "has_title", "trigger" => sub {
	my ( $self, $title ) = @_;
	$self->_load_page( {"page_name" => $title->{name}, "page_namespace" => $title->{ns} } );
} );


has "dbh" => (
	is            => "rw",
	lazy_build    => 1,
	documentation => "DBI database handler"
);

has "master" => (
	is            => "rw",
	isa           => "Bool",
	default       => 0,
	documentation => "set to true/1 if master connection is used for reading revision data"
);

has "title"     	=> ( is => "rw", isa => "Str" );
has "namespace" 	=> ( is => "rw", isa => "Int" );
has "id"  			=> ( is => "rw", isa => "Int" );
has "is_redirect" 	=> ( is => "rw", isa => "Int" );
has "is_new"		=> ( is => "rw", isa => "Int" );
has "latest"		=> ( is => "rw", isa => "Int" );
has "touched"		=> ( is => "rw", isa => "Str" );
has "length"		=> ( is => "rw", isa => "Int" );
has "dbtitle"		=> ( is => "rw", isa => "Str", lazy_build => 1 );
has "nstext"		=> ( is => "rw", isa => "Str", lazy_build => 1 );
has "variables"		=> ( is => "rw", isa => "HashRef", lazy_build => 1 );
has "server"		=> ( is => "rw", isa => "Str", lazy_build => 1 );
has "sitename"		=> ( is => "rw", isa => "Str", lazy_build => 1 );
has "article_name"  => ( is => "rw", isa => "Str", lazy_build => 1 );
has "article_path"  => ( is => "rw", isa => "Str", lazy_build => 1 );
has "url"			=> ( is => "rw", isa => "Str", lazy_build => 1 );

=head1 METHODS

=head2 _load_page

	load page from database

=cut

sub _load_page {
	my ( $self, $data ) = @_;
	
	return if( $self->has_id && $self->has_title );

	if ( defined $data ) {
		my $dbh = $self->dbh();
		
		my @w = ();
		foreach (keys %$data) {
			my $value = $_ . " = " . $dbh->quote($data->{$_}); 
			push @w, $value;
		}		
		
		my $q = sprintf("SELECT * FROM page WHERE %s", join(" and ", @w) );
		my $sth = $dbh->prepare( $q );
		$sth->execute();
		my $row = $sth->fetchrow_hashref;

		#
		# if row doesn't exists use master connection
		#
		if( !exists( $row->{"rev_id" } ) && ! $self->master ) {
			$self->master( 1 );
			$dbh = $self->dbh();
			my $sth = $dbh->prepare($q);
			$sth->execute();
			$row = $sth->fetchrow_hashref;
		}
		
		if ( $row ) {
			$self->title    	( $row->{'page_title'} );
			$self->namespace	( $row->{'page_namespace'} );
			$self->id			( $row->{'page_id'} );
			$self->latest 		( $row->{'page_latest'} );
			$self->is_redirect	( $row->{'page_is_redirect'} );
			$self->is_new		( $row->{'page_is_new'} ); 
			$self->touched		( $row->{'page_touched'} ); 
			$self->length		( $row->{'page_len'} );
		}
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

sub _build_dbtitle {
	my ( $self ) = @_;
	
	my $title = $self->title;
	$title =~ s/ /\_/gi; 	
	$self->dbtitle( $title );
}

sub _build_nstext {
	my ( $self ) = @_;
	
	my $WF = Wikia::WikiFactory->new( city_dbname => $self->db );
	my $NS = $WF->namespaces();
	my $nstext = ( $self->namespace > 0 ) ? $NS->{ $self->namespace } : '';
	$nstext = '' unless $nstext;
	
	$self->nstext( $nstext );
}

sub _build_variables {
	my ( $self ) = @_;
	
	my $WF = Wikia::WikiFactory->new( city_dbname => $self->db );
	$self->variables( $WF->variables() );
}

sub _build_server {
	my ( $self ) = @_;
	$self->variables();
	$self->server ( $self->variables->{ 'wgServer' } );
}

sub _build_sitename {
	my ( $self ) = @_;
	$self->variables();
	$self->sitename ( $self->variables->{ 'wgSitename' } );
}

sub _build_article_name {
	my ( $self ) = @_;
	
	$self->nstext();
	$self->dbtitle();
	$self->article_name ( $self->namespace > 0 ) ? sprintf("%s:%s", Wikia::Utils->urlencode( $self->nstext ), $self->dbtitle) : $self->dbtitle;
}

sub _build_article_path {
	my ( $self ) = @_;
	
	#$self->variables();
	my $path = $self->variables()->{'wgArticlePath'};
	$path = '/wiki/$1' unless $path;

	my @vars = ($path =~ m/(\$\w+)[^\w]*/g);

	if( scalar(@vars) > 0 ) {
		foreach ( @vars ) {
			my $var = $_; my $key = $var;
			$key =~ s/^\s+|\s+$//g;
			$key =~ s/\$//g;
			
			if ( !looks_like_number( $key ) ) {
				my $replace = $self->variables->{ $key };
				if( $replace ) {
					$path =~ s/\$$key/$replace/g;
				}
			}
		}
	}	
					
	$self->article_path ( $path );	
}

sub _build_url {
	my ( $self ) = @_;
	
	my $article = $self->article_name;
	my $path = $self->article_path;
	$path =~ s/\$1/$article/g; 

	my $url = $self->server . $path;
	$self->url($url);
}

sub _build_lastedit {
	my( $self ) = @_;
	
	my $sth = $self->dbh->prepare( qq{SELECT rev_timestamp FROM revision, page WHERE page_title = ? AND page_namespace = ? and page_latest = rev_id} );
	$sth->execute( $self->title, $self->namespace );
	my $row = $sth->fetchrow_hashref;
	$self->lastedit( $row->{'rev_timestamp'} );
}

1;
