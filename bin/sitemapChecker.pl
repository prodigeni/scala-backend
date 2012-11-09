#!/usr/bin/env perl

package Wikia::SitemapChecker;

use feature "say";
use LWP::UserAgent;
use Data::Dump;
use XML::LibXML;
use Try::Tiny;
use Compress::Zlib;
use Moose;

has city_url => ( is => "rw", isa => "Str", documentation => "Main url for wiki." );
has sitemap_idx => ( is => "rw", isa => "Str", documentation => "Url for sitemap index" );
has sitemaps => ( is => "rw", isa => "ArrayRef", documentation => "Basket for storing urls for all sitemap files grabbed from sitemap index file" );

sub parse_robots_txt {
	my ( $self ) = @_;

	my $url = $self->city_url . "robots.txt";
	say "Getting $url";

	my $ua = LWP::UserAgent->new();
	$ua->timeout( 180 );
	my $txt = $ua->get( $url )->as_string;

	#
	# find sitemap string
	#
	my ( $sitemap ) = $txt =~ m!^Sitemap: (.+)\b!ms;
	if( defined $sitemap ) {
		$self->sitemap_idx( $sitemap );
	}
	else {
		$self->sitemap_idx( $self->city_url . "sitemap.xml" );
	}
}

sub parse_index {
	my ( $self ) = @_;

	say "Getting ${ \$self->sitemap_idx}";

	my $ua = LWP::UserAgent->new();
	$ua->timeout( 180 );
	my $response = $ua->get( $self->sitemap_idx );
	if( $response->is_success ) {
		my $doc = XML::LibXML->load_xml( string => $response->content );
#		if( $doc->is_valid ) {
			my $xmlschema = XML::LibXML::Schema->new( location => "http://www.sitemaps.org/schemas/sitemap/0.9/siteindex.xsd" );
			try {
				$xmlschema->validate( $doc );
			}
			catch {
				say "Sitemap from ${ \$self->sitemap_idx } is not valid sitemap file according to xsd schema";
			};
#		}
#		else {
#			say "Sitemap from ${ \$self->sitemap_idx } is not valid XML document"
#		}
		my @s = ();
		for my $node ( @{ $doc->getElementsByTagName( "sitemap" ) } ) {
			my $loc = $node->getChildrenByTagName( "loc" );
			push @s, $loc->string_value();
		}
		$self->sitemaps( \@s );
	}
}

sub parse_sitemaps {
	my ( $self ) = @_;

	for my $sitemap ( @{ $self->sitemaps } ) {
		say "Getting $sitemap";

		my $ua = LWP::UserAgent->new();
		$ua->timeout( 180 );
		my $response = $ua->get( $sitemap );
		if( $response->is_success ) {
			my $txt = "";
			if( $response->header( "content-type" ) eq "application/x-gzip" ) {
				 $txt = Compress::Zlib::memGunzip( $response->content );
			}
			else {
				$txt = $response->content;
			}
			#
			# check sitemap size, there is limit for that
			#
			use bytes;
			my $size = bytes::length( $txt );
			no bytes;
			say "Sitemap is $size bytes long";

			my $doc = XML::LibXML->load_xml( string => $txt );
		}
	}
}

sub check {
	my ( $self ) = @_;

	$self->parse_robots_txt();
	$self->parse_index();
	$self->parse_sitemaps();
}

no Moose;
1;


package main;

use FindBin qw/$Bin/;
use lib "$Bin/../lib";

#
# public
#
use Pod::Usage;
use Getopt::Long;
use Data::Dump;

#
# private
#
use Wikia::WikiFactory;
use Wikia::LB;

my $modes = qw(random bigest);
my $mode = "random";
my $help = undef;

GetOptions( "mode=s" => \$mode, "help" => \$help );

#
# get random wiki
#
my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::EXTERNALSHARED );

my $sth = $dbh->prepare( "SELECT city_id, city_url FROM city_list WHERE city_public = 1 ORDER BY rand() LIMIT 1" );
$sth->execute();
my $row = $sth->fetchrow_hashref;
dd( $row );

my $sitemap = Wikia::SitemapChecker->new( city_url => $row->{ "city_url" } );
$sitemap->check();
1;
