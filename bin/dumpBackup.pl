#!/usr/bin/perl

use FindBin qw/$Bin/;
use lib "$Bin/../lib";

#
# public
#
use common::sense;
use Getopt::Long qw(:config pass_through);
use Data::Dumper;
use XML::Writer;
use IO::File;
use DateTime;

#
# private
#
use Wikia::LB;
use Wikia::WikiFactory;
use Wikia::Revision;

sub XMLWriter( $$$ ) {
	my( $siteinfo, $pages, $lang ) = @_;

	my $writer = new XML::Writer( OUTPUT => \*STDOUT, NEWLINES => 0, DATA_MODE => 1, DATA_INDENT => 2 );

	$writer->startTag( "mediawiki",
		"xmlns" => "http://www.mediawiki.org/xml/export-0.3/",
		"xmlns:xsi" => "http://www.w3.org/2001/XMLSchema-instance",
		"xsi:schemaLocation" => "http://www.mediawiki.org/xml/export-0.3/ http://www.mediawiki.org/xml/export-0.3.xsd",
		"version" => "0.3",
		"xml:lang" => $lang
	);
	$writer->startTag( "siteinfo" );
	for my $key ( keys %$siteinfo ) {
		if( $key ne "namespaces" ) {
			$writer->dataElement( $key, $siteinfo->{ $key } );
		}
		else {
			$writer->startTag( "namespaces" );
			for my $ns ( sort { $a <=> $b } keys %{ $siteinfo->{ $key } } ) {
				$writer->dataElement( "namespace", $siteinfo->{ $key }->{ $ns }, "key" => $ns );
			}
			$writer->endTag( "namespaces" );
		}
	}
	$writer->endTag( "siteinfo" );

	#
	# pages
	#
	for my $page ( @$pages ) {
		$writer->startTag( "page" );
		$writer->dataElement( "title", $page->{ "title" } );
		$writer->dataElement( "id", $page->{ "id" } );
		$writer->dataElement( "restrictions", $page->{ "restrictions" } ) if $page->{ "restrictions" };
		for my $rev ( @{ $page->{ "revisions" } } ) {
			$writer->startTag( "revision" );
			$writer->dataElement( "id", $rev->{ "id" } );
			$writer->dataElement( "timestamp", $rev->{ "timestamp" } );
			$writer->startTag( "contributor" );
			$writer->dataElement( "username", $rev->{ "contributor" }->{ "username" } );
			$writer->dataElement( "id", $rev->{ "contributor" }->{ "id" } );
			$writer->endTag( "contributor" );
			$writer->emptyTag( "minor" ) if $rev->{ "minor" };
			$writer->dataElement( "comment", $rev->{ "comment" } );
			$writer->startTag( "text", "xml:space" => "preserve" );
			$writer->characters( $rev->{ "text" } );
			$writer->endTag( "text" );
			$writer->endTag( "revision" );
		}
		$writer->endTag( "page" );
	}

	$writer->endTag( "mediawiki" );
	$writer->end;
}

#
# get restrictions from new table, page_restrictions
# (function not used currently for compatibility with mediawiki dumper)
#
#
sub restrictions {
	my( $page_id, $dbh ) = @_;

	my $sth = $dbh->prepare( "SELECT * FROM page_restrictions WHERE pr_page = ?" );
	my $result = undef;
	my @restrictions = ();

	$sth->execute( $page_id );
	while( my $row = $sth->fetchrow_hashref ) {
		push @restrictions, $row->{ "pr_type" } . "=" . $row->{ "pr_level" };
	}
	$result = join( ":", @restrictions ) if scalar @restrictions;

	return $result;
}

my $s_timestamp = DateTime->now();

#
# variable definition
#
my ( $db, $city_id, $output, $full, $current ) = undef;

GetOptions(
	"output=s"  => \$output,
	"city-id=i" => \$city_id,
	"full"      => \$full,
	"current"   => \$current,
	"city-db=s" => \$db
);

#
# some variables exclude each other so we have to inform people
#
if( defined $db && defined $city_id ) {
	die( "either --city-db or --city-id\n" );
}

if( defined $full && defined $current ) {
	die( "either --full or --current\n" );
}

#
# get data from WikiFactory
#
my $wikiFactory = undef;

if( defined $city_id ) {
	$wikiFactory = Wikia::WikiFactory->new( city_id => $city_id );
}
elsif( defined $db ) {
	$wikiFactory = Wikia::WikiFactory->new( city_dbname => $db );
}
else {
	print "you have to define one parameter --city-db=<dbname> or --city-id=<city_id>\n";
	exit( 1 );
}


printf "Dumping %s backup for %s (%d) on cluster %s\n",
	( $full ) ? "full" : "current",
	$wikiFactory->city_dbname,
	$wikiFactory->city_id,
	$wikiFactory->city_cluster;

$current = !$full;

#
# read namespaces
#
my $namespaces = $wikiFactory->namespaces();

#
# create siteinfo
#
my %siteinfo = (
	"generator"  => "Wikia Inc. perl dumper",
	"case"       => "first-letter",
	"base"       => $wikiFactory->variables->{ "wgServer" },
	"namespaces" => $namespaces,
	"sitename"   => $wikiFactory->variables->{ "wgSitename" }
);

#
# read all pages for wiki
#
my $lb = Wikia::LB->new();
my $dbh = $lb->getConnection( Wikia::LB::DB_SLAVE, undef, $wikiFactory->city_dbname );

#
# read current or all revisions for
#
my @pages = ();
my $sth = $dbh->prepare("SELECT * FROM page ORDER BY page_id" ); # hmm, that's it? { "mysql_use_result" => 1 }
$sth->execute();
while( my $row = $sth->fetchrow_hashref ) {
	my @revisions;
	#
	# for current read revisions from page_latest
	#
	if( $current ) {
		my $rev = new Wikia::Revision( db => $wikiFactory->city_dbname, id => $row->{ "page_latest" } );
		push @revisions, {
			"text"        => $rev->text,
			"id"          => $rev->id,
			"minor"       => $rev->minor_edit,
			"timestamp"   => $rev->timestamp_iso8601,
			"comment"     => $rev->comment,
			"contributor" => { "username" => $rev->user_text, "id" => $rev->user_id }
		};
	}
	else {
		#
		# read all revisions for that page
		#
		my $sth = $dbh->prepare( "SELECT * FROM revision WHERE rev_page = ?" );
		$sth->execute( $row->{ "page_id" } );
		while( my $row = $sth->fetchrow_hashref ) {
			my $rev = new Wikia::Revision( db => $wikiFactory->city_dbname, id => $row->{ "rev_page" } );
			push @revisions, {
				"text"        => $rev->text,
				"id"          => $rev->id,
				"minor"       => $rev->minor_edit,
				"timestamp"   => $rev->timestamp_iso8601,
				"comment"     => $rev->comment,
				"contributor" => { "username" => $rev->user_text, "id" => $rev->user_id }
			};
		}
	}
	push @pages, {
		"id"           => $row->{ "page_id" },
		"title"        => $row->{ "page_namespace" }
			? sprintf( "%s:%s", $namespaces->{ $row->{ "page_namespace" } }, $row->{ "page_title" } )
			: $row->{ "page_title" },
		"namespace"    => $row->{ "page_namespace" },
		"restrictions" => $row->{ "page_restrictions" },
		"revisions"    => \@revisions
	};
}

XMLWriter( \%siteinfo, \@pages, $wikiFactory->city_lang );
#print Dumper( \@pages );
my $e_timestamp = DateTime->now();
my $duration = $e_timestamp->subtract_datetime( $s_timestamp );
say $duration->in_units( "hours", "minutes", "seconds" );
