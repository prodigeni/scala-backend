#!/usr/bin/perl -w

use common::sense;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";

package Wikia::Fix::FB35460;

use Moose;
with 'Wikia::Maintenance';

use Wikia::Settings;
use Wikia::WikiFactory;
use MediaWiki::API;

use Data::Printer;

has username => (
	is => "rw",
	isa => "Str"
);

has password => (
	is => "rw",
	isa => "Str"
);


sub prepare {
	my( $self ) = @_;

	my $settings = Wikia::Settings->instance;
	$self->username( $settings->variables()->{ "wgWikiaBotUsers" }->{ "bot" }->{ "username" } );
	$self->password( $settings->variables()->{ "wgWikiaBotUsers" }->{ "bot" }->{ "password" } );
}

my %messages = (); # private cache for messages

sub execute {
	my ( $self ) = @_;

	my $wiki = Wikia::WikiFactory->new( city_id => $self->current );
	my $url = $wiki->city_url . "api.php";

	my $mw = MediaWiki::API->new();
	$mw->{config}->{api_url} = $url;
	$mw->login( { lgname => $self->username, lgpassword => $self->password }  );
	say "Connecting to $url (wiki id: " . $wiki->city_id . ")";

	unless( $mw->{error}->{code} == 0 ) {
		# can't log in
		say "error ${ \$mw->{error}->{code} }: ${ \$mw->{error}->{details} }.";
	}
	else {
		say "success."
	}

	#
	# get message for wiki language
	#
	unless( exists $messages{ $wiki->city_lang } ) {
		print "Getting message broken-file-category for language " . $wiki->city_lang . "...: ";
		my $res = $mw->api( {
			action => "query",
			meta   => "allmessages",
			ammessages => "broken-file-category",
			amlang => $wiki->city_lang
		} );
		if( exists $res->{ 'query' }->{ 'allmessages' }->[ 0 ]->{ '*' } ) {
			$messages{ $wiki->city_lang } = $res->{ 'query' }->{ 'allmessages' }->[ 0 ]->{ '*' };
			say $messages{ $wiki->city_lang };
		}
		else {
			return; # skip this wiki, we don't know what to edit
		}
	}

	my $pagename = "Category:" . $messages{ $wiki->city_lang };
	my $page = $mw->get_page( { title => $pagename } );
	if( exists $page->{ "missing" } ) {
		my $timestamp = $page->{ "timestamp" };
		my $res = $mw->edit( {
			action => "edit",
			title  => $pagename,
			text   => "__HIDDENCAT__\n",
			bot    => 1,
			createonly => 1,
			summary => "Hide 'Pages with broken file links' category, see [http://www.mediawiki.org/wiki/Help:Tracking_categories]"
		} ) or say $mw->{error}->{code} . ': ' . $mw->{error}->{details};
		p $res;
	}
}

1;
binmode STDOUT, ':encoding(utf8)';
binmode STDERR, ':encoding(utf8)';
my $fix = Wikia::Fix::FB35460->new_with_options;
$fix->prepare;
$fix->run;
