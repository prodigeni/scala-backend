#!/usr/bin/perl -w

#
# options
#
use strict;
use common::sense;

use FindBin qw/$Bin/;
use lib "$Bin/../lib/";

use Wikia::Settings;
use MediaWiki::API;

use Data::Dump;

my @tests = ( "http://mediawiki116.wikia.com/api.php" );

my $settings = Wikia::Settings->instance();

my $username = $settings->variables()->{ "wgWikiaBotUsers" }->{ "staff" }->{ "username" };
my $password = $settings->variables()->{ "wgWikiaBotUsers" }->{ "staff" }->{ "password" };
my $proxy = "http://dev-eloy:80/";

for my $url ( @tests ) {
	my $mw = MediaWiki::API->new();
	$mw->{config}->{api_url} = $url;;
	$mw->{ua}->proxy( "http", $proxy );
	$mw->login( { lgname => $username, lgpassword => $password }  );
	say "Connecting to $url... via proxy $proxy";

	unless( $mw->{error}->{code} == 0 ) {
		# can't log in
		say "error ${ \$mw->{error}->{code} }: ${ \$mw->{error}->{details} }.";
		die( $mw->{error}->{details} );
	}
	else {
		say "success."
	}

	my $pagename = "Watchlist";
	my $page = $mw->get_page( { title => $pagename } );
	unless( $page->{ "missing" } ) {
		my $timestamp = $page->{ "timestamp" };
		my $res = $mw->edit( {
			action => "edit",
			title => $pagename,
#			basetimestamp => $timestamp,
			text => $page->{ "*" } . "\n--~~~~"
		} ) or die $mw->{error}->{code} . ': ' . $mw->{error}->{details};

		dd( $res );
	}
}
