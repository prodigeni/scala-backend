package Wikia::Hooks::Twitter;

require LWP::UserAgent;

use YAML::XS;
use Moose::Role;
use IO::File;
use Data::Dumper;

use Wikia::Settings;
use Wikia::Utils;
use Wikia::User;

use strict;
use warnings;

use HTTP::Cookies;

use FindBin qw/$Bin/;

our $VERSION = "0.01";

use Net::Twitter;

=head1 NAME

Wikia::Hooks::Twitter - use Twitter API 

=head1 VERSION

version $VERSION

=cut

#
# builders
#
sub twitter_oauth {
	my ( $self, $params ) = @_;
	
# hack - taken from Wikia::Settings
 	my $CONSUMER_KEY = 'dDT3aK1zkDXoFQpjAvjv0A';
	my $CONSUMER_SECRET = '60z3ujszzIAfZ6ePigyo1O1omWvUyrv7esZIOhKtNg';
	my $ACCESS_TOKEN = '267073997-tASx4U57DoDemXMI1HRia1ZOKtGFmktx4KjxXPNr';
	my $ACCESS_TOKEN_SECRET = 'FoGdz8ozZUbNU5cxxIum2JNqM1H6h0Ebfr3vsMXHc';
	
#	my $oMW = Wikia::Utils->json_decode($msg);
	my $res = 0;
	if ( UNIVERSAL::isa($params, 'HASH') ) {
		my $proxy = 'http://squid-proxy.local:3128';

		$ENV{HTTP_PROXY}               = $proxy;
		$ENV{HTTP_PROXY_USERNAME}      = '';
		$ENV{HTTP_PROXY_PASSWORD}      = '';
		
		$ENV{HTTPS_PROXY}               = $proxy;
		$ENV{HTTPS_PROXY_USERNAME}      = '';
		$ENV{HTTPS_PROXY_PASSWORD}      = '';
		
		my $tClient = Net::Twitter->new(
		  traits   => [qw/OAuth API::REST/],
		  consumer_key        => $CONSUMER_KEY,
		  consumer_secret     => $CONSUMER_SECRET,
		  access_token        => $ACCESS_TOKEN,
		  access_token_secret => $ACCESS_TOKEN_SECRET,
		);

		$res = $tClient->update( $params->{'text'} );
	}
	
	return $res;
}

sub twitter {
	my ( $self, $params ) = @_;
	
	my $t_user = "glee_wikia_comm"; # taken from param
	my $t_pass = "molirzondzi"; # taken from param
	
	my $ua = LWP::UserAgent->new;
		
	my $proxy = 'http://squid-proxy.local:3128';
	$ua->proxy(['http'], $proxy);

	$proxy = 'http://squid-proxy.local:3128';
	$ENV{HTTPS_PROXY}               = $proxy;
	$ENV{HTTPS_PROXY_USERNAME}      = '';
	$ENV{HTTPS_PROXY_PASSWORD}      = '';

	my $t_url = 'http://mobile.twitter.com/';
	my $t_sess_new = 'https://mobile.twitter.com/session/new';
	my $t_sess = 'https://mobile.twitter.com/session';
	
	my $user_agent = "Wikia-Hooks-Twitter 0.0.1";
		
	my $res = 0;
	if ( UNIVERSAL::isa($params, 'HASH') ) {
		my @header = ( 'Referer' => $t_url,'User-Agent' => $user_agent);

		# set cookie 
		my $cookie_file = "cookies.dat";
		my $cookie_jar = HTTP::Cookies->new( file => $cookie_file, autosave => 1, ignore_discard => 1 );
		$ua->cookie_jar($cookie_jar);
		
		# get authenticity token
		my $response = $ua->get( $t_sess_new, @header );
		my $form_data = $response->content;

		#  parse response to find token
		$form_data =~ s/\n//g;
		$form_data =~ /input name="authenticity_token" type="hidden" value="(.*?)"/ig;
		my $auth_token = $1;

		# logged in to twitter
		my $login_params = [ 'username' => $t_user, 'password' => $t_pass, 'authenticity_token' => $auth_token ];
		$response = $ua->post( $t_sess, $login_params, @header);

		# created cookie so save it 
		$cookie_jar->extract_cookies( $response );
		$cookie_jar->save;

		# see page after logged in
		$response = $ua->get( $t_url, @header );
		$form_data = $response->content;

		# find token
		$form_data =~ s/\n//g;
		$form_data =~ /input name="authenticity_token" type="hidden" value="(.*?)"/ig;
		$auth_token = $1;

		# and finally posting tweet on twitter
		@header = ( 'Referer' => $t_url, 'User-Agent' => $user_agent );
		$response = $ua->post( $t_url, [ 'tweet[text]' => $params->{'text'}, 'authenticity_token' => $auth_token ], @header);
		
		$form_data = $response->content;
		unlink($cookie_file);
		
		$res = 1;
	}
	
	return $res;
}

1;
