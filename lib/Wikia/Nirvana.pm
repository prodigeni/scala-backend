package Wikia::Nirvana;

use Net::HTTP::Spore;
use Data::Dumper;
use Moose;
use FindBin qw/$Bin/;
use File::Basename;
use JSON::XS;


has "wiki_url"  => ( is => "rw", isa => "Str" );
has "client"    => ( is => "rw", isa => "Object", lazy_build => 1 );
has "spec_file" => ( is => "rw", isa => "Str", default => sub { return dirname( __FILE__ ) . "/Nirvana.json"; });

sub _build_client {
	my( $self ) = @_;

	my $client = Net::HTTP::Spore->new_from_spec( $self->spec_file, base_url => $self->wiki_url );
	$client->enable('Format::JSON');

	$self->client($client);
};

sub send_request {
	my( $self, $controller, $method, $params ) = @_;

	$params->{format}     = 'json';
	$params->{controller} = $controller;
	$params->{method}     = $method;

	my $response = $self->client->send_request( %$params );
	my $responseException = ();

	if( ref($response->body) eq 'HASH' ) {
		$response->body->{'http_status'} = $response->{'status'};
		return $response->body;
	}
	elsif( ( $response->body ne '' ) && ( index($response->body, '{') == 0 ) ) {
		$responseException = decode_json $response->body;
		$responseException->{'http_status'} = $response->{'status'};
	}
	else {
		$responseException->{'http_status'} = 555;
	}
	return $responseException;
};

1;
