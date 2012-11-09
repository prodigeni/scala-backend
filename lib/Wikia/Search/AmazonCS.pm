package Wikia::Search::AmazonCS;

use Data::Dumper;
use Moose;
use File::Basename;
use JSON::XS;
use LWP::UserAgent;
use HTTP::Request;
 
has "doc_endpoint"  => ( is => "rw", isa => "Str", default => 'http://doc-handler-wikia-zt7w5qb3vbdattbk4s5yevgv5e.us-east-1.cloudsearch.amazonaws.com/' );
#has "doc_endpoint"  => ( is => "rw", isa => "Str", default => 'http://doc-wikia-test-dq6m57jtoklr4ajzy7zj2phhzi.us-east-1.cloudsearch.amazonaws.com/' );
has "response"  => ( is => "rw" );

sub send_document_batch {
	my ( $self, $payload ) = @_;

	my $ua = LWP::UserAgent->new;
	$ua->timeout(10);
	$ua->env_proxy;

	my $request = HTTP::Request->new('POST', $self->doc_endpoint . '2011-02-01/documents/batch');
	$request->header('Content-Type' => 'application/json; charset=utf-8');
	$request->header('Content-Length' => length($payload));
	$request->content($payload);

	my $response = $ua->request($request);

	$self->response( $response );

	return $self->response;
}

sub write_document_batch {
	my ( $self, $payload ) = @_;
	
	print Dumper $payload;
}

1;
