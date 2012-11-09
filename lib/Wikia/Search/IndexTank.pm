package Wikia::Search::IndexTank;

use Wikia::Settings;
use Net::HTTP::Spore;
use Data::Dumper;
use Moose;
use File::Basename;
use JSON::XS;

use constant INDEX_NAME  => 'TestIndex';
use constant MAX_DOCSIZE => 99; # in kB

has "spec_file" => ( is => "rw", isa => "Str", default => sub { return dirname( __FILE__ ) . "/IndexTank.json"; } );
has "base_url"  => ( is => "rw", isa => "Str", default => sub { return Wikia::Settings->instance()->variables()->{"wgWikiaSearchIndexTankApiUrl"}; } );
has "client"    => ( is => "rw", isa => "Object", lazy_build => 1 );
has "response"  => ( is => "rw" );

=item list_indexes
	list all indexes
=cut
sub list_indexes {
	my( $self ) = @_;
	my $indexes = $self->client->list_indexes->body;

	return $self->response( $indexes );
};

=item get_index
	get index info

	params:
		name => "index_name"
=cut
sub get_index {
	my( $self, $params ) = @_;
	my $index_info = $self->client->get_index(%$params)->body;

	return $self->response( $index_info );
};

=item add_index
	crete new index

	params:
		name => "index_name"
=cut
sub add_index {
	my( $self, $params ) = @_;
	$self->client->create_index(%$params);
};

=item del_index
	delete existing index

	params:
		name => "index_name"
=cut
sub del_index {
	my( $self, $params ) = @_;
	$self->client->delete_index(%$params);
};

=item list_functions
	list relevancy functions

	params:
		name => "index_name"
=cut
sub list_functions {
	my( $self, $params ) = @_;
	my $functions = $self->client->get_functions(%$params)->body;

	return $self->response( $functions );
};

=item search
	perform search

	params:
		index => "index_name"
		query => "query_string"
=cut
sub search {
	my ( $self, $params ) = @_;
	my $search = $self->client->search(name => $params->{index}, q => $params->{query}, fetch => '*', snippet => 'text', fetch_variables => 'true', fetch_categories => 'true')->body;

	$self->response( $search );
	return $self->response->{results};
};

=item autocomplete
	get autocomplete suggestions

	params:
		index => "index_name"
		query => "query_string"
=cut
sub autocomplete {
	my ( $self, $params ) = @_;
	my $search = $self->client->autocomplete(name => $params->{index}, query => $params->{query})->body;

	$self->response( $search );
	return $self->response->{suggestions};
}

=item add_documents
	add new document to index

	params:
		index => "index_name"
		docs => @documents_to_index
=cut
sub add_documents {
	my ( $self, $params ) = @_;

	my @docs = $params->{docs};
	my $response = $self->client->add_documents( name => $params->{index}, payload => @docs );
	$self->response( $response );

	return $self->response;
}

sub _build_client {
	my( $self ) = @_;

	my $client = Net::HTTP::Spore->new_from_spec( $self->spec_file, base_url => $self->base_url );
	$client->enable('Format::JSON');

	$self->client($client);
};

1;
