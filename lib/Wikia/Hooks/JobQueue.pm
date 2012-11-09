package Wikia::Hooks::JobQueue;

use Moose::Role;

use Wikia::SimpleQueue;

use FindBin qw/$Bin/;

our $VERSION = "0.01";

=head1 NAME

Wikia::Hooks::JobQueue 

=head1 VERSION

version 0.01

=cut

#
# builders
#
sub jobqueue {
	my ( $self, $params ) = @_;
	
	my $res = 0;
	my $sqs = 1;
	if ( UNIVERSAL::isa($params, 'HASH') ) {
		if ( $params->{jobs} > 0 ) {
			my $queue = Wikia::SimpleQueue->instance( name => "spawnjob" );
			$queue->push( $params->{wiki_id} );
			$res = 1;
		}
	}
	
	return $res;
}

1;
