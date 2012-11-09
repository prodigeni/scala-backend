package Wikia::Stomp;

use strict;
use Carp;
use DBI;
use IO::File;
use Switch;
use Data::Dumper;
use Net::Stomp;

use constant wgStompServer => '10.8.2.221'; #'10.10.10.163'
use constant wgStompPort => 61613;
use constant wgStompUser => 'guest';
use constant wgStompPassword => 'guest';
use constant wgStompKey => 'wikia.apache.stats.#';

use base qw(Class::Accessor);
__PACKAGE__->mk_accessors(qw(host port user passwd stomp queue key durable));
our $VERSION = '0.01';

sub new {
    my $class  = shift;
    my $self   = $class->SUPER::new(@_);

	$self->host(wgStompServer) unless $self->host;
	$self->port(wgStompPort) unless $self->port;
	$self->user(wgStompUser) unless $self->user;
	$self->passwd(wgStompPassword) unless $self->passwd;
	$self->key(wgStompKey) unless $self->key;
	$self->durable('true') if !defined $self->durable ;

	my $stomp = Net::Stomp->new( 
		{ 
			hostname 	=> $self->host, 
			port 		=> $self->port
		} 
	);

	$stomp->connect( 
		{ 
			login 		=> $self->user, 
			passcode 	=> $self->passwd
		} 
	);

	if ( $self->queue ) {
		$stomp->subscribe (
			{   
				'destination'                   => $self->queue,
				'ack'                           => 'client',
				'exchange'                      => 'amq.topic',
				'durable'						=> $self->durable,
				'auto-delete'					=> 'false',
				'activemq.prefetchSize'         => 1,
				'routing_key'                   => $self->key,
				'id'                            => $self->queue,
			}
		);
	}

	$self->stomp($stomp);
    return $self->stomp;
}

1;
__END__
