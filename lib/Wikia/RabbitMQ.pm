package Wikia::RabbitMQ;

use strict;
use Carp;
use DBI;
use IO::File;
use Switch;
use Data::Dumper;
use Net::RabbitMQ;

use constant wgRabbitMQServer => '10.10.10.8'; #'10.8.2.221'
use constant wgRabbitMQPort => 5672;
use constant wgRabbitMQUser => 'guest';
use constant wgRabbitMQPassword => 'guest';
use constant wgRabbitMQChannel => 1;
use constant wgRabbitMQChannelMax => 0;
use constant wgRabbitMQFrameMax => 0;

use base qw(Class::Accessor);
__PACKAGE__->mk_accessors(qw(host port user password queue channel channel_max frame_max rabbitmq exchange routing_key));
our $VERSION = '0.01';

sub new {
    my $class  = shift;
    my $self   = $class->SUPER::new(@_);

	$self->host(wgRabbitMQServer) unless $self->host;
	$self->port(wgRabbitMQPort) unless $self->port;
	$self->user(wgRabbitMQUser) unless $self->user;
	$self->password(wgRabbitMQPassword) unless $self->password;
	$self->channel(wgRabbitMQChannel) unless $self->channel;
	$self->channel_max(wgRabbitMQChannelMax) unless $self->channel_max;
	$self->frame_max(wgRabbitMQFrameMax) unless $self->frame_max;

	my $rabbitmq = Net::RabbitMQ->new();

	$rabbitmq->connect( $self->host, 
		{ 
			user		=> $self->user, 
			password	=> $self->password
		}
	);

	$rabbitmq->channel_open( $self->channel );

	my $queue = $rabbitmq->queue_declare(
		$self->channel, $self->queue, 
		{ 
			passive => 0, 
			durable => 1, 
			exclusive => 0,
			auto_delete => 1 
		}
	);
	print Dumper($queue);
	$self->queue($queue);
	
	$rabbitmq->queue_bind($self->channel, $self->queue, $self->exchange, $self->routing_key);	

	$self->rabbitmq($rabbitmq);
    return $self->rabbitmq;
}

1;
__END__
