package Wikia::SimplePreforkServer;

use strict;
no warnings;

use base qw/Class::Accessor::Fast/;
use Data::Dumper;

use Thrift::ServerSocket;
use Thrift::FramedTransportFactory;
use Thrift::PreforkServer;
#use Thrift::SimpleServer;

use Scribe::Thrift::scribe;
use base qw/Scribe::Thrift::scribeIf/;

__PACKAGE__->mk_accessors(qw/port handler category workers sendTimeout recvTimeout/);

sub new {
    my ($class, @args) = @_;
    my $self  = $class->SUPER::new;
    $self->handler( $args[0] );
    $self->category( $args[1] );
    $self->port( $args[2] || 1463);
    $self->workers( $args[3] || 10);
    $self->sendTimeout( defined $args[4] ? $args[4] : 10000);
    $self->recvTimeout( defined $args[5] ? $args[5] : 10000);
    bless $self, $class;
}

sub trim {
	my ($self, $string) = @_;
	$string =~ s/^\s+//;
	$string =~ s/\s+$//;
	return $string;
}

sub Log {
	my ($self, $messages) = @_;

	my $result = Scribe::Thrift::ResultCode::OK;
	if ( defined($messages) && UNIVERSAL::isa($messages,'ARRAY') ) {
		if ( $self->handler && $self->handler->can("Log") ) {
			my $res = [];
			if ( $self->category ) {
				foreach my $m (@$messages) {
					next unless $m->{category} eq $self->category;
					push @$res, { 'category' => $m->{category}, 'message' => $m->{message} };
				}
			} else {
				$res = $messages;
			}
			$result = $self->handler->Log($res);
		}
	}
	
	return $result;
}

sub run {
	my $self = shift;
	my $processor = new Scribe::Thrift::scribeProcessor($self);
	my $socket = new Thrift::ServerSocket($self->port);
	$socket->setSendTimeout($self->sendTimeout);
	$socket->setRecvTimeout($self->recvTimeout);
	my $transport = new Thrift::FramedTransportFactory();
	my $protocol = new Thrift::BinaryProtocolFactory();
	#my $server = new Thrift::SimpleServer( $processor, $socket, $transport, $protocol);
	my $server = new Thrift::PreforkServer( $processor, $socket, $transport, $protocol, $self->workers);
 	#my $server = new Thrift::ForkingServer($processor, $socket, $transport, $protocol);
	print "starting server (port: " . $self->port . ") \n";
	$server->serve();
}

1;
