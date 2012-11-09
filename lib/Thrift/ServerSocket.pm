package Thrift::ServerSocket;
use strict;
use warnings;
use base qw/Thrift::ServerTransport Class::Accessor::Fast/;

__PACKAGE__->mk_accessors(qw/port handle/);

use IO::Socket;
use Thrift::Socket;

sub new {
    my $class = shift;
    my $self = $class->SUPER::new;
    $self->port( shift || 1463 );
    bless $self, $class;
}

sub listen {
    my $self = shift;
    $self->handle(
        IO::Socket::INET->new(
            LocalPort => $self->port,
            Listen    => SOMAXCONN,
            Proto     => 'tcp',
            Reuse     => 1,
        )
    );
}

sub accept {
    my $self = shift;
    if (defined $self->handle) {
        my $sock = $self->handle->accept;

        my $trans = Thrift::Socket->new;
        $trans->setHandle( $sock );
        $trans->setRecvTimeout($self->{recvTimeout});
        $trans->setSendTimeout($self->{sendTimeout});

        return $trans;
    }
    return;
}

package Thrift::Socket;

sub setHandle {
    my ($self, $sock) = @_;
    $self->{handle} = Wikia::IO::Select->new( $sock );
}

package Wikia::IO::Select;

use base qw(IO::Select);

sub can_read {
    my $self = shift;
    my ($timeout) = @_;

    # Pass in undef when we set the timeout to zero
    return $timeout ? $self->SUPER::can_read($timeout) : $self->SUPER::can_read();   
}

1;
