package Thrift::PreforkServer;
use strict;
use warnings;
use base qw/Class::Accessor::Fast/;
use Data::Dumper;

use Thrift;
use Thrift::BinaryProtocolFactory;
use Thrift::TransportFactory;

use Parallel::Prefork;

__PACKAGE__->mk_accessors(qw/processor serverTransport transportFactory protocolFactory maxWorkers/);

sub new {
    my ($class, @args) = @_;
    my $self  = $class->SUPER::new;

    $self->processor       ( $args[0] );
    $self->serverTransport ( $args[1] );
    $self->transportFactory( $args[2] || Thrift::TransportFactory->new );
    $self->protocolFactory ( $args[3] || Thrift::BinaryProtocolFactory->new );
    $self->maxWorkers      ( $args[4] || 10);

    bless $self, $class;
}

sub _handleException {
	my $self = shift;
	my $e    = shift;
	
	if ($e =~ m/TException/ and exists $e->{message}) {
		my $message = $e->{message};
		my $code    = $e->{code};
		my $out     = $code . ':' . $message;
		
		$message =~ m/TTransportException/ and die $out;
		if ($message =~ m/TSocket/) {
			# suppress TSocket messages
		} else {
			print $out . "\n";
		}
	} else {
		print $e . "\n";
	}
}

sub serve {
    my $self = shift;
    $self->serverTransport->listen;

    my $pm = Parallel::Prefork->new({
        max_workers  => $self->maxWorkers,
        trap_signals => {
            TERM => 'TERM',
            HUP  => 'TERM',
        },
    });

    while ($pm->signal_received ne 'TERM') {
        $pm->start and next;

        while (1) {
            my $client = $self->serverTransport->accept or die Thrift::TException->new($!);
            my $trans = $self->transportFactory->getTransport($client);
            my $prot  = $self->protocolFactory->getProtocol($trans);

            eval {
                while (1) { $self->processor->process( $prot, $prot ) }
            };
            if ($@) {
				$self->_handleException($@);
            }
        }

        $pm->finish;
    }
    $self->serverTransport->close;
}

1;
