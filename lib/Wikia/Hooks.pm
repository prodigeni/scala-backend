package Wikia::Hooks;

use common::sense;
use feature "say";

use Data::Dumper;
use Wikia::Log;
use YAML::XS;
use MooseX::Singleton;
use IO::File;
use Thread::Pool::Simple;

with 'Wikia::Hooks::User';
with 'Wikia::Hooks::Mobile';
with 'Wikia::Hooks::Twitter';
with 'Wikia::Hooks::Watchlist';
with 'Wikia::Hooks::MultiLookup';
with 'Wikia::Hooks::Search';
with 'Wikia::Hooks::JobQueue';

our $VERSION = "0.01";

has "config" 	=> ( is => "rw", lazy_build => 1 );
has "debug"     => ( is => "rw", "isa" => "Int", required => 1 );
has "threads" => ( is => "rw", "isa" => "Int", default => 10 );
has "notfound" => ( is => "rw", "isa" => "Int", default => 0 );
has "processed" => ( is => "rw", "isa" => "Int", default => 0 );
has "invalid" => (is => "rw", "isa" => "Int", default => 0 );

=head1 NAME

Wikia::Hooks

=head1 VERSION

version $VERSION

=cut

#
# builders
#
sub _build_config {
	my ( $self ) = @_;
	
	my $config = {};
	
	$self->config ( $config );	
}

sub worker {
	my( $self, $worker_id, $method, $params ) = @_;

	my $response = eval { $self->$method($params) } ;
	if ( !defined $response ) {
		$self->notfound++;
	} elsif ( $response == 0 ) {
		$self->invalid++;
	} else {
		$self->processed++;
	}
}

sub Log {
	my ($self, $messages) = @_;

	# check time
	my $process_start_time = time();
	
	# default result;
	my $ok = 1;
	$self->notfound( 0 );
	$self->processed( 0 );
	$self->invalid( 0 );

	my $sc_keys = {};
	if ( defined($messages) && UNIVERSAL::isa($messages,'ARRAY') ) {
	
		my $pool = Thread::Pool::Simple->new(
			min => 1,
			max => $self->threads,
			load => 4,
			do => [sub {
				$self->worker( @_ );
			}],
			monitor => sub {
				say "done";
			},
			passid => 1,
		);
		
		my $loop = 1;
		print "Number of messages: " . scalar @$messages . "\n"; 
		foreach ( @$messages ) {
			# from scribe
			my $s_key = $_->{category};
			my $s_msg = $_->{message};

			my $oMW = Wikia::Utils->json_decode($s_msg);
			my $res = 0;
			if ( UNIVERSAL::isa($oMW, 'HASH') ) {
				if ( defined $oMW->{method} ) {
					print sprintf("\t%d. %s: %s\n", $loop, $s_key, $oMW->{method}) if ( $self->debug );
					$sc_keys->{$oMW->{method}} = 0 unless ( $sc_keys->{$oMW->{method}} );
					$sc_keys->{$oMW->{method}}++;
					
					$pool->add( $oMW->{method}, $oMW->{params} );

					#push @{$sc_keys->{$oMW->{method}}}, $oMW->{params};
					$loop++;		
				}		
			}
		}
		$pool->join;
	}
  	                                 
	my $msg = "";
	#print Dumper(%$sc_keys);
	if ( scalar keys %$sc_keys ) {
		foreach my $method ( sort keys %$sc_keys) {
			$msg .= $method . ": " . $sc_keys->{$method} . ",";
		}
	}
	print "\n" . $msg . "\n";

	my $process_end_time = time();
	my @ts = gmtime($process_end_time - $process_start_time);
	
	print sprintf("result: %0d not found, %0d records, %0d invalid messages\n", $self->notfound, $self->processed, $self->invalid );
	print "messages processed: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);

	# update log #bugid: 6713
	if ( $ok ) {
		my $log = Wikia::Log->new( name => "hookd" );
		$log->update();
	}
	
	print "ok = $ok \n" if ( $self->debug );
	return ($ok) ? Scribe::Thrift::ResultCode::OK : Scribe::Thrift::ResultCode::TRY_LATER;
}

1;
