package Wikia::DirtyEvents;

use common::sense;
use Data::Dumper;
use Time::HiRes qw(gettimeofday tv_interval);
use Moose;
use Moose::Util::TypeConstraints;
use MooseX::Types::JSON qw( JSON );
use JSON::XS qw(encode_json decode_json);
use Redis;

use Wikia::Scribe;
use Wikia::Settings;
use Wikia::Log;
use Wikia::DB;
use Wikia::LB;

our $search_queue_name = 'search';

has "host"   => (
	is			=> "rw",
	isa			=> "Str",
	required	=> 1,
	default		=> '127.0.0.1' #'10.10.10.153'
);
has "port"   => (
	is			=> "rw",
	isa			=> "Int",
	required	=> 1,
	default 	=> 6379 #1218
);
has "queue"  => (
	is			=> "rw",
	isa			=> "Redis",
	default 	=> sub {
		my $self = shift;
		return Redis->new( 
			server => sprintf("%s:%s", $self->host, $self->port)
		);
	}
);
has "debug" => (
	is 			=> "rw",
	isa			=> "Int",
	default		=> 0
);
has "allowed_keys" => ( 
	is 			=> "rw", 
	isa 		=> "ArrayRef", 
	default 	=> sub { 
		my @scribeKeys = keys %{$Wikia::Scribe::scribeKeys};
		return \@scribeKeys;
	}
);
has "parsed_keys" => ( 
	is			=> "rw", 
	isa			=> "HashRef", 
	default		=> sub { 
		my %keys = (	
			Wikia::Scribe::EDIT_CATEGORY        => 0,
			Wikia::Scribe::CREATEPAGE_CATEGORY	=> 0,
			Wikia::Scribe::UNDELETE_CATEGORY    => 0,
			Wikia::Scribe::DELETE_CATEGORY      => 0
		); 
		return \%keys;
	}
);
has "output" => ( 
	is 			=> "rw", 
	isa 		=> "HashRef",
	default		=> sub { 
		{ 'processed' => 0, 'invalid' => 0 } 
	}
);
has "number_messages" => ( 
	is			=> "rw", 
	isa			=> "Int", 
	default		=> 0 
);
has "hosts" => ( 
	is			=> "rw", 
	isa			=> "HashRef", 
	default		=> sub { {} }
);      
has "scribe_message" => (
	is			=> "rw",
	trigger 	=> sub {
		my ( $self, $message ) = @_;
		
		if ( ref( $message ) && grep $_ eq $message->{'category'}, @{$self->allowed_keys} ) {
			$self->parse_message( $message ) ;
		} else {
			if ( ref( $message ) ) {
				say sprintf( "\tInvalid category: %s\n", $message->{'category'} );
			} else {
				say sprintf( "\tInvalid JSON\n" );
			}
			$self->output->{'invalid'}++;
		}
	}
);

__PACKAGE__->meta->make_immutable;

sub interval_time {
	my ( $self, $t_start ) = @_;
	
	return tv_interval( $t_start, [ $self->current_time() ] );
}

sub current_time {
	my $self = shift;
	return gettimeofday();
}

sub parse_message {
	my ( $self, $message ) = @_;
		
	$self->output->{'processed'}++;
	$self->parsed_keys->{ $message->{'category'} }++;
		
	say sprintf("\t%d. %s: %s\n", $self->output->{'processed'}, $message->{'category'}, $message->{'message'}) if ( $self->debug );
	
	my $info = decode_json( $message->{'message'} );
	
	if ( defined $info->{'hostname'} ) {
		$self->hosts->{ $info->{'hostname'} } = 0 unless ( $self->hosts->{ $info->{'hostname'} } );
		$self->hosts->{ $info->{'hostname'} }++;
	}
	
	$self->queue->lpush( $message->{'category'}, $message->{'message'} );
	
	#my $sinfo = {
	#	'cat' => $message->{'category'},
	#	'message' => $message->{'message'}
	#};
	#$self->queue->lpush( $search_queue_name, encode_json( $sinfo ) );
}

sub host_info {
	my $self = shift;
	
	if ( scalar keys %{$self->hosts} ) {
		my $lb = Wikia::LB->instance;
		my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS )} );
		foreach ( keys %{$self->hosts} ) {
			my $data = {
				'hostname' => $_,
				'-logdate' => 'curdate()+0',
				'logcount' => $self->hosts->{$_}
			};
			my $options = [ " ON DUPLICATE KEY UPDATE logcount = logcount + values(logcount) " ];
			my $res = $dbs->insert( 'scribe_log', "", $data, $options, 1 );			
		}
	}
}

sub queue_status {
	my $self = shift;
	
	foreach ( @{$self->allowed_keys} ) {
		say "Queue status (" . $_. "): " . $self->queue->llen( $_ ) . " messages";
	}	
}

sub update_log {
	my $self = shift;
	
	my $log = Wikia::Log->new( name => "scribec" );
	$log->update();	
}

sub Log {
	my ($self, $messages) = @_;

	my $t_start = [ $self->current_time() ];
	$self->output({});
	$self->parsed_keys({});
	
	if ( defined($messages) && UNIVERSAL::isa($messages, 'ARRAY') ) {
		# number of messages
		$self->number_messages( scalar @$messages );
		say "Number of messages: " . scalar @$messages; 
		foreach ( @$messages ) {
			$self->scribe_message( $_ );
		}
	}

	# processed, invalid
	say join( ', ', map { $_ . ": " . $self->output->{$_} } keys %{$self->output} );
	# log ith keys
	say join( ', ', map { $_ . ": " . $self->parsed_keys->{$_} } keys %{$self->parsed_keys} );

	# update SQL table with number of messages sent from hosts
	say "Update host stats";
	$self->host_info();
	# update table uses by nagios
	say "Update deamon log";
	$self->update_log();
	# display queue status
	$self->queue_status() if $self->debug;

	say "Messages processed: " . $self->interval_time( $t_start );

	return Scribe::Thrift::ResultCode::OK;
}

1;
