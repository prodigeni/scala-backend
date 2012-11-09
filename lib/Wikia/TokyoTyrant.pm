package Wikia::TokyoTyrant;

use strict;
use Carp;
use DBI;
use IO::File;
use Switch;
use Data::Dumper;
use TokyoTyrant;
use Switch;

use constant wgTTServer => '127.0.0.1'; #'10.10.10.163'; #'10.8.2.221';
use constant wgTTPort => 1978;
use constant wgTTTout => 10;

use base qw(Class::Accessor);
__PACKAGE__->mk_accessors(qw(host port tout debug rdb table type));
our $VERSION = '0.01';

sub new {
    my $class  = shift;
    my $self   = $class->SUPER::new(@_);

	$self->host(wgTTServer) unless $self->host;
	$self->port(wgTTPort) unless $self->port;
	$self->tout(wgTTTout) unless $self->tout;
	$self->type('hash') unless $self->type;

	$TokyoTyrant::DEBUG = 1 if $self->debug;
	
	my $rdb = undef;
	if ( $self->type eq 'table' ) {
		$rdb = TokyoTyrant::RDBTBL->new();		
	} else {
		$rdb = TokyoTyrant::RDB->new();
	}

	if ( !$rdb->open( $self->host, $self->port, $self->tout ) ) {
		my $ecode = $rdb->ecode();
		print STDERR "Could not open TT connection " . $rdb->errmsg($ecode) . " \n";
		return 0;
	}

	if ( $self->table ) {
	    foreach my $col (keys %{$self->table}) {
	    	my $type = $rdb->ITDECIMAL;
	    	if ( $self->table->{$col} ) {
				switch ($self->table->{$col}) {
					case 'pk' 	{ $col = ''; $type = $rdb->ITDECIMAL; }
					case 'int' 	{ $type = $rdb->ITDECIMAL; }
					case 'str' 	{ $type = $rdb->ITDECIMAL; }
					case 'bool' { $type = $rdb->ITTOKEN; }
					case 'text' { $type = $rdb->ITQGRAM; }    		
				}
			} else {
				$type = $rdb->ITVOID;
			}

			if ( !$rdb->setindex($col, $type) ) { 
				my $ecode = $rdb->ecode();
				print STDERR sprintf( "Could not set index on column %s: %s \n", $col, $rdb->errmsg($ecode) );
				return 0;
			}
		} 
	}

	$self->rdb($rdb);
    return $self->rdb;
}

1;
__END__
