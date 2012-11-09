#!/usr/bin/perl
package TestHandler;

use strict;
use warnings;
use Data::Dumper;
use lib 'gen-perl';
use lib 'lib';

use SimplePreforkServer;

use Data::Dumper;
use base qw/Class::Accessor::Fast/;

sub new {
    my ($class, @args) = @_;
    my $self  = $class->SUPER::new;
    bless $self, $class;
}

sub Log {
	my ($self, $messages) = @_;
	
	if ( defined($messages) && UNIVERSAL::isa($messages,'ARRAY') ) {
		my $loop = 1;
		foreach ( @$messages ) {
			print sprintf("%d. %s: %s \n", $loop, $_->{category}, $_->{message});
			$loop++;
		}
	}
	return Scribe::Thrift::ResultCode::OK;;
}

my $handler = new TestHandler;
my $category = 'edit_log';
my $server = SimplePreforkServer->new( $handler, $category, 9090 );
$server->run;
