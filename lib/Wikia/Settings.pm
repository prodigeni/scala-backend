package Wikia::Settings;

use strict; # just for silencing perl::critic warnings
use common::sense;

use YAML::XS;
use MooseX::Singleton;
use IO::File;

our $VERSION = "0.01";

our $YML_PATH = { 
	'sg' => {
		'env'	=> 'WIKIA_SENDGRID_YML',
		'path'	=> '/usr/wikia/conf/current/Sendgrid.yml'
	},
	'default' => {
		'env'	=> 'WIKIA_SETTINGS_YML',
		'path'	=> '/usr/wikia/conf/current/Settings.yml'
	}
};

has "variables"  => ( is => "rw", lazy_build => 1 );
has "debug"		 => ( is => "rw", default => 0 );

=head1 NAME

Wikia::Settings - expose Mediawiki settings


=head1 VERSION

version $VERSION

=cut

#
# builders
#
sub _build_variables {

	my ( $self ) = @_;

	#
	# read configs
	#
	my $fh = new IO::File;
	my %vars = ();
		
	foreach my $section ( keys %{$YML_PATH} ) {
		# load file into variable
		my $yml = ( defined $ENV{ $YML_PATH->{ $section }->{ env } } ) 
			? $ENV{ $YML_PATH->{ $section }->{ env } }
			: $YML_PATH->{ $section }->{ path };

		if( $fh->open( $yml ) ) {
			my @yml = <$fh>;
			my %h = %{ Load join( "", @yml ) };
			@vars{ keys %h } = values %h;
			$fh->close();
		}
		else {
			say "Cannot open $yml" if ( $self->debug  );
		}
	}
	$self->variables( \%vars );
}

1;
