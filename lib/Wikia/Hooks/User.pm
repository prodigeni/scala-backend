package Wikia::Hooks::User;

use YAML::XS;
use Moose::Role;
use IO::File;
use Data::Dumper;

use Wikia::Settings;
use Wikia::Utils;
use Wikia::User;

use FindBin qw/$Bin/;

our $VERSION = "0.01";

=head1 NAME

Wikia::Hooks::User - use MW User hooks

=head1 VERSION

version $VERSION

=cut

#
# builders
#
sub login {
	my ( $self, $params ) = @_;
	
#	my $oMW = Wikia::Utils->json_decode($msg);
	my $res = 0;
	if ( UNIVERSAL::isa($params, 'HASH') ) {
		my $lb = Wikia::LB->instance;
		my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS )} );

		my $user = new Wikia::User( db => 'wikicities', id => $params->{user_id} );

		if ( $user ) {
			my $user_options = $user->options;
			my %data = (
				"user_id"   		=> $params->{user_id},
				"city_id"   		=> $params->{city_id},
				"ulh_from"			=> $params->{ulh_from},
				"ulh_rememberme" 	=> $user_options->{'rememberpassword'}
			);
			
			my $status = $dbw->insert("user_login_history", "", \%data, '', 1);
			
			%data = (
				"user_id"   		=> $params->{user_id},
				"-ulh_timestamp" 	=> 'now()'
			);	
			
			my @ins_options = ( " ON DUPLICATE KEY UPDATE ulh_timestamp = now() " );
			$status = $dbw->insert( "user_login_history_summary", "", \%data, \@ins_options, 1 );
			$res = 1;
		}
	}
	
	return $res;
}

sub savepreferences {
	my ( $self, $params ) = @_;
	
	my $cluster = [
		'c2',
		'c3'
	];
	
	#my $oMW = Wikia::Utils->json_decode($msg);
	my $res = 0;
	if ( UNIVERSAL::isa($params, 'HASH') ) {
		my $lb = Wikia::LB->instance;
		my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS )} );

		my $user = new Wikia::User( db => 'wikicities', id => $params->{user_id} );

		if ( $user ) {
			print "log for user: " . $params->{user_id} . "\n";
			my %data = (
				"user_id"          => $params->{user_id},
				"user_name"        => $params->{user_name},
				"user_real_name"   => $params->{user_real_name},
				"user_password"    => $params->{user_password},
				"user_newpassword" => $params->{user_newpassword},
				"user_email"       => $params->{user_email},
				"user_options"     => $params->{user_options},
				"user_touched"     => $params->{user_touched},
				"user_token"       => $params->{user_token}
			);
			
			my $status = $dbw->insert("user_history", "", \%data, '', 1);
			
			# move to other cluster
=disabled for ExternalUser			
			foreach ( @{$cluster} ) {
				#my $exists = $user->user_exists_cluster( $_ );
				#if ( !$exists ) {
				$user->copy_to_cluster( $_ );
				#}
			}
=cut			
			
			$res = 1 if ( $status );
		}
	}
	
	return $res;
}

1;
