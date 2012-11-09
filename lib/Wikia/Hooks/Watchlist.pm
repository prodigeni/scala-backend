package Wikia::Hooks::Watchlist;

use YAML::XS;
use Moose::Role;
use IO::File;
use Data::Dumper;

use Wikia::LB;
use Wikia::DB;
use Wikia::Settings;
use Wikia::Utils;
use Wikia::User;

use FindBin qw/$Bin/;

binmode STDOUT, ":utf8";

our $VERSION = "0.01";

=head1 NAME

Wikia::Hooks::Watchlist - use MW WatchedItem hooks

=head1 VERSION

version $VERSION

=cut

#
# builders
#
sub addWatch {
	my ( $self, $params ) = @_;
	
#	my $oMW = Wikia::Utils->json_decode($msg);
	my $res = 0;
	if ( UNIVERSAL::isa($params, 'HASH') ) {
		my $lb = Wikia::LB->instance;
		my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::DATAWARESHARED )} );

		if ( defined $params->{wl_notificationtimestamp} ) {
			
			my %data = (
				"gwa_user_id"  	=> $params->{wl_user},
				"gwa_city_id"   => $params->{wl_wikia},
				"gwa_title"		=> $params->{wl_title},
				"gwa_namespace" => $params->{wl_namespace},
				"gwa_rev_id"	=> $params->{wl_revision},
				"gwa_rev_timestamp" => $params->{wl_rev_timestamp}
			);
			
			$dbw->delete( "global_watchlist", \%data );
			
			$data{ "gwa_timestamp" } = $params->{wl_notificationtimestamp};
							
			my $status = $dbw->insert("global_watchlist", "", \%data, '', 1);
			$res = 1 if ( $status );
		} else {
			# new watch - so don't update global watchlist - notify only changes
			# in watched pages 
			$res = 1;
		}
	}
	
	return $res;
}

sub removeWatch {
	my ( $self, $params ) = @_;
	
	my $res = 0;
	if ( UNIVERSAL::isa($params, 'HASH') ) {
		my $lb = Wikia::LB->instance;
		my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::DATAWARESHARED )} );

		my %data = (
			"gwa_user_id"  	=> $params->{wl_user},
			"gwa_city_id"   => $params->{wl_wikia},
			"gwa_title"		=> $params->{wl_title},
			"gwa_namespace" => $params->{wl_namespace},
		);
		
		my $status = $dbw->delete( "global_watchlist", \%data );

		$res = 1 if ( $status );
	}
	
	return $res;
}

sub updateWatch {
	my ( $self, $params ) = @_;
	
	my $res = 0;
	if ( UNIVERSAL::isa($params, 'HASH') ) {
		my $lb = Wikia::LB->instance;
		my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::DATAWARESHARED )} );

		my $where = $params->{where};
		my $what = $params->{update};
		my $wikia_id = $params->{wl_wikia};

		if ( UNIVERSAL::isa($where, 'HASH') && UNIVERSAL::isa($what, 'HASH') ) {

			my %data = (
				"gwa_user_id"  	=> $where->{wl_user},
				"gwa_city_id"   => $wikia_id,
				"gwa_title"		=> $where->{wl_title},
				"gwa_namespace" => $where->{wl_namespace}		
			);
			
			$dbw->delete( "global_watchlist", \%data );
				
			if ( defined $what->{wl_notificationtimestamp} ) {
					
				# insert (with ignore) first
				my $status = $dbw->insert("global_watchlist", "", \%data, '', 1);
				
				# ... then update
				my @conditions = ();
				foreach my $x ( keys %data ) {
					push @conditions, "$x = " . $dbw->quote( $data{$x} );
				};
				
				# ... but what update
				%data = ();
				foreach my $y ( keys %{$what} ) {
					my $key = undef;
					if ( $y eq 'wl_user' ) {
						$key = 'gwa_user_id';
					} 
					elsif ( $y eq 'wl_title' ) {
						$key = 'gwa_title'
					}
					elsif ( $y eq 'wl_namespace' ) {
						$key = 'gwa_namespace' ;
					}
					elsif ( $y eq 'wl_notificationtimestamp' ) {
						$key = 'gwa_timestamp';
					}
					elsif ( $y eq 'wl_revision' ) {
						$key = 'gwa_rev_id';
					}			
					elsif ( $y eq 'wl_rev_timestamp' ) {
						$key = 'gwa_rev_timestamp';
					}		
					
					if ( defined $key ) {
						$data{ $key } = $what->{$y};
					}
				}
				
				if ( scalar keys %data ) {
					$dbw->update('global_watchlist', \@conditions, \%data);
				}
			} else {
				print "Empty wl_notificationtimestamp (wikia:" . $wikia_id . ", page: " . $where->{wl_title} . ")  - don't update \n";
			}
		}
		$res = 1;
	}
	
	return $res;
}

1;
