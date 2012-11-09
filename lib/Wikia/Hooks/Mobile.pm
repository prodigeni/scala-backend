package Wikia::Hooks::Mobile;

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
sub mobile_apps {
	my ( $self, $params ) = @_;
	
	my $res = 0;
	if ( UNIVERSAL::isa($params, 'HASH') ) {
		print "log for mobile: " . $params->{app} . "\n";
		my $lb = Wikia::LB->instance;
		my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS )} );

		my $date = DateTime->from_epoch( epoch => ( $params->{time} ) ? $params->{time} : time() )->strftime('%F %T');

		my %data = (
			"appname"   => $params->{app},
			"url"   	=> $params->{uri},
			"os" 		=> $params->{os},
			"ts"		=> $date
		);
		
		my $status = $dbw->insert("mobile_apps", "", \%data, '', 1);
	}
	
	return $res;
}

1;
