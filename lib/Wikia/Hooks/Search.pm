package Wikia::Hooks::Search;

use YAML::XS;
use Moose::Role;
use IO::File;
use Data::Dumper;

use Wikia::Settings;
use Wikia::Utils;

use FindBin qw/$Bin/;

our $VERSION = "0.01";

#
# builders
#
sub searchmiss {
	my ( $self, $params ) = @_;
	
#	my $oMW = Wikia::Utils->json_decode($msg);
	my $res = 0;
	if ( UNIVERSAL::isa($params, 'HASH') ) {
		my $lb = Wikia::LB->instance;
		my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS )} );

		my %data = (
			"sd_wiki"   	=> $params->{sd_wiki},
			"sd_query"		=> $params->{sd_query},
			"-sd_lastseen"	=> 'now()',
			"sd_misses"		=> 1,
		);
			
		my @ins_options = ( " ON DUPLICATE KEY UPDATE sd_lastseen = now(), sd_misses = sd_misses + 1 " );
		my $ins = $dbw->insert( "specials.searchdigest", "", \%data, \@ins_options, 1 );
		$res = 1;
	}
	
	return $res;
}

1;
