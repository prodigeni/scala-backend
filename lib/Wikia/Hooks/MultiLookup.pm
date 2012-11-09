package Wikia::Hooks::MultiLookup;

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

Wikia::Hooks::MultiLookup - save data for Special:MultiLookup page

=head1 VERSION

version $VERSION

=cut

#
# builders
#
sub ipActivity {
	my ( $self, $params ) = @_;
	
#	my $oMW = Wikia::Utils->json_decode($msg);
	my $res = 0;
	if ( UNIVERSAL::isa($params, 'HASH') ) {
		my $lb = Wikia::LB->instance;
		
		my $cnt = 0;
		my $maxts = undef;
		if ( Wikia::Utils->is_ip( $params->{ip} ) ) {
			my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, 'stats', $params->{'dbname'} );
			if ($dbh) {
				my $sth = $dbh->prepare("select count(0) as cnt, max(rc_timestamp) as max_time from recentchanges where rc_ip = ?");
				if ( $sth->execute( $params->{ip} ) ) {
					my ( $cnt, $maxts ) = $sth->fetchrow_array();
				}
			}
			
			if ( $cnt && $maxts ) {
				my %data = (
					"ml_city_id"	=> $params->{wiki_id},
					"-ml_ip"  		=> "INET_ATON('" . $params->{ip} . "')",
					"ml_count"   	=> $cnt,
					"ml_ts"			=> $maxts
				);				

				my @options = ( " ON DUPLICATE KEY UPDATE ml_count=values(ml_count), ml_ts = values(ml_ts) " );
				
				my $dbm = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, 'stats', Wikia::LB::STATS );
				my $dbs = new Wikia::DB( {"dbh" => $dbm } );	
				
				$res = $dbs->insert( '`specials`.`multilookup`', "", \%data, \@options, 1 );				
			}
		}
	}
	
	return $res;
}

1;
