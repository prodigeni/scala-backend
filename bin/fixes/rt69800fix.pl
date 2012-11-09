#!/usr/bin/perl -w

#
# fix for #69800: wgLogo variable - clean up in DB or move to global config maybe?
#


use common::sense;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib";
use Wikia::LB;
use Wikia::WikiFactory;
use PHP::Serialization qw(unserialize serialize);


my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::EXTERNALSHARED );

my $sth = $dbh->prepare( "select * from city_variables where cv_variable_id=(select cv_id from city_variables_pool where cv_name='wgLogo') and cv_value not like '%Upload%'" );
$sth->execute();
while( my $row = $sth->fetchrow_hashref ) {
	my $before = unserialize( $row->{ "cv_value" } );
	my $wf = Wikia::WikiFactory->new( city_id => $row->{ "cv_city_id" } );
	my $middle = $wf->variables()->{ "wgUploadPath" };
	my $after = $before;
	substr( $after, 0, length( $middle ), '$wgUploadPath' );
	say "Changing value from $before to $after";
	$wf->set_variable( name => "wgLogo", value => $after );
}
