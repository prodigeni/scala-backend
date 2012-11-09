#!/usr/bin/env perl

#
# this is generator for lighttpd conf
#
use strict;
use common::sense;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";

use Wikia::KnifeSearch;

#
# port hardcoded, @todo - read from yaml file
#
my $port = 3456;

my $search = Wikia::KnifeSearch->new();
my $hosts = $search->hosts_by_search( "roles:thumbnailer" ); # add here excludes for devboxes
use Data::Dump;

=pod lighttpd mod_proxy example
    proxy.server = ( "" =>
        ( ( "host" => "10.0.0.242", "port" => 81 ) )
    )

=cut

for my $h ( @$hosts ) {
	say "${ \$h->{ip} }:$port";
}
