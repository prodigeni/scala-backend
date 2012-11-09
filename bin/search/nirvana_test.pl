#!/usr/bin/perl

use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use Wikia::Nirvana;
use Getopt::Long;
use Data::Dumper;

GetOptions(
    'spec=s'     => \my $spec,
    'base_url=s' => \my $base_url,
);

my $nirvana = new Wikia::Nirvana({ "wiki_url" => "http://muppet.adi.wikia-dev.com/" });

my $response = $nirvana->send_request( "WikiaSearch", "getPage", { 'id' => 3851 } );

print Dumper $response;
