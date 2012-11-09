#!/usr/bin/perl

use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use JSON::XS;
use Wikia::Search::IndexTank;
use Getopt::Long;
use Data::Dumper;

GetOptions(
    'base_url=s' => \my $base_url,
    'index=s'    => \my $index,
    'method=s'   => \my $method,
    'params=s%'  => \my $params
);

my %params = ( defined $base_url ) ? ( "base_url" => $base_url ) : ();
my $indexTank = new Wikia::Search::IndexTank(\%params);

print Dumper $indexTank->$method($params);
