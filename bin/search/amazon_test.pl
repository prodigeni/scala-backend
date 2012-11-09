#!/usr/bin/perl

use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use JSON::XS;
use Wikia::Search::AmazonCS;
use Getopt::Long;
use Data::Dumper;

GetOptions(
    'method=s'   => \my $method,
    'params=s%'  => \my $params
);

my $amazon_cs = new Wikia::Search::AmazonCS();

#print Dumper $params;
#print Dumper $amazon_cs->$method($params);

print Dumper $amazon_cs->send_document_batch({ payload => '[]' });
