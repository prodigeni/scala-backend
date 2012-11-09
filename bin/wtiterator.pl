#!/usr/bin/perl

use common::sense;

use FindBin qw/$Bin/;
use lib "$Bin/../lib/";
use Data::Dumper;
use Wikia::WikiFactory::Iterator;
use Getopt::Long;
use feature "say";


my( @city_c, @city_p) = undef;


my @city_l = ["en", "pl"];

my $tab;
my $w = Wikia::WikiFactory::Iterator->new(city_path => ['slot1']);
say Dumper($w);
say Dumper($w->list_cityid);
say $w->nextOnList() ;
my $a = $w->nextOnList();
while ($a > -1){
    say $a;
    $a = $w->nextOnList();
}
1;