#!/usr/bin/env perl

use Dancer ':syntax';
use FindBin '$RealBin';
use Plack::Runner;
use Data::Dumper;

# For some reason Apache SetEnv directives dont propagate
# correctly to the dispatchers, so forcing PSGI and env here 
# is safer.
my $psgi = path($RealBin, '.', 'app.pl');
die "Unable to read startup script: $psgi" unless -r $psgi;

my @argc = @ARGV;
my $env = 'development';
while ( my $arg = shift @argc ) { $env = shift @argc if ( $arg eq '-E' ) ; }

set apphandler => 'PSGI';
set startup_info => 0;
set environment => $env;

my $runner = Plack::Runner->new;
$runner->parse_options(@ARGV);
$runner->run($psgi);
