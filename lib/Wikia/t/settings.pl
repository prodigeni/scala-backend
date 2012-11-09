#!/usr/bin/env perl

use common::sense;

use FindBin qw/$Bin/;
use lib "$Bin/../../";

use Data::Dump;

use Wikia::Settings;


my $settings = Wikia::Settings->instance;
my $t = $settings->variables();
dd( $t->{ "wgWikiaBotUsers" }->{ "staff" } );
dd( $t->{ "wgWikiaBotUsers" }->{ "staff" }->{ "username" } );
