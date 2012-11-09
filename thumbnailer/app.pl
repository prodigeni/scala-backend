#!/usr/bin/env perl
use Dancer;

use FindBin qw/$Bin/;
use lib "$Bin/../lib/";

use Wikia::Thumbnailer;

dance;
