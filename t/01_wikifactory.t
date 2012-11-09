use strict;
use Test::More (tests => 2);

#
# push to path
#
use FindBin qw/$Bin/;
use lib "$Bin/../lib/";

use_ok("Wikia::WikiFactory");
use Wikia::WikiFactory;
my $wf = Wikia::WikiFactory->new( city_id => 69704 );

#
# check for database
#
ok($wf->city_dbname eq "testdummy", "check for database name" );

#
# clear cache
#
$wf->clear_cache;
