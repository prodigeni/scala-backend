#!/usr/bin/env perl

use strict;
use warnings;

use Data::Dumper;
use Getopt::Long qw(:config pass_through);
use FindBin qw/$Bin/;
use lib "$Bin/../lib/";

use Wikia::LB;

#
# defaults
#
my $type = "master";
my $name = "wikicities";

GetOptions( "type=s" => \$type, "name=s" => \$name );

#
# get connection to given database
#
my $dbh = Wikia::LB->instance->getConnection( $type eq "master" ? Wikia::LB::DB_MASTER : Wikia::LB::DB_SLAVE, undef, $name );
my $info = Wikia::LB->instance->info();
print qq{-u$info->{user} -p$info->{pass} -h$info->{host} $info->{name}\n}
