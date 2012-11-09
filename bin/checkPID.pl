#!/usr/bin/perl

use strict;
use Getopt::Long;
use Data::Dumper;

my $script = "";
my $exec = "";
my $dry = 0;
GetOptions(
	'script=s' => \$script,
	'exec=s' => \$exec,
	'dry' => \$dry
);

$exec = 'perl' unless $exec;
my @exec_params = split(/\s/, $exec);
foreach(split("\\n", `ps auxwwwe | grep '$script'`)) {
	my $cmd = $_;
	unless ($cmd  =~ m/$0|grep|launcher/) {
		my $match = 0;
		foreach ( @exec_params ) {
			$match++ if (grep /^\Q$_\E$/, $cmd);
		}
		if ( $match == scalar @exec_params ) {
			print "Process exists - so exit !!! \n" unless $dry;
			exit;
		}
	}
}

my $run = "$exec $script &";
exec("$run");
exit;
