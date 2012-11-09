#!/usr/bin/perl -w

use strict;

use Try::Tiny;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";

use Fcntl ":flock";
use Getopt::Long;
use Sys::Hostname;
use Data::Dumper;
use Digest::MD5 qw(md5_hex);
use File::Path;
use File::Copy qw(cp);

use Wikia::LB;


my $dbe = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::EXTERNALSHARED );
my $paths = get_dict($dbe,qq(SELECT city_dbname, cv_value from city_list l left join city_variables v on (l.city_id = v.cv_city_id) where cv_variable_id = 17;),'city_dbname','cv_value');

foreach my $k (keys %{$paths}) {
	if ( $paths->{$k} =~ /^[^"]*"(.*)"[^"]*$/ ) {
		$paths->{$k} = $1;
#		printf "%s = %s\n", $k, $paths->{$k};
	} else {
		die "error reading upload dir for $k\n";
	}
}

my ($oldname,$failed,$all) = ("",0,0);

while (<STDIN>) {
	chomp;
	my $line = $_;
	next if (substr($line,0,1) eq "#");
	my ($lang,$dbname,$file) = split('/',$line,3);
	my $lang_subdir = '';

#	$dbname = lc($dbname);
	print_wiki_stats($dbname);
#	if ( $lang ne "en" && substr($dbname,0,2) eq $lang ) {
#		$dbname = substr($dbname,2);
#		$lang_subdir = "/$lang";
#	}
	if ( substr($file,0,1) eq ":" ) {
		$file = substr($file,1);
	}
	my $hash = md5_hex( $file );

	my $path = sprintf("%s/%s/%s/%s",
		$paths->{$dbname},
		substr($hash, 0, 1),
		substr($hash, 0, 2),
		$file);
	if ( ! -f $path ) {
		print "$path\n";
		$failed++;
	}
	$all++;
}

print_wiki_stats("");

sub print_wiki_stats {
	my ($newname) = (shift);
	if ($newname ne $oldname) {
		if ($failed > 0) {
			printf "# %s %d/%d\n", $oldname, $failed, $all;
		}
		$oldname = $newname;
		$failed = 0;
		$all = 0;
	}
}

sub get_dict {
        my ($db, $q, $key, $vkey) = (shift,shift,shift,shift);
        my $stt = $db->prepare($q);
        return unless $stt->execute();

        my $data = {};
        my $value;
        while (my $r = $stt->fetchrow_hashref) {
                $data->{$r->{$key}} = $vkey ? $r->{$vkey} : $r;
        }
        $stt->finish();
        return $data;
}

