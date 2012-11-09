#!/usr/bin/perl -w

use common::sense;
use feature "say";
use encoding "UTF-8";

use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";
use Getopt::Long;
use Data::Dumper;
use PHP::Serialization qw(serialize unserialize);
use File::Temp;
use File::Copy;
use Cwd;

use constant TMP_TEMPLATE => 'apacheconfXXXXXX';
use constant CHEF_SLOTS_TGZ => "cookbooks/apache2/files/default/slots-conf.tgz";

my ($chefrepo, $force);

GetOptions(
		'chef-repo=s' => \$chefrepo,
		'force' => \$force,
);
$chefrepo or die("Use --chef-repo to specify chef directory\n");

my ($dir, $tgz, $PWD);

print "Creating temporary directory...\n";
my $tmpdir = File::Temp::newdir( TEMPLATE => TMP_TEMPLATE, TMPDIR => 1, CLEANUP => 0 ) or die("...Can't create temporary directory: $!\n");
print "...created: $tmpdir\n";

print "Generating apache config...\n";
$dir = "$tmpdir/slots";
mkdir( $dir );
system("perl $Bin/generate_apache_config.pl --dest '$dir'");
$? == 0 or die('...failed: $!\n');
 
opendir(DIR, $dir) or die "...Can't open directory '$dir': $!\n";
# Grab everything looks like slot configuration
my @slots = grep { /^slot\d+\.conf/ } readdir(DIR);
closedir(DIR);

scalar(@slots) or die ("...failed: no slot configuration files found\n");

$tgz = "$tmpdir/slots.tgz";
print "Gzipping slot configuration...\n";
$PWD = getcwd();
chdir($dir);
system("tar -zcf $tgz *");
my $tarres = $?;
chdir($PWD);
$tarres == 0 or die("...failed: $!\n");
die ("...tgz file not found\n") if ! -e $tgz;

print "Updating chef configuration repo...\n";
system("svn up $chefrepo");
$? == 0 or die("...failed: $!\n");

print "Copying slots.conf to chef repo...\n";
die("...old slots.tgz not found - is --chef-repo correct?") if ! -e ("$chefrepo/".CHEF_SLOTS_TGZ) && ! $force;
copy($tgz,"$chefrepo/".CHEF_SLOTS_TGZ) or die("...Can't copy file tgz to chef repo\n");

print "Uploading cookbook...\n";
system("knife cookbook upload apache2");
$? == 0 or die("...failed: $!");

print "Committing new slots.conf...\n";
system("svn ci -m \"uploaded new apache2 slots configuration\" $chefrepo");
$? == 0 or die("...failed: $!");

1;



