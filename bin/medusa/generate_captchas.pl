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

use Wikia::Utils;
use Wikia::DB;
use Wikia::LB;
use Wikia::Title;

use constant TMP_TEMPLATE => 'captchagenXXXXXX';

my ($slot, $captchaSecret);

GetOptions(
	'slot' => \$slot,
	'secret' => \$captchaSecret,
);

my $code = Wikia::Utils::code_path();
my $tmpdiro = File::Temp::newdir( TEMPLATE => TMP_TEMPLATE, TMPDIR => 1 );
my $tmpdir = $tmpdiro->dirname;

my $settings = Wikia::Settings->instance();
$captchaSecret = $settings->variables->{'wgCaptchaSecret'} if !defined $captchaSecret;

mkdir( $tmpdir . '/images' );


1;



