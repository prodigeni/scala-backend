#!/usr/bin/perl
use strict; 
no strict 'refs';

use Data::Dumper;
use Getopt::Long;
use MIME::Lite;

$|++;
my ($file, $emails, $from, $subject) = ();
GetOptions( 
	'file=s'	=> \$file,
	'emails=s'	=> \$emails,
	'from=s'	=> \$from,
	'subject=s' => \$subject
);

my @emails = split(",", $emails);
if ( scalar @emails ) {
	foreach (@emails) {
		my $msg = MIME::Lite->new(
			From     => $from,
			To       => $_,
			Subject  => $subject,
			Path     => $file
		);
		$msg->send;
		print "send email $subject to " . $_ . " \n";
	}
}

1;
