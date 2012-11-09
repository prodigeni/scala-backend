#!/usr/bin/env perl
use Dancer;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";

use Imager;
use LWP::UserAgent;
use HTTP::Request;

my $thumb_urls = 'http://test.thumbnailers.wikia.com/wiki/Thumb_test?action=raw';
my $thumb_server = 'http://10.8.34.21:5000';

my $ua = LWP::UserAgent->new;
my $req = HTTP::Request->new(GET => $thumb_urls);
my $res = $ua->request($req);

# Check the outcome of the response
if ($res->is_success) {
	my @urls = split /\n/, $res->content;
	
	foreach ( @urls ) {
		next unless ( $_ );
		next if ( /^\#/ );
		s/\*//g;
		my ( $url, $width, $height ) = split /\;/, $_, 3;
		print "Test url: " . $url . "\n";
	
		$req = HTTP::Request->new(GET => $thumb_server . $_);
		$res = $ua->request($req);
	
		if ( $res->is_success ) {
			my $mimetype = $res->header('Content-Type');
			my ( $imgtype ) = $mimetype =~ m![^/+]/(\w+)!;
			#print "\t mime: $mimetype, $imgtype \n";
			my $image = Imager->new;
			$image->read( data => $res->content, type => $imgtype );
			my $w = $image->getwidth();
			my $h = $image->getheight();
			if ( $w == $width && $h == $height ) {
				print "\tOK - image: " . $image->getwidth() . " x " . $image->getheight() . " => " . $width . " x " . $height . " time: " . $res->header('X-Thumbnailer-Time') . "\n";
			} else {
				print "\tERROR - image: " . $image->getwidth() . " x " . $image->getheight() . " => " . $width . " x " . $height . "\n";				
			}
		} else {
			print "\t" . $res->status_line . "\n";
		}
	}
}
else {
  print $res->status_line, "\n";
}

