package Wikia::Thumbnailer::Bitmap;

use Moose;
extends 'Wikia::Thumbnailer::Image';

sub thumb {
	my $self = shift;
	
	$self->parse();
}

sub is_correct() {
	my $self = shift;
	
type	image/gif	gif
type	image/ief	ief
type	image/ifs	ifs
type	image/jpeg	jpeg,jpg,jpe,jfif,pjpeg,pjp
type	image/png	png
	
	return 
}

1;
