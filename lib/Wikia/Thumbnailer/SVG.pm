package Wikia::Thumbnailer::SVG;

use Dancer;
use common::sense;
use feature "say";
use Data::Dumper;

use Moose;
use Image::LibRSVG;
use Image::Info qw(image_info dim);
use XML::Simple;
use Data::Types qw(:all);
@Image::Info::SVG::PREFER_MODULE = qw(Image::Info::SVG::XMLLibXMLReader Image::Info::SVG::XMLSimple);

extends 'Wikia::Thumbnailer::File';

sub _prepare_size {
	my $self = shift;
	
	my $info = image_info( \$self->file_content );
	
	# check width/height from image info
	my ( $origw, $origh ) = ( 0, 0 );
	( $origw, $origh ) = dim( $info );
	
	$origw = $self->scaleSVG( $origw );
	$origh = $self->scaleSVG( $origh );
	$self->thumb_ratio( $origw / $origh ) if $origh > 0;
	
	debug "Original size " . $origw . " x " . $origh . ", ratio: " . $self->thumb_ratio . " \n";

	# check width/height from viewBox
	unless( $origw && $origh ) {
		# http://www.w3.org/TR/SVG/coords.html#ViewBoxAttribute
		my $xmlp = XMLin( $self->file_content );
		debug "There's no width and height defined for SVG file, checking viewbox \n";
		my $viewBox = $xmlp->{ "viewBox" };
		if( $viewBox && $viewBox =~/\d+[\s|,]*\d+[\s|,]*(\d+)[\s|,]*(\d+)/ ) {
			$origw = $1;
			$origh = $2;
			$self->thumb_ratio( $origw / $origh ) if $origh;
			debug "Viewbox parse: size " . $origw . " x " . $origh . ", ratio: " . $self->thumb_ratio . " \n";
		}
	} 

	# still don't have width/height - set default params
	unless( $origw && $origh ) {
		$self->thumb_ratio( 1 ) unless $self->thumb_ratio;
		$origw = $self->config->{svg_def_width};
		$origh = $self->config->{svg_def_width} / $self->thumb_ratio;
		debug "Default width/heigth params: " . $origw . " x " . $origh . ", ratio: " . $self->thumb_ratio . " \n";
	}

	$self->file_width( $origw );
	$self->file_height( $origh );	
	
	undef $info;	
}

sub _make_output {
	my $self = shift;
	
	my $t_start = [ $self->current_time() ];
	my $rsvg = new Image::LibRSVG;

	my $w = $self->thumb_width;
	my $h = $self->thumb_height;
	
	if ( $self->need_manipulation ) {
		$w = $self->config->{svg_def_width};
		$h = $w / $self->thumb_ratio;
	}
				
	my $args = { "dimension" => [$w, $h], "dimesion" => [$w, $h] }; # bug in module!
	$rsvg->loadImageFromString( $self->file_content, 0, $args );
	my $output = $rsvg->getImageBitmap( $self->thumb_ext );

	my $t_elapsed = $self->interval_time( $t_start );
	debug "Reading svg as image (for transforming), time: " . $t_elapsed;
		
	my $image = Imager->new;
	
	$image->read( data => $output, type => $self->thumb_ext );

	$t_elapsed = $self->interval_time( $t_start );
	debug "Creating " . $w . " x " . $h . " preview from svg file for cropping, time: " . $t_elapsed;

	if ( $self->need_manipulation ) {
		$w = $self->manipulation->{'x2'} - $self->manipulation->{'x1'};
		$h = $self->manipulation->{'y2'} - $self->manipulation->{'y1'};
		if( $w > 0 && $h > 0 ) {
			$image = $image->crop(
				left 	=> $self->manipulation->{'x1'}, 
				top 	=> $self->manipulation->{'y1'}, 
				right 	=> $self->manipulation->{'x2'}, 
				bottom 	=> $self->manipulation->{'y2'}  
			);
				
			$t_elapsed = $self->interval_time( $t_start );
			debug "Cropping into " . $self->manipulation->{'x1'} . ", " . $self->manipulation->{'x2'} . " x " . $self->manipulation->{'y1'} . ", " . $self->manipulation->{'y2'} . ", time: $t_elapsed";
		}
	} 

	$image = $image->scale( xpixels => $self->thumb_width, ypixels => $self->thumb_height, qtype => 'mixing' );
	
	$self->image( $image );
	
	undef $rsvg;
}

sub _build_image {
	my $self = shift;
	my $cropped = 0;

	my $t_start = [ $self->current_time() ];
	my $t_elapsed = 0;

	debug "Read SVG from content (hostname: " . $self->hostname . ") \n";
	
	if ( 
		( $self->file_type->{'mimetype'} =~ m!^image/svg\+xml! || $self->file_type->{'mimetype'} =~ m!text/xml! || $self->file_ext eq "svg" ) 
		&&  
		( $self->thumb_ext eq 'png' ) 
	) {
		
		# prepare widh/height/ratio
		$self->_prepare_size();
		
		# make proper image scale
		$self->scale();

		$t_elapsed = $self->interval_time( $t_start );
		debug "Reading svg as xml (for size checking) " . $self->thumb_width . " x " . $self->thumb_height . ", time: " . $t_elapsed;

		my $output = $self->_make_output();
		
		if ( ! defined $self->image ) {
			debug "Cannot create thumb: " . $self->thumb_url . " from SVG file: " . $self->file_url;
			return 500;		
		}

		$t_elapsed = $self->interval_time( $t_start );
		debug "File " . $self->thumb_path . " was written, time: " . $t_elapsed;	

	} else {
		debug "Invalid mimetype of requested SVG file: " . $self->file_type->{'mimetype'} . " or thumb request: " . $self->thumb;
	}
}

# taken from ImageFunctions.php
sub scaleSVG {
	my( $self, $size ) = @_;

	return 0 unless defined $size;

	my %units = (
		"px" => 1.0,
		"pt" => 1.25,
		"pc" => 15.0,
		"mm" => 3.543307,
		"cm" => 35.43307,
		"in" => 90.0,
		"em" => 16.0, # fake it?
		"ex" => 12.0, # fake it?
	);

	if( $size =~ /^\s*(\d+(?:\.\d+)?)(em|ex|px|pt|pc|cm|mm|in|%|)\s*$/ ) {
		$size = to_float( $1 );
		my $u = $2;

		if( $u eq "%" ) {
			$size = $size * 0.01 * $self->config->{svg_def_width};
		}
		elsif( exists( $units{ $u } ) ) {
			$size = $size * $units{ $u };
		}
	}

	$size = to_float( $size );

	return $size;
}

1;
