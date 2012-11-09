package Wikia::Thumbnailer::Image;

use Dancer;
use Dancer::Exception qw(:all);
use common::sense;

use Moose;
use Data::Dumper;
use Math::Round qw(round);
use List::Util qw(min max);

extends 'Wikia::Thumbnailer::File';

sub _build_image {
	my $self = shift;
	my $cropped = 0;

	my $t_start = [ $self->current_time() ];

	debug "Read image from content (hostname: " . $self->hostname . ")";
	my $image = Imager->new;
	#if ( ! $image->read( data => $self->file_content, type => $self->file_type->{'imgtype'} ) ) {
	if ( !$self->content_read( $image ) ) {
		$self->error_code( 404 );
		$self->disp_error( "Cannot read content for image: " . $self->file_url );
		return;
	}

	# for params (x1, y1, x2, yx )
	if ( $self->need_manipulation ) {

		#
		# cut rectangle from original, do some checkups first
		#
		my $imgw  = $image->getwidth() || 0;
		my $imgh  = $image->getheight() || 0;

		my $w = $self->manipulation->{'x2'} - $self->manipulation->{'x1'};
		my $h = $self->manipulation->{'y2'} - $self->manipulation->{'y1'};
		debug "Crop: $w x $h ( original size: $imgw x $imgh )";
		if( ( $w > 0 && $h > 0 ) && ( $w <= $imgw && $h <= $imgh ) ) {
			$image = $image->crop(
				left 	=> $self->manipulation->{'x1'},
				top 	=> $self->manipulation->{'y1'},
				right 	=> $self->manipulation->{'x2'},
				bottom 	=> $self->manipulation->{'y2'}
			);
			my $t_elapsed = $self->interval_time( $t_start );
			debug "Cropping into " . $self->manipulation->{'x1'} . ", " . $self->manipulation->{'x2'} . " x " . $self->manipulation->{'y1'} . ", " . $self->manipulation->{'y2'} . ", time: $t_elapsed";
			$cropped = 1;
		}

		if ( $self->thumb_ratio ) {
			debug "Scale image with ratio: " . $self->thumb_ratio ;
			$image = $image->scale(
				xpixels => $image->getwidth() * $self->thumb_ratio,
				ypixels => $image->getheight() * $self->thumb_ratio,
				qtype => 'normal',
				type => 'nonprop'
			);
		}
	}

	my $origw  = $image->getwidth() || 0;
	my $origh  = $image->getheight() || 0;

	debug "Original size " . $origw . " x " . $origh;
	if ( $origw && $origh ) {
		# original size
		$self->file_width( $origw );
		$self->file_height( $origh );

		# make proper image scale
		$self->scale();

		debug "Scale thumb: " . $self->thumb_width . " x " . $self->thumb_height . ", file: " . $self->file_width . " x " . $self->file_height . ", scale: " . $self->thumb_scale;
		
		# scale image for W X H params
		if ( defined $self->thumb_scale && $self->thumb_scale > 0 ) {		
			debug "Use new method of image scaling with (WIDTH)x(HEIGHT) params";

			if ( $self->file_width < $self->thumb_width || $self->file_height < $self->thumb_height ) {					
				# generate transparent PNG
				my $new_image = Imager->new( 
					xsize => $self->thumb_width, 
					ysize => $self->thumb_height,
					channels => 4
				);
		
				my $top = ( $self->file_height - $self->thumb_height ) * ( 1 - ( ( 10 - $self->thumb_scale ) / 10 ) );
				if ( $self->file_height > $self->thumb_height || $self->file_width > $self->thumb_width ) {
					$image = $image->crop(
						width => $self->thumb_width,
						height => $self->thumb_height,
						top => $top
					);
				}
				
				debug "Put image " . $self->file_width . " x " . $self->file_height . " into transparent image " . $self->thumb_width . " x " . $self->thumb_height ;
				$new_image->paste(
					src		=> $image, 
					left	=> ( $self->thumb_width - $self->file_width ) / 2,
					top 	=> ( $self->thumb_height - $self->file_height ) / 2,
				);
				
				$image = $new_image;
						
			} else {
				my ( $w_ratio, $h_ratio ) = 0;
				$w_ratio = sprintf( "%0.1f", $self->file_width / $self->thumb_width ) if ( $self->thumb_width );
				$h_ratio = sprintf( "%0.1f", $self->file_height / $self->thumb_height ) if ( $self->thumb_height );
				
				# set new value of thumb ratio
				debug "Count thumb ratio: w_ratio: " . $w_ratio . ", h_ratio: " . $h_ratio;
				$self->thumb_ratio ( max ( min $w_ratio, $h_ratio ), 1 );

				debug "Scale image with ratio: " . $self->thumb_ratio ;
				$image = $image->scale(
					xpixels => $self->file_width / $self->thumb_ratio,
					ypixels => $self->file_height / $self->thumb_ratio,
					qtype => 'normal',
					type => 'nonprop'
				) if ( $self->thumb_ratio );

				my ( $img_width, $img_height ) = ( $image->getwidth(), $image->getheight() );
				my $top = ( $img_height - $self->thumb_height ) * ( 1 - ( ( 10 - $self->thumb_scale ) / 10 ) );
				debug "Crop image $img_width x $img_height, " . $self->thumb_width . " x " . $self->thumb_height . ", " . $top;
				$image = $image->crop(
					height	=> $self->thumb_height,
					width 	=> $self->thumb_width,
					top		=> int $top
				);
			}
		} 
		elsif ( $self->thumb_width < $self->file_width ) {
			$image = $image->scale(
				xpixels => $self->thumb_width,
				ypixels => $self->thumb_height,
				qtype => 'normal',
				type => 'nonprop'
			);
			debug "Image scaled to " . $self->thumb_width . " x " . $self->thumb_height;
		} 
		# zoom for video files
		elsif ( $self->thumb_zoom ) {
			if ( $self->thumb_width > $self->config->{maxzoom} ) {
				$self->thumb_width = $self->config->{maxzoom};
			}
			# for video thumbs - increase image size
			# if after image cropping, width > cropped image width * max zoom ratio
			# set fixed width
			my $new_w = $self->thumb_width;
			my $new_h = $self->thumb_height;

			if ( $self->thumb_zoom && $self->thumb_width > $self->file_width * $self->config->{maxzoomratio} ) {
				$new_w = $self->file_width * $self->config->{maxzoomratio};
				$new_h = round( $self->file_height * $new_w /  $self->file_width );
			}

			$image = $image->scale(
				xpixels => $new_w,
				ypixels => $new_h,
				qtype => 'normal',
				type => 'nonprop'
			);
			#$image->filter(type=>'flines');
			debug "Image scaled to " . $new_w . " x " . $new_h;
		}
	}

	if ( $cropped ) {
		#
		# for cropped images thumbnail which is smaller
		# than requested we add white border and put
		# thumbnail into it
		#
		my $crop_width = $image->getwidth();
		my $crop_height = $image->getheight();
		my $background_color = sprintf( "#%s", $self->thumb_background );

		debug "Size after cropping: " . $crop_width . " x " . $crop_height . ", requested size: " . $self->thumb_width . " x " . $self->thumb_height;
		debug "Background image: " . $background_color;

		if ( $crop_width <= $self->thumb_width ) {
			debug "crop smaller than requested width: $crop_width < " . $self->thumb_width;

			my $background = Imager->new ( 
				xsize => $self->thumb_width,
				ysize => $self->thumb_height 
			)->box( 
				filled => 1,
				color => $background_color 
			);
			debug "Crop size: " . $self->thumb_width . " x " . $self->thumb_height;

			my $offsetx = ( $self->thumb_width - $crop_width ) / 2;
			my $offsety = ( $self->thumb_height - $crop_height ) / 2;
			$background->paste( src => $image, left => $offsetx, top => $offsety );
			$image = $background;
		}
	}

	$self->image( $image );

	my $t_elapsed = $self->interval_time( $t_start );
	debug "Thumb created, time: $t_elapsed";
}

1;
