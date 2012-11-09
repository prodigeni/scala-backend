package Wikia::Thumbnailer::File;

use Dancer;
use Moose;
use namespace::autoclean;

use common::sense;

use URI::Escape;
use LWP::UserAgent;

use DateTime;
use Time::HiRes qw(gettimeofday tv_interval);
use MIME::Types qw(by_suffix by_mediatype);

use File::LibMagic;
use File::Path qw(make_path);
use Math::Round qw(round);
use Sys::Hostname;
use Imager;

use Data::Dumper;
use Data::Types qw(:all);

=requested oryginal file
=cut
has file_url => (
	is 				=> "rw",
	isa				=> "Str",
	documentation	=> "URL address of original file",
);
has file_path => (
	is 				=> "rw",
	isa				=> "Str",
	documentation	=> "Path to original file",
);
has file_name => (
	is				=> "rw",
	isa 			=> "Str",
	required		=> 1,
	documentation 	=> "Name of original file"
);
has file_type => (
	is				=> "rw",
	isa				=> "HashRef",
	documentation	=> "Structure with MIME type, image type of requested image"
);
has file_ext => (
	is				=> "rw",
	isa				=> "Str",
	required		=> 1,
	documentation	=> "Original file extension"
);
has file_width => (
	is 				=> "rw",
	isa				=> "Num",
	documentation	=> "Oryginal file width"
);
has file_height => (
	is 				=> "rw",
	isa				=> "Num",
	documentation	=> "Oryginal file height"
);
has file_content => (
	is				=> "rw",
	isa				=> "Str",
	documentation	=> "File content"
);

=thumb options
=cut
has thumb => (
	is				=> "rw",
	isa				=> "Str",
	required		=> 1,
	documentation 	=> "Path of requested thumbnail",
	trigger			=> sub {
		my $self = shift;

		debug "File path: " . $self->thumb;
		debug "Thumb name: " . $self->thumb_name;

		$self->thumb_path( sprintf( "%s/%s", $self->config->{basepath}, $self->thumb ) );
		$self->thumb_url ( sprintf( "%s/%s", $self->config->{baseurl}, $self->thumb ) );

		# oryginal file path
		(my $file_path = $self->thumb_path) =~ s/\/thumb//i;
		$self->file_path( $file_path );

		# oryginal file URL
		(my $file_url = $self->thumb_url) =~ s/\/thumb\//\//i;

		#
		# sanity check & conversions, ugly hack
		# @todo: find a better way
		#
		unless( index( $file_url, "/" ) == -1 ) {
			my @parts = split( /\//, $file_url );
			my $last = pop @parts;
			$last = uri_escape_utf8( $last );
			$file_url = join( "/", @parts ) . "/" . $last;
		}

		$self->file_url( $file_url );

=cut
		if ( $self->file_ext eq 'svg' || $self->file_ext eq 'ogg' ) {
			my $thumb_path = sprintf( "%s.%s", $self->thumb_path, $self->thumb_ext );
			my $thumb_url =  sprintf( "%s.%s", $self->thumb_url, $self->thumb_ext );

			$self->thumb_path( $thumb_path );
			$self->thumb_url ( $thumb_url );
		}
=cut
		$self->thumb_path( sprintf( "%s/%s", $self->thumb_path, $self->thumb_name) );
		$self->thumb_url( sprintf( "%s/%s", $self->thumb_url, $self->thumb_name) );
	}
);
has thumb_path	=> (
	is 				=> "rw",
	isa				=> "Str",
	documentation	=> "Full path (with directory) to the requested thumbnail"
);
has thumb_url => (
	is 				=> "rw",
	isa				=> "Str",
	documentation	=> "Full URL (with domain) path to requested thumbnail"
);
has thumb_name => (
	is				=> "rw",
	isa				=> "Str",
	required		=> 1,
	trigger			=> sub {
		my $self = shift;
		debug "Thumbname: " . $self->thumb_name;
		if( $self->thumb_name =~ /(\d+),(\d+),(\d+),(\d+)/ ) {
			$self->manipulation( { 'x1' => $1, 'x2' => $2, 'y1' => $3, 'y2' => $4 } );
		}
	},
	documentation	=> "Full name of requested thumbnail"
);
has thumb_ext => (
	is				=> "rw",
	isa				=> "Str",
	required		=> 1,
	documentation	=> "Extension of requested thumbnail"
);
has thumb_length => (
	is 				=> "rw",
	isa				=> "Num"
);
has thumb_size => (
	is 				=> "rw",
	isa 			=> "Str",
	default			=> 0,
	trigger			=> sub {
		my $self = shift;
		debug ( "Thumb_size params: " . $self->thumb_size );
		my ( $w, $h, $r, $z, $color, $scale );
		# size = (\d+)px
		if ( $self->thumb_size =~ /^(\d+)$/ ) {
			$w = $1;
		}
		# size = (\d+x\d+) => width x height
		elsif ( $self->thumb_size =~ /^(\d+)x(\d+)$/ ) {
			$w = $1;
			$h = $2;
			$scale = $self->config->{default_scale};
		}
		# size = (\d+x\d+x\d+) => width x height x scale (0 - 10)
		elsif ( $self->thumb_size =~ /^(\d+)x(\d+)x(\d+)$/ ) {
			$w = $1;
			$h = $2;
			$scale = $3;
			$scale = $self->config->{scale}->{max} if ( $scale > $self->config->{scale}->{max} );
			$scale = 0 if ( $scale < 0 );
		} 
		elsif ( $self->thumb_size =~ /^v\,([0-9a-f]{6}\,)?(\d+)px$/i ) {
			$color = $1 || $self->config->{background};
			$w = $2;
			$z = 1;
		} 
		else {
			$w = $self->thumb_size;
		}

		if ( $w > $self->config->{maxwidth} ) {
			$w = $self->config->{maxwidth};
		}

		debug "Thumb size params: $w, $r, $h, $scale";

		$self->thumb_width( $w ) if ( $w );
		$self->thumb_height( $h ) if ( $h );
		$self->thumb_ratio( $r ) if ( $r );
		$self->thumb_zoom( $z ) if ( $z );
		$self->thumb_scale( $scale ) if ( $scale );
		$self->thumb_background( $color ) if ( $color );
	},
	documentation	=> "Thumbnail's size"
);
has thumb_width => (
	is 				=> "rw",
	isa 			=> "Str",
	default			=> 0,
	documentation	=> "Thumbnail's width"
);
has thumb_height => (
	is				=> "rw",
	isa 			=> "Int",
	default			=> 0,
	documentation	=> "Thumbnail's height"
);
has thumb_ratio => (
	is 				=> "rw",
	isa				=> "Num",
	default 		=> sub {
		my $self = shift;
		debug "default ratio: " . $self->thumb_ratio;
		return ( $self->file_ext eq 'svg' ) ? $self->config->{svg_ratio} : 0;
	}
);
has thumb_zoom => (
	is 				=> "rw",
	isa				=> "Num",
	default 		=> 0
);
has thumb_scale => (
	is				=> "rw",
	isa				=> "Num",
	default			=> 0,
	documentation	=> 'Scale thumb for width and height params'
);
has thumb_background => (
	is 				=> "rw",
	isa				=> "Str",
	default 		=> sub {
		return shift->config->{background};
	}
);
has thumb_content => (
	is				=> "rw",
	isa				=> "Str",
	documentation	=> "Thumb content"
);
has thumb_type => (
	is				=> "rw",
	isa				=> "HashRef",
	documentation	=> "Structure with MIME type and image type of thumbnail"
);
has image => (
	is 				=> "rw",
	isa				=> "Imager",
	documentation	=> "Instance of Imager"
);

=other parameters
=cut
has config => (
	is				=> "rw",
	isa 			=> "HashRef",
	required 		=> 1,
	default 		=> sub { {} }
);
has wikia => (
	is				=> "rw",
	isa				=> "Str",
	required		=> 1,
	documentation 	=> "Wikia dbname"
);
has archive => (
	is 				=> "rw",
	isa 			=> "Bool",
	required		=> 1,
	documentation	=> "File exists in archive directory",
	default			=> 0
);
has last_modified => (
	is				=> "rw",
	isa				=> "Str",
	documentation	=> "Date of last file modification"
);
has libfile => (
	is 				=> "rw",
	isa				=> "File::LibMagic",
	documentation	=> "Instance of File::LibMagic",
	default			=> sub { return new File::LibMagic }
);
has manipulation => (
	is 				=> "rw",
	isa 			=> "HashRef",
	default 		=> sub { {} },
	documentation	=> "Additional parameters needed to image manipulation",
	trigger			=> sub {
		my $self = shift;
		$self->need_manipulation (
			(
				is_int( $self->manipulation->{'x1'} ) &&
				is_int( $self->manipulation->{'x2'} ) &&
				is_int( $self->manipulation->{'y1'} ) &&
				is_int( $self->manipulation->{'y2'} )
			) ? 1 : 0
		);
	}
);
has need_manipulation => (
	is 				=> "rw",
	isa				=> "Int",
	documentation	=> "Check if image need some manipulation (crop, resize, etc.)",
	default			=> 0
);
has hostname => (
	is 				=> "rw",
	isa				=> "Str",
	default			=> sub { return hostname; }
);
has disp_error => (
	is 				=> "rw",
	isa				=> "Str"
);
has error_code => (
	is				=> "rw",
	isa				=> "Int"
);

sub interval_time {
	my ( $self, $t_start ) = @_;

	return tv_interval( $t_start, [ $self->current_time() ] );
}

sub current_time {
	my $self = shift;
	return gettimeofday();
}

sub fetch_content {
	my $self = shift;

	my $t_start = [ $self->current_time() ];

	my $datetime = DateTime->now();
	my $last_modified = $datetime->strftime( "%a, %d %b %Y %T GMT" );

	debug "Fetch file content: " . $self->file_url;
	# fetch content using LWP
	use bytes;
	my $ua = LWP::UserAgent->new();
	$ua->default_header('Authenticate' => '1');
	$ua->env_proxy;
	$ua->timeout( $self->config->{lwp_timeout} );
	$ua->proxy( "http", $self->config->{lwp_proxy} ) if $self->config->{lwp_proxy};

	my $response = $ua->get( $self->file_url );

	# got it?
	if ( $response->is_success ) {
		$self->file_content( $response->content );
		$last_modified = $response->header("Last-Modified") if ( $response->header("Last-Modified") );
	}
	no bytes;

	$self->last_modified( $last_modified );

	debug "Read remote " . $self->file_url . ", content-length: " . length( $self->file_content ) . ", time: " . $self->interval_time( $t_start ) . ", code: " . $response->code();

	# return response code
	return $response->code();
}

sub get_filetype {
	my ( $self, $type ) = @_;

	debug "Make mimetype for: " . $type . " and file suffix: " . $self->thumb_ext;

	my $mimetype;
	if ( $type eq 'thumb' ) {
		if ( $self->thumb_ext ) {
			( $mimetype ) = by_suffix( $self->thumb_ext );
		} else {
			$mimetype = $self->file_type->{'mimetype'};
		}
	} else {
		$mimetype = $self->libfile->checktype_contents( $self->file_content );
	}

	# check allowed image type
	my $imgtype;
	$mimetype = $1 if ( $mimetype =~ s/(.*)[\;?](charset=)?// );

	debug "Check mime type from image_mimetype structure: " .  $mimetype . ", structure: " . $self->config->{image_mimetype}->{ $mimetype };
	if ( defined $self->config->{image_mimetype}->{ $mimetype } ) {
		$imgtype = $self->config->{image_mimetype}->{ $mimetype };
	} else {
		( $imgtype ) = $mimetype =~ m![^/+]/(\w+)!;
	}

	my $info = {
		'mimetype' 	=> $mimetype,
		'imgtype' 	=> $imgtype
	};

	debug "Mime type ( $type ): " . $mimetype . ", image type: " . $imgtype;

	if ( $type eq 'file' ) {
		$self->file_type( $info );
	} else {
		$self->thumb_type( $info );
	}
}

sub parse {
	my $self = shift;

	my $t_start = [ $self->current_time() ];
	my $code = $self->fetch_content();

	if ( $code != 200 ) {
		debug "Cannot fetch image: " . $self->file_url;
		return $code;
	}
	$self->get_filetype('file');
	$self->get_filetype('thumb');

	debug "Image parsed, time: " . $self->interval_time( $t_start );

	return 0;
}

sub save_thumb {
	my $self = shift;

	debug "Make thumb: " . $self->thumb_type->{'imgtype'} . ", quality: " . $self->config->{jpegquality} ;

	my $output;
	if ( $self->image->write( data => \$output, type => $self->thumb_type->{'imgtype'}, jpegquality => $self->config->{jpegquality} ) ) {
		use bytes;

		$self->thumb_content( $output );
		my $len = length( $self->thumb_content );

		$self->thumb_length( $len );
		debug "Thumb length: " . $len;

		if ( $len > 0 ) {
			$self->save_file();
		}
		no bytes;
	} else {
		debug "Cannot write image data: " . $self->image->errstr;
	}
}

sub save_file {
	my $self = shift;

	my $errstr = "";
	my $dir = dirname( $self->thumb_path );
	unless( -d $dir ) {
		unless( make_path( $dir, { err => \$errstr } ) ) {
			debug "Could not create folder for $dir: $errstr";
		}
	}

	#unless( write_file( $self->thumb_path, {binmode => ':raw' }, $self->thumb_content ) ) {
	unless ( $self->image->write( file => $self->thumb_path, type => $self->thumb_type->{'imgtype'} ) ) {
		debug "Could not write thumbnail: " . $self->thumb_path;
	}
}

sub scale {
	my $self = shift;

	my $old_h = $self->thumb_height;
	my $old_w = $self->thumb_width;

	if ( $self->thumb_width == 0  )  {
		$self->thumb_height( 0 );
	}
	else {
		if ( !$self->thumb_height ) {
			$self->thumb_height( round( $self->file_height * $self->thumb_width /  $self->file_width ) );
		}
	}
		
	debug "Resize file: " . $old_w . " x " . $old_h . " -> " . $self->thumb_width . " x ". $self->thumb_height;
}

sub make_thumb {
	my $self = shift;

	debug "Make thumbnailer for: " . $self->thumb_url . " from image: " . $self->file_url;

	my $code = $self->parse();

	debug "width: " . $self->thumb_width;
	debug "height: " . $self->thumb_height;
	debug "ratio: " . $self->thumb_ratio;

	my $cropped = 0;

	my $t_start = [ $self->current_time() ];

	if ( 0 != $code ) {
		my $err = "Cannot find image: " . $self->file_url;
		debug $err;
		$self->disp_error( $err );
		return $code;
	}

	debug "Create image from fetched content";
	$self->_build_image();

	if ( ! defined $self->image ) {
		if ( ! $self->error_code ) {
			my $err = "Cannot create thumb: " . $self->thumb_url . " from image: " . $self->file_url;
			debug $err;
			$self->disp_error( $err );
			$self->error_code( 500 );
		}
		return $self->error_code;
	}

	debug "Save thumbnail: " . $self->thumb_path;

	$self->save_thumb();

	my $t_elapsed = $self->interval_time( $t_start );
	debug "File " . $self->thumb_path . " was written, time: $t_elapsed";

	return $self->thumb_content;
}

sub write_file {
	my ( $self ) = @_;
	my $filename = sprintf( "/tmp/%s", $self->file_name );
	open OUT,">$filename" or debug "Cannot create tmp file $filename";
	binmode OUT;
	print OUT $self->file_content;
	close OUT;
	
	debug "Use GraphicsMagick to read corrupted file - save content in $filename";
	
	return $filename;
}

sub fixname {
	my ( $self ) = @_;
	my $filename = sprintf( "/tmp/thumb_%s_%d.%s", time(), $self->file_name, $self->thumb_ext ); 

	debug "Use GraphicsMagick to read corrupted file - convert corrupted file and save in $filename";
	return $filename;
}

sub content_read {
	my ( $self, $image ) = @_;
	my $result = 0;
	
	# try to read image with Imager library 
	if ( $self->file_content ) {
		if ( ! $image->read( data => $self->file_content, type => $self->file_type->{'imgtype'} ) ) {
			debug "Read error ( ". $self->file_type->{'imgtype'} . "): " . $image->errstr;
			# use ImageMagick to read invalid files - this library is too heavy to use live, but can be used
			# as second option for reading corrupted files
=todo
			use Graphics::Magick;
			my $p = new Graphics::Magick;

			my $filename = $self->write_file();
			my $fixname = $self->fixname();
			eval {
				my $read = $p->Read( $filename );
				debug "GraphicsMagick read error: " . $read if ( $read );
				my $write = $p->Write( $fixname );
				debug "GraphicsMagick write error: " . $write if ( $write );
			};

			$p = undef if ( $@ );
			
			if ( defined $p ) {
				$result = 1 if ( $image->read( file => $fixname ) );
			}
			
			eval {
				# clear tmp files
				unlink( $fixname );
				unlink( $filename );
			}
=cut			
		} else {
			debug "File was read with original image library";
			$result = 1;
		}
	}
	
	return $result;
}

__PACKAGE__->meta->make_immutable;

=head1 NAME

Wikia::Thumbnailer::File - Moose role for other transformers

=head1 DESCRIPTION

Wikia::Thumbnailer::File is base Moose role for other transformers

=head1 AUTHOR

Krzysztof Krzyżaniak (eloy)
Piotr Molski (moli)

=head1 COPYRIGHT

The following copyright notice applies to all the files provided in
this distribution, including binary files, unless explicitly noted
otherwise.

Copyright 2011 Wikia Inc.

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut


1;
