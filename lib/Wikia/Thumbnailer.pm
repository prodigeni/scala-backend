package Wikia::Thumbnailer;
use Dancer ':syntax';
use Dancer::Plugin;
use Dancer::Hook;

use common::sense;
use feature "say";

use Data::Dumper;
use Data::Types qw(:all);

use Wikia::Thumbnailer::SVG;
use Wikia::Thumbnailer::Image;
use Wikia::Thumbnailer::Video;

our $VERSION = '0.1';

umask( 0002 );

hook before => sub {
	error "Request: " . request->path();
	my $path = request->path();
	my $path_info = request->path_info();
	$path_info =~ s/(http(s?):\/\/)([^\/]+\.[^\/]+)//gi;
	$path =~ s/(http(s?):\/\/)([^\/]+\.[^\/]+)//gi;
	$path =~ s/\/{1,}/\//gi;
	$path_info =~ s/\/{1,}/\//gi;
	request->path($path);
	request->path_info($path_info);
};

register make_thumb => sub {
	my $conf = plugin_setting();
	my $class = vars->{class};
	my $Thumb = $class->new (
		config		=> config,
		thumb		=> vars->{thumbpath},
		wikia 		=> vars->{dbname},
		archive 	=> ( vars->{archive} ) ? true : false,
		file_name 	=> vars->{filename},
		file_ext	=> vars->{fileext},
		thumb_size 	=> vars->{width},
		thumb_name 	=> vars->{thumbname},
		thumb_ext 	=> vars->{thumbext}
	);

	my $t_start = [ $Thumb->current_time() ];
	my $response = $Thumb->make_thumb();
	my $t_elapsed = $Thumb->interval_time( $t_start );
	if ( is_int( $response ) ) {
		status 'not_found';
		template 'thumb_error', { error => $Thumb->disp_error };
		error "Response: " . $response . " (ERROR)";
		$response = $Thumb->disp_error;
	} else {
		status 'ok';
		headers
			'Cache-Control' 		=> sprintf( "max-age=%d", $Thumb->config->{max_age} ),
			'Content-Length' 		=> $Thumb->thumb_length,
			'Last-Modified'			=> $Thumb->last_modified,
			'Connection' 			=> 'keep-alive',
			'X-Thumbnailer-Hostname'=> $Thumb->hostname,
			'Content-Type'			=> $Thumb->thumb_type->{'mimetype'},
			'X-Thumbnailer-Time'	=> $t_elapsed;

		if ( vars->{send_file} ) {
			header "X-LIGHTTPD-send-file" => $Thumb->thumb_path;
		}

		error "Response: 200 (OK)";
	}
	return $response;
};

register_plugin;

=index
Default index
=cut
get '/' => sub {
	template 'wikia_404', { path => request->path };
};

=status
Return server status
=cut
get '/status' => sub {
	return config->{status_msg};
};

=enf
Return server env
=cut
get '/env' => sub {
	return "<pre>". Dumper( config ) . "</pre>";
};

=image
Image (png|gif|jpg) thumbnailer
=cut
get qr{ \/(\w\/(.+)\/(images|avatars)\/thumb((?!\/archive).*|\/archive)?\/\w\/\w{2}\/(.+)\.(jpg|jpeg|png|gif{1,}))\/(((\d+)px|(\d+x\d+)|(\d+x\d+x\d+))\-(.+)\.(jpg|jpeg|jpe|png|gif))$ }xi => sub {
	my ( @params ) = splat;
	
	debug "Image (png|gif|jpg) thumbnailer";
	
	# $thumbpath, $dbname, $archive, $filename, $fileext, $width, $thumbname, $thumbext
	var thumbpath 	=> $params[0];
	var dbname 		=> $params[1];
	var type		=> $params[2];
	var archive		=> $params[3];
	var filename	=> $params[4];
	var fileext		=> $params[5];
	var width		=> $params[8] || $params[7];
	var thumbname	=> $params[6];
	var thumbext	=> $params[10];
	var class		=> "Wikia::Thumbnailer::Image";

	debug sprintf ( "Bitmap: %s, %s.%s (thumb: %s.%s), size: %d", vars->{dbname}, vars->{filename}, vars->{fileext}, vars->{thumbname}, vars->{thumbext}, vars->{width} );

	return &make_thumb;
};

=svg
SVG thumbnailer
=cut
get qr{ \/(\w\/(.+)\/images\/thumb((?!\/archive).*|\/archive)?\/\w\/\w{2}\/(.+)\.svg)\/(((\d+)px|(\d+x\d+)|(\d+x\d+x\d+))\-(.+)\.(.*))$ }xi => sub {
	my ( @params ) = splat;

	debug "SVG thumbnailer";
	
	# $thumbpath, $dbname, $archive, $filename, $width, $thumbname
	var thumbpath 	=> $params[0];
	var dbname 		=> $params[1];
	var archive		=> $params[2];
	var filename	=> $params[3];
	var fileext		=> 'svg';
	var width		=> $params[5];
	var thumbname	=> $params[4];
	var thumbext	=> 'png';
	var class		=> "Wikia::Thumbnailer::SVG";

	debug sprintf ( "SVG: %s, %s (thumb: %s), width: %d", vars->{dbname}, vars->{filename}, vars->{thumbname}, vars->{width} );

	return &make_thumb;
};

=video
Video (OGG) thumbnailer
=cut
get qr{ \/(\w\/(.+)\/images\/thumb((?!\/archive).*|\/archive)?\/\w\/\w{2}\/(.+)\.ogg)\/((\d+px|seek=\d+|mid)\-(.+)\.(jpg))$ }xi => sub {
	my ( @params ) = splat;

	debug "OGG extension";
	my $width = undef;

	( $width ) = $params[5] =~ /^(\d+)px$/ if $params[5] =~ /^(\d+)px$/;
	( $width ) = $params[5] =~ /^seek=(\d+)$/ if $params[5] =~ /^seek=(\d+)$/;
	$width = -1 if $params[5] =~ /^mid$/;

	#  $thumbpath, $dbname, $archive, $filename, $width, $thumbname
	var thumbpath => $params[0];
	var dbname    => $params[1];
	var archive   => $params[2];
	var filename  => $params[3];
	var fileext   => 'ogg';
	var width     => $width;
	var thumbname => $params[4];
	var thumbext  => $params[7];
	var send_file => 1;
	var class     => "Wikia::Thumbnailer::Video";

	debug sprintf( "Video: %s, %s (thumb: %s), width: %d", vars->{dbname}, vars->{filename}, vars->{thumbname}, $width );

	return &make_thumb;
};

=without extension
Image without extension
=cut
get qr{ \/(\w\/(.+)\/images\/thumb((?!\/archive).*|\/archive)?\/\w\/\w{2}\/(.+))\/((((v\,([0-9a-f]{6}\,)?)?\d+)px|(\d+x\d+)|(\d+x\d+x\d+))\-(.+))$ }xi => sub {
	my ( @params ) = splat;

	debug "Image without extension";

	# $thumbpath, $dbname, $archive, $filename, $width, $thumbname
	var thumbpath 	=> $params[0];
	var dbname 		=> $params[1];
	var archive		=> $params[2];
	var filename	=> $params[3];
	var fileext		=> '';
	var width		=> $params[5];
	var thumbname	=> $params[4];
	var thumbext	=> '';
	var class		=> "Wikia::Thumbnailer::Image";

	debug sprintf ( "MW File %s (thumb: %s), width: %d", vars->{dbname}, vars->{filename}, vars->{width} );

	return &make_thumb;
};

=default request
404
=cut
any qr{.*} => sub {
	status 'not_found';
	template 'wikia_404', { path => request->path };
};

true;
