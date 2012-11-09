package Wikia::Thumbnailer::Video;

use Dancer;
use Cwd;
use common::sense;
use feature "say";

use Moose;
use Data::Dumper;
use Data::Types qw(:all);
use File::Path qw(make_path);

extends 'Wikia::Thumbnailer::File';

sub _video_mid_time {
	my $self = shift;
	
	my $cmd = "/usr/bin/oggLength " . $self->file_path ;
	my $length = `$cmd`;
	my $time = to_int( $length / 1000 / 2 ) || 1;
	
	return $time ;
}

sub _build_image {
	my $self = shift;
	
	my $seek = ( $self->thumb_width == -1 ) ? $self->_video_mid_time() : $self->file_width;
	
	debug "Thumb width: " . $self->thumb_width . ", seek : $seek \n";
	
	my $pwd = getcwd();
	chdir( $self->config->{video_tmp_dir} );

	my $cmd  = $self->config->{video_thumb};
	my $args = sprintf( $self->config->{video_thumb_params}, $seek, $self->file_path );
	
	debug "Execute " . $cmd . " " . $args ;

	my @result = `$cmd $args`;

	my $out = join "", @result;
	my $find_file = $self->config->{video_thumb_result_parse};
	debug "Parse result with: " . $find_file ;
	my ( $file ) = $out =~ m/$find_file/;
	
	my $errstr = "";
	my $dir = dirname( $self->thumb_path );
	unless( -d $dir ) {
		unless( make_path( $dir, { err => \$errstr } ) ) {
			debug "Could not create folder for $dir: $errstr";
		}
	}
			
	debug "Move file " . $file . " to " . $self->thumb_path ;
			
	if ( rename $file, $self->thumb_path ) {
		chmod 0664, $self->thumb_path;
		# read image thumbnail
		debug "Read thumb content " . $self->thumb_path ;
		my $len = 0;
		use bytes;
		if ( open ( FH, "<" . $self->thumb_path ) ) {
			binmode (FH);
			my ($buf, $data, $n);
			while (($n = read FH, $data, 65536) != 0) { 
				$buf .= $data;
			}
      
			$self->thumb_content( $buf );
			$len = length( $self->thumb_content );
			close (FH);	
		} else {
			debug "Cannot open file: " . $self->thumb_path . "!";
		}
		
		$self->thumb_length( $len );
		debug "Thumb length: " . $len;
				
		no bytes;
	} else {
		debug "Cannot move file !";
	} 
	
	$self->get_filetype('thumb');
		
	chdir( $pwd );	
}

sub make_thumb {
	my $self = shift;
	
	debug "Make thumbnailer: " . $self->thumb_url . " from video: " . $self->file_path;
	
	my $t_start = [ $self->current_time() ];
	
	# build thumbnail from ogg
	$self->_build_image();

	my $t_elapsed = $self->interval_time( $t_start );
	debug "File " . $self->thumb_path . " was written, time: $t_elapsed";	
	
	return $self->thumb_content;
}

1;
