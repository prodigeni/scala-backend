package skindetector;
use Dancer ':syntax';
use Imager::SkinDetector;
use Time::HiRes qw(gettimeofday tv_interval);
use LWP::UserAgent;
use File::Temp;

our $VERSION = '0.1';

get '/' => sub {
    template 'start';
};

post '/detect' => sub {
	# Check a local file
	my $name = param('url');
	my $start = [ gettimeofday() ];
	my $error = '';
	
	my $url = param('url');
	my $image = undef;
	my $file_name = undef;
	if ( $url =~ m{^https?://} ) {
		my $ua = LWP::UserAgent->new();
		$ua->default_header('Authenticate' => '1');
		$ua->env_proxy;
		$ua->timeout( config->{lwp_timeout} );
		$ua->proxy( "http", config->{lwp_proxy} ) if config->{lwp_proxy};

		my $response = $ua->get( $url );		
		if ( $response->is_success ) {
			my $tmpf = File::Temp->new( UNLINK => 0 );
			$file_name = $tmpf->filename();
			binmode $tmpf;
			print $tmpf $response->content;
			close $tmpf;
			$image = Imager::SkinDetector->new(file => $file_name);
		} else {
			$error = 'Invalid file';
		}
	} else {
        $error = 'Invalid file';
	}
	
	if ( !defined $image ) {
		$error = "Can't load image [" . param('url') . "]";
		template 'error' => { error => $error };
	} else {
		my $skinniness = $image->skinniness();
		my $prob = $image->contains_nudity();
		
		unlink $file_name;
	
		my $etime = tv_interval( $start, [ gettimeofday() ] );
		template 'result' => { skinny => sprintf( "%3.2f%%", $skinniness * 100), prob => sprintf( "%.2f%%", $prob * 100 ), name => $name, etime => $etime };
	}
};

true;
