#!/usr/bin/env perl

package Wikia::Cache::Warm;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use feature "say";
use Data::Dumper;
use Try::Tiny;
use Sys::Hostname qw(hostname);
use Thread::Pool::Simple;
use LWP::UserAgent;
use List::Util qw(shuffle);

use Moose;

has vcl_path => ( 
	is => "rw", 
	isa => "Str", 
	trigger => sub {
		my ( $self, $path ) = @_;
		$self->_read_vcl();
	}
);
has debug => ( 
	is => "rw", 
	isa => "Int", 
	default => 0 
);
has hosts => ( 
	is => "rw", 
	isa => "HashRef", 
	default => sub { 
		return {}; 
	} 
);
has host => ( 
	is => "rw", 
	isa => "Str", 
	trigger => sub {
		my ( $self, $host ) = @_;
		$self->_add_host( $host ) if ( $host ) ;
	}
);
has hostname => ( 
	is => "rw", 
	isa => "Str", 
	default => hostname 
);
has workers => ( 
	is => "rw", 
	isa => "Int", 
	default => 10 
);

=constant
=cut
use constant TAG_FASTLY => '5NzYW6HIKNZhcSUjVHUzWP';
use constant ALLOWED_DOMAINS => '(\w+).wikia.com';
use constant ALLOWED_SUBDOMAINS =>  '[^(images|liftium)]';
use constant NOT_ALLOWED_URLS => '\/__spotlights\/|\/__varnish\/|\/__varnish_liftium\/|\/beacon\?|\/__am\/';

=methods
=cut

sub _read_vcl {
	my $self = shift;
	
	open( my $fh, $self->vcl_path ) || say "Cannot open VCL file: " . $self->vcl_path;
	my $vcl = ''; while( <$fh> ) { chomp; $vcl .= $_ . "\n"; } close( $fh );
	my %hosts = ();
	while ( $vcl =~ m/backend\s+?(ap_\w+)\s*?\{(((\s*)(\n?)(.*)(\;\n))*)\}/g ) {
		my ( $host, $details ) = ( $1, $2 );
		say "Found '$&'  ( $host, $details )" if $self->debug;
		if ( $details =~ m/\.?host\s*\=\s*(\'|\")?(((\d){1,3}\.){3}(\d){1,3})(\'|\")?\;/g ) {
			$hosts{ $host } = $2;
		}
	}
	close ( $fh );
	$self->hosts( \%hosts );
}

sub _add_host {
	my ( $self, $host ) = @_;
	$self->hosts->{ $host } = $host;
}

sub _call_url {
	my ( $self, $hostname, $url ) = @_;
	my $ok = 0;
	say "Found " . scalar keys %{ $self->hosts } if $self->debug;

	if( scalar keys %{ $self->hosts } ) {
		# random slave if exists
		my $host_ip = $self->hosts->{ @{ [ shuffle( keys %{ $self->hosts } ) ] }[ 0 ] };
		my $wikia_url = sprintf("http://%s%s", $hostname, $url);
		
		say "Call $wikia_url with proxy: $host_ip";
	    
	    my $lwp = LWP::UserAgent->new();
	    say "Set proxy: $host_ip" if $self->debug;
	    $lwp->proxy('http', "http://$host_ip:80/");
	    say "Run $wikia_url" if $self->debug;
		my $resp = $lwp->request( HTTP::Request->new( "GET", $wikia_url ) );
		my $content = $resp->content;		
		if ( $content =~ m/Served(\s+?)by(\s+?)([a-z\-\d]+)(\s+?)in(\s+?)([0-9\.]+)(\s+?)secs\.(\s+?)cpu\:(\s+?)([0-9\.]+)/g ) {
			say "\tServed by $3 in $6 secs, cpu: $10";
		}
		$ok = 1;
	}    
	
	return $ok;
}

sub _thread_worker {
	my ( $self, $text ) = @_;
	
	my @lines = split( /\n/, $text );
	
	my ( $url, $status, $fastly, $method, $hostname, $backend ) = ( '', 200, '', 'GET', '', '' );

	for ( my $i=0; $i<scalar @lines;$i++) {
		my $line = $lines[$i];
		if ( $line =~ m/\d+[\s*]RxURL(\s*?)c(\s+?)(.*)/g)  {
			$url = $3;
		} elsif ( $line =~ m/\d+[\s*]TxStatus(\s*?)c(\s*?)(\d+)/g)  {
			$status = $3;
		} elsif ( $line =~ m/\d+[\s*]ReqEndFastly(\s*?)c(\s*?)(\w+)/g)  {
			$fastly = $3;
		} elsif ( $line =~ m/\d+[\s*]RxRequest(\s*?)c(\s*?)(\w+)/g) {
			$method = $3;
		} elsif ( $line =~ m/\d+[\s*]RxHeader(\s*?)c(\s*?)host[\:*?][\s*](.*)/gi) {
			$hostname = $3;
		} elsif ( $line =~ m/\d+[\s*]RxHeader(\s*?)\-(\s*?)x\-served\-by\-backend[\:*?][\s*](.*)/gi) {
			$backend = $3; 
		}
	}
	
	say "Found: \n\turl:$url,\n\tstatus:$status,\n\tfastly:$fastly,\n\tmethod:$method,\n\thostname:$hostname,\n\tbackend:$backend\n" if $self->debug;
	
	if ( $status != 200 ) {
		say "Invalid response status: $status" if $self->debug;
		return 1;
	}
	
	if ( $method ne 'GET' ) {
		say "Invalid request method: $method" if $self->debug;
		return 1;
	}

	if ( $fastly ne TAG_FASTLY ) {
		say "Invalid ReqEndFastly tag value: $fastly" if $self->debug;
		return 1;
	}
	
	if ( $url =~ m/${\(NOT_ALLOWED_URLS)}/g ) {
		say "Invalid url to run: $url" if $self->debug;
		return 1;
	}
	
	if ( $backend =~ m/ap\-i(\d+)/g ) {
		say "Request from Iowa backend" if $self->debug;
		return 1;
	}
	
	if ( $hostname && ( $hostname =~ m/${\(ALLOWED_DOMAINS)}/g ) && ( $1 =~ m/${\(ALLOWED_SUBDOMAINS)}/g ) ) {
		$self->_call_url( $hostname, $url );
	}

	return 1;
}

sub run {
	my $self = shift;
	
	my $pool = Thread::Pool::Simple->new(
		min => 2,
		max => $self->workers,
		load => 4,
		do => [ sub {
			$self->_thread_worker( shift );
		} ]		
	);	

	open(my $foo, "varnishlog -o -i RxURL,ReqEndFastly,TxStatus,RxRequest,RxHeader |");
	my $text = '';
	while ( <$foo> ) {
		chomp;
		if ( $_ eq '' ) {
			$pool->add( $text );
			#print "text = $text \n";
			$text = '';
		}
		$text .= $_ . "\n";
	}
	close ( $foo );
	say "Wait until all threads finish";	
	$pool->join();
}

no Moose;
1;

package main;

use Getopt::Long;

sub usage {
	say ( "$0 --vcl-path=<vcl-file>" );
	say ( "\twhere <vcl-file> is varnish config file" );
	exit 1;
}

GetOptions( 
	"vcl-path=s"	=> \( my $vcl_path = '' ),
	"host=s" 		=> \( my $host = '' ),
	"debug"			=> \( my $debug = 0 ),
	"workers=i"		=> \( my $workers = 10 )
);

usage() unless $vcl_path || $host;

my $warmcache = Wikia::Cache::Warm->new( vcl_path => $vcl_path, host => $host, workers => $workers, debug => $debug );
$warmcache->run();
1;
