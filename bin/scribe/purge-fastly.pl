#!/usr/bin/perl
use threads;
use Thread::Queue;
use LWP::UserAgent;
use LWP::ConnCache;
use HTTP::Request;
use JSON::XS;
use Time::HiRes;

my $THREADS = 15;
my $url_q = Thread::Queue->new(); 
my $start = time();
$| = 1;

sub main(){

	my $oldest;

	launch_threads($THREADS);

	while(!$oldest){
		$oldest = get_oldest('/var/spool/scribe/varnish_purges/');
		sleep 1;
		print "no oldest file sleeping\n";
	}

	while (1){
		print "opening: $oldest \n";
		$previous_oldest = $oldest;
		$oldest = process_file($oldest); 
		purges_per_second();
		while($url_q->pending() >0) {
			sleep 1;
		}
		unlink $previous_oldest;
	}
	
}
sub launch_threads(){
	my $launch_threads = shift;
	my @thread_list = ();
	for ( my $i = 0; $i <= $launch_threads ; $i++ ) {
		# Launch threads, and store references in ARRAY
		# they are currently detached so, can't be joined on
		$thread_list[$i] = threads->create(sub {
			my $cache     = LWP::ConnCache->new;
			$cache->total_capacity(50);	
			my $ua        = LWP::UserAgent->new;
			$ua->timeout(5);
			$ua->conn_cache($cache);
			# Static array of Fastly Edge nodes to set at proxies
			# this minimizes the # of connections to open
			# DO NOT CHANGE TO A LOCAL SQUID NODE
			my @proxies = ("cache-s24.hosts.fastly.net:80", 
					"varnish-s1.hosts.fastly.net:80",
					);
			$ua->proxy("http", "http://" .  $proxies[int(rand(4))]  . "/");
			my $counter=0;
			# use non-blocking queue request
			# returns undef if queue is empty
			# this only works becuase i populate the queue
			# before launching threads;
			while (my $url = $url_q->dequeue()) {
				my $request = HTTP::Request->new( PURGE => $url);
				my $response = $ua->request($request);
				# Status could be checked here, but ignoring for now
			}
		})->detach();
	print "lauched $i\n";
	}
	return 0;
	
}

sub get_oldest(){
	my $path = shift;
	opendir DIR, $path or die "Error reading $path: $!\n";
	my @files = sort grep {/purge_[0-9]+/} readdir(DIR);
	closedir DIR;
	if ($files[0]){
		return $path . $files[0];
	}else{
		return undef();
	}
}

sub process_file(){
	my $file = shift;
	
	my $junk;
	my $next_log; 
	my $pos = 0;

	while(1){
		open(my $fh, $file) || die $?;
		seek($fh, $pos, 1);
		while(<$fh>){
			if ($_ =~ /"url":/){
				my $object = decode_json($_);
				$url_q->enqueue($object->{url});
			}
			if ($_  =~ /^scribe_meta/){
				($junk, $next_log) = split(": ", $_);
			}
		}
		if($next_log) {
			last();
		}
                $pos=tell($fh);
                close($fh);
                purges_per_second();
	}
	close($fh);
	return($next_log);
}

sub purges_per_second(){
	while($url_q->pending() > 0) {
		my $process_start_time = [Time::HiRes::gettimeofday];
		my $p1 = $url_q->pending() . "\n";
		sleep 1;
		my $elapsed = Time::HiRes::tv_interval($process_start_time, [Time::HiRes::gettimeofday]);
		my $secs  = $elapsed % 60 + ($elapsed-int($elapsed));
		my $mins  = int($elapsed / 60) % 60;
		my $hours = int($elapsed / 60 / 60);
		my $p2 = $url_q->pending() . "\n";
		my $processed = $p1 - $p2;
		printf("%d purges in 0  minutes 1 seconds\n", $processed);
		printf("$prefix%d messages processed in %d hours %d minutes %.2f seconds\n", $processed, $hours, $mins, $secs);
		if ( ($start + 60) <= time() ) {
		   system("/usr/bin/gmetric -n purges_per_second -T 'Purges Per Second' -g purges -t int32 -v " . $processed);
		}
	}
}

main();
