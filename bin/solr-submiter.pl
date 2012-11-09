#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use LWP::UserAgent;
use HTTP::Request::Common qw(POST);
use POE::Queue::Array;
use IO::Compress::Gzip qw(gzip $GzipError);
use XML::Simple;
use Encode;
use Data::Dumper;
use Getopt::Long;

binmode STDERR, ":utf8";
binmode STDIN, ":utf8";
binmode STDOUT, ":utf8";

# some configuration
my $xml_dir = '/var/spool/solr/';
my $solr_host = 'search-s10';
my $timeout = '600';

GetOptions('xml-dir|x=s' => \$xml_dir,
	   'solr-host|h=s' => \$solr_host,
	   'timeout|t=s' => \$timeout
);

my $solr_url = "http://$solr_host:8983/solr/update";

my %stats;

my $pqa = POE::Queue::Array->new();

# read all files from xml dirs
opendir(DIR, $xml_dir) || die("Cannot open xml directory '$xml_dir': $!");
# get only .xml files and sort them by time
my @files = sort { -M "$xml_dir/$a" <=> -M "$xml_dir/$b" } (grep { /\.xml$/ } readdir(DIR));
closedir(DIR); 

# enqueue items with priorities
foreach my $file (@files) {
	my $priority = 1;
	if ($file =~ /\.bulk\.xml$/) {
		$priority = 10;
	}
	$pqa->enqueue($priority, $file);
}

#$pqa->enqueue(10, 'deletes.search-s1:8983.2004.1335164946.bulk.xml');

my $ua = LWP::UserAgent->new();
$ua->timeout($timeout);

# for stats
$stats{'files_count'} = $pqa->get_item_count();
$stats{'ok'} = 0;
$stats{'empty'} = 0;
$stats{'errors'} = 0;

my $time_start = time();

# process all files in queue
while ($pqa->get_item_count()) {
	my ($priority, $queue_id, $payload) = $pqa->dequeue_next();
	#last unless defined $priority;
	print "working on file: $xml_dir$payload, priority: $priority...\t";
	my $time_local = time();

	# read file to var
	open (FILE, "<", $xml_dir.$payload);
	my $tmp_data = do { local $/; <FILE> };
	if ($tmp_data eq "<delete><query></query></delete>") {
		print "EMPTY (".(time-$time_local)."s)\n";
		$stats{'empty'}++;
		unlink($xml_dir.$payload) or warn("error deleting file '$xml_dir$payload': $!");
		next;
	}
	my $file_contents = encode("UTF-8", $tmp_data);
	close(FILE);

	# post it to solr
	my $response = $ua->post($solr_url,Content_Type=> "text/xml; charset=utf-8",Content=>$file_contents);
	if ($response->is_success) {
		my $xml = new XML::Simple;
		my $data = $xml->XMLin($response->decoded_content);
		if ($data->{'lst'}{'int'}{'status'}{'content'} eq 0) {
			print "OK (".(time-$time_local)."s)\n";
			$stats{'ok'}++;
			unlink($xml_dir.$payload) or warn("error deleting file '$xml_dir$payload': $!");
		}
	} else {
		print "ERROR (".(time-$time_local)."s)\n";
		$stats{'errors'}++;
		print $response->error_as_HTML;
		gzip $xml_dir.$payload => $xml_dir."failed/".$payload.".gz" or warn "Gzip of file '$xml_dir$payload' failed: $GzipError\n";
		unlink($xml_dir.$payload) or warn("error deleting file '$xml_dir$payload': $!");;
	}
	#{
	#	use bytes;
	#	$stats{'files'}{$payload}{'size'} = length($file_contents);
	#}
	#$stats{'files'}{$payload}{'time'} = (time()-$time_start);
}

print "[DONE] ".$stats{'files_count'}." files (ok:".$stats{'ok'}." empty:".$stats{'empty'}." errors:".$stats{'errors'}.") in ".(time-$time_start)."s\n" if (scalar(@files) > 0);
#print Dumper(%stats);
sleep 1;
exit 0;
