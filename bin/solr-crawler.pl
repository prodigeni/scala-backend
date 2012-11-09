#!/usr/bin/perl
#
# Send a list of all known pages of a wiki to the search indexer given the wiki's URL.
#
# Can be run with withcity to iterate through all wiki's via
#
#   /usr/wikia/backend/bin/withcity 'system("perl solr-crawler.pl $url")'
#
# Run "solr-crawler.pl -h" for full help
#

use strict;
use warnings;

use LWP::UserAgent;
use Data::Dumper;
use JSON::XS;
use JSON::DWIW;
use URI::Escape;
use Getopt::Long;

use constant API_MAX_ROWS  => 500;
use constant SOLR_MAX_ROWS => 10_000;
use constant SOLR_BASE_URL => 'http://10.8.42.26:8983/solr/select';

our $SOLR_URL;
our $MASTER;

our ($FULL, $VERBOSE);
GetOptions('full|f'     => \$FULL,
           'verbose|v'  => \$VERBOSE,
           'master|m=s' => \$MASTER,
           'help|h'     => sub { help(); exit }
          );

my ($url) = @ARGV;

if ($MASTER) {
	$MASTER .= ':8983' unless $MASTER =~ /:/;
	$SOLR_URL = "http://$MASTER/solr/select";
	print STDERR "Using master $MASTER\n";
} else {
	$SOLR_URL = SOLR_BASE_URL();
}

binmode STDIN, ":utf8";
binmode STDOUT, ":utf8";
binmode STDERR, ":utf8";
$|=1;

# Remove any trailing slash from the URL
$url =~ s!/$!!;
$url = 'http://'.$url unless $url =~ m!^http://!;

print "Processing: $url\n";

my $namespaces = get_namespaces($url);
die "Could not find any namespaces for '$url'\n" unless $namespaces;

my %page_ids;
fetch_from_wiki($url, $namespaces, \%page_ids);
die "No events found for '$url'\n" unless scalar keys %page_ids;

# If we got the --full flag, also get any URLs solr knows about
if ($FULL) {
	my $cur_seen = scalar keys %page_ids;
	fetch_from_solr($url, \%page_ids);
	my $new_seen = scalar keys %page_ids;
	
	print "-- Found ".($new_seen - $cur_seen)." additional pages in the solr index\n"
	    if $VERBOSE;
}

submit_events($url, \%page_ids);

################################################################################

sub get_namespaces {
	my ($wiki_url) = @_;

	print "-- Requesting namespaces ... " if $VERBOSE;
	my $res = request("$wiki_url/api.php",
					  action => 'query',
					  format => 'json',
					  meta   => 'siteinfo',
					  siprop => 'namespaces',
					 );
	print "done\n" if $VERBOSE;

	my ($docs) = get_query_field($res, 'namespaces');

	return $docs ? [ keys %$docs ] : undef;
}

sub fetch_from_wiki {
	my ($wiki_url, $namespaces, $page_ids) = @_;
	my $events = '';

	print "-- Fetching pages:\n" if $VERBOSE;
	foreach my $ns (@$namespaces) {
		print "\tFrom NS $ns .." if $VERBOSE;
        
		my $rows = API_MAX_ROWS();
		my $size = 0;
		my $start_from = "";
		do {
			print "." if $VERBOSE;

			my $res  = request("$wiki_url/api.php",
							   action      => 'query',
							   format      => 'json',
							   aplimit     => $rows,
							   list        => 'allpages',
							   apfrom      => $start_from,
							   apnamespace => $ns,
							  );            
			my ($docs, $qf) = get_query_field($res, 'allpages');
            
			if ($docs) {
				$size = @$docs;
				foreach my $d (@$docs) {
					$page_ids->{$d->{pageid}} = 1;
				}

				$start_from = $qf->{"apfrom"};
			} else {
				$size = 0;
				print " No results in namespace -" if $VERBOSE;
			}

		} while ($start_from && $size == $rows );

		print " done\n" if $VERBOSE;
	}
}

sub fetch_from_solr {
	my ($wiki_url, $page_ids) = @_;
	my $events = '';
	my $rows   = SOLR_MAX_ROWS();
	my $offset = 0;
	my $size   = 0;

	# Just get the host part
	my $host = $wiki_url;
	$host =~ s!^http://|/$!!g;

	print "-- Fetching indexed pages " if $VERBOSE;

	do {
		print "." if $VERBOSE;
		my $res = request(SOLR_BASE_URL(),
						  q     => 'host:'.$host,
						  fl    => 'pageid',
						  wt    => 'json',
						  rows  => $rows,
						  start => $offset
						 );

		my ($j, $docs);       
		$j = JSON::DWIW::deserialize($res->content) if $res->content;
		$docs = $j->{'response'}->{'docs'} if $j;

		if ($docs) {
			$size = @$docs;
			$offset = $offset + $rows;
			foreach my $d (@$docs) {
				$page_ids->{$d->{pageid}} = 1;
			}
		}
	} while ( $size == $rows );

	print " done\n" if $VERBOSE;
}

sub get_query_field {
	my ($res, $field) = @_;
	return unless $res->content;

	my $j = decode_json($res->content);
	return unless $j;

	return ($j->{'query'}->{$field}, $j->{'query-continue'}->{$field});
}

sub request {
	my ($url, %params) = @_;

	my $ua = LWP::UserAgent->new;
	$ua->parse_head(0);
	$ua->timeout(10);
	$ua->requests_redirectable('');
	$ua->env_proxy();

	my $req = HTTP::Request->new(GET => $url);
	$req->url->query_form(%params);

	my $res = $ua->request($req);

	die "Bad response from $url: ".$res->status_line unless $res->is_success;

	return $res;
}

sub submit_events {
	my ($wiki_url, $page_ids) = @_;

	print "-- Submitting pages for reindex ... " if $VERBOSE;
    
	open my $pipe, "|-", "/usr/bin/scribe_cat", "search_bulk"
		or die "Could not run scribe_cat: $!";

	my $count = 0;
	my $events = '';
	foreach my $id (keys %$page_ids) {
		$count++;

		$events .= encode_json({serverName => $wiki_url,
								pageId     => $id}) . "\n";

		# Send messages out every 1,000 IDs
		if ($count > 1_000) {
			print $pipe $events;
			$count = 0;
			$events = '';
		}
	}

	# Finish off the remaining events
	chomp($events);
	print $pipe $events;

	close($pipe);

	print "done\n" if $VERBOSE;
}

sub help {
	my ($prog) = $0 =~ m!([^/]+)$!;

	print qq(
NAME

	$prog - Send a list of all known pages of a wiki to the search indexer given the wiki's URL.

SYNOPSIS

	$prog [--full] [--help] URL

DESCRIPTION

	Given a URL, query the API to find all page IDs for that wiki and send them via scribe to the solr indexer to force a reindex of those pages.

OPTIONS

	--full, -f
		Contact the solar indexer for a full list of URLs it has in the index.  This is useful when it is suspected that there are deleted pages in the index.  If there is a deleted page in the index this will send it back to it to reindex, whereupon it will encounter a 404 and drop the URL from the index.

	--help, -h
		This help message

);
}
