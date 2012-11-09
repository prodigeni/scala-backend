#!/usr/bin/perl -w

use strict;

use MediaWiki::API;
use Getopt::Long;
use LWP::Simple;

use constant WIKI_NAME => 'searchtest';
use constant WIKI_HOST => WIKI_NAME().'.wikia.com';
use constant WIKI_API  => 'http://'.WIKI_HOST().'/api.php';
use constant WIKI_SEARCH => 'http://'.WIKI_HOST().'/wiki/index.php?search=';

our $USER = 'searchtest';
our $PASS = '1q2w3e4r5t';
our $VERBOSE;

GetOptions('verbose|v' => \$VERBOSE);

my $mw = MediaWiki::API->new();
$mw->{config}->{api_url} = WIKI_API();

# log in to the wiki
$mw->login({lgname     => $USER,
			lgpassword => $PASS}) or die showerr($mw);

my @ts = localtime;
$ts[5] += 1900;
$ts[4]++;

# Generate today's page name and fetch it
my $pagename = sprintf("%04d-%02d-%02d", @ts[5,4,3]);

print STDERR "Fetching page $pagename ... " if $VERBOSE;
my $ref = $mw->get_page({title => $pagename});
print STDERR "done\n" if $VERBOSE;

# Grab the content and look for the last edit made
my $text = $ref->{'*'}||'';
my @lines = split("\n", $text);
my $last_edit;

while (@lines) {
	$last_edit = pop @lines;
	last if $last_edit =~ /^Search-edit: \d+$/;
}

# If there a last edit with no result time, see if search returns results now
if ($last_edit) {
	my ($edit_time) = $last_edit =~ /^Search-edit: (\d+)$/;

	print STDERR "Searching for $edit_time ... " if $VERBOSE;
	my $result_time = search($pagename, $edit_time);

	if ($result_time) {
		print STDERR "found\n" if $VERBOSE;
		update_ganglia($edit_time, $result_time);

		print STDERR "Updating wiki with result ... " if $VERBOSE;
		$text = update_wiki_result($mw, $pagename, $text, $result_time);
		print STDERR "done\n" if $VERBOSE;
	} else {
		print STDERR "not found\n" if $VERBOSE;
		# If we have an outstanding search and no results, just exit and wait
		exit(0);
	}
}

# If we're here, we're ready to tack on a new search time
print STDERR "Updating wiki with new edit time ... " if $VERBOSE;
update_wiki_edit($mw, $pagename, $text);
print STDERR "done\n" if $VERBOSE;

################################################################################

sub showerr {
	my ($mw) = @_;
	return $mw->{error}->{code}.': '.$mw->{error}->{details};
}

sub search {
	my ($pagename, $edit_time) = @_;
	my $content = get(WIKI_SEARCH().$edit_time);

	return unless $content;
	
	if ($content =~ m!title="$pagename">$pagename</a>.+<span class="searchmatch">$edit_time!) {
		return time;
	} else {
		return;
	}
}

sub update_wiki_edit {
	my ($mw, $pagename, $text) = @_;
	$text = $text."Search-edit: ".time()."\n";

	$mw->edit({action => 'edit',
			   title  => $pagename,
			   text   => $text,
			  }) or die showerr($mw);
}

sub update_wiki_result {
	my ($mw, $pagename, $text, $result_time) = @_;
	$text = $text.' # Search-result: '.$result_time."\n\n";

	$mw->edit({action => 'edit',
			   title  => $pagename,
			   text   => $text,
			  }) or die showerr($mw);
	return $text;
}

sub update_ganglia {
	my ($edit_time, $result_time) = @_;
	my $delta = sprintf("%.1f", ($result_time - $edit_time)/60);

	system("/usr/bin/gmetric --group solr --name=search_index_latency --value=$delta --type=float --units=minutes --tmax=600");
}