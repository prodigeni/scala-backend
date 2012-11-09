#!/usr/bin/perl

#### IMPORTS ####

use warnings;
use strict;
use Switch;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Data::Dumper;
use JSON::XS;
use URI;
use URI::Escape;
use LWP::UserAgent;
use LWP::ConnCache;
use IPC::Open2;
use Encode;
use List::MoreUtils qw(uniq);
use Getopt::Long;
use File::Copy;
use URI::Escape::XS;
use HTML::Strip;
use HTML::Entities;
use Time::HiRes qw(time);
use UNIVERSAL 'isa';
use Wikia::Utils;
use WebService::Solr;
use constant FAIL_CATEGORY  => 'search_failure';
use constant RETRY_CATEGORY => 'search_retry';

#### GLOBALS ####

our ($WIKI_UA, $SOLR_UA);
our $DEBUG   = 0;
our $DRY_RUN = 0;
our $MAX_UPDATE_ROWS = 10000;
our $PAGES_PER_REQUEST = 10;
our $SOLR_VERSION = '3.6';
our @MASTERS;
our $POSTFILE = "/opt/apache-solr/example/exampledocs/post.jar";
our %REDIRECTS_BY_HOST;
our %HOSTS_TO_WIDS;
our $XML_FOLDER = "/var/spool/solr";
our $HTML_STRIPPER = HTML::Strip->new( emit_spaces => 1 );
our $TYPE = "bulk";
our %SOLR_SERVICES;
our %HOST_TO_ID;
our %HOST_MISSING_PAGES;
our %DELETEFILES;
our %ADDFILES;

our @SUPPORTED_LANGUAGES = ('ar', 'bg', 'ca', 'cz', 'da', 'de', 'el', 
				'en', 'es', 'eu', 'fa', 'fi', 'fr', 'ga', 
				'gl', 'hi', 'hu', 'hy', 'id', 'it', 'ja', 
				'ko', 'lv', 'nl', 'no', 'pl', 'pt', 'ro', 
				'ru', 'sv', 'sv', 'th', 'tr', 'zh'
	);

our @MULTIVALUED_LANGUAGE_FIELDS = ('redirect_titles', 'categories');
our @BASIC_LANGUAGE_FIELDS = ('title', 'html', 'wikititle');
our @UNSTORED_LANGUAGE_FIELDS = ('first500', 'beginningText');
our @MULTIVALUED_UNSTORED_LANGUAGE_FIELDS;

#### BINMODE FORMATTING ####

binmode STDIN, ":utf8";
binmode STDOUT, ":utf8";
binmode STDERR, ":utf8";
binmode $DB::OUT, ':utf8' if $DB::OUT;


#### PRECONFIGURATION ####

GetOptions('help|h'	=> sub { help(); exit() },
		   'dry-run|d' => \$DRY_RUN,
		   'verbose|v' => \$DEBUG,
		   'master|m=s' => \@MASTERS,
		   'pages-per-request|n=i' => \$PAGES_PER_REQUEST,
		   'max-update-rows|r=i' => \$MAX_UPDATE_ROWS,
		   'postfile|p=s' => \$POSTFILE,
		   'solr-xml-folder|s=s' => \$XML_FOLDER,
		   'solr-version|a=s' => \$SOLR_VERSION,
);

if ($ARGV[0] =~ /event/) {
	$TYPE="event";
}

our $FILE_ID = "$$.".int(time).".$TYPE";

print "Writing out to file identifier $FILE_ID\n";

if (@MASTERS) {

	for (my $i = 0; $i < scalar(@MASTERS); $i++) 
	{

		$MASTERS[$i] .= ':8983' unless $MASTERS[$i] =~ /:/;

		print "($$) Using master_$i ".$MASTERS[$i]."\n";

	}
} else {
	@MASTERS = ('dev-search:8983');
	$SOLR_VERSION = '3.6';
}

unless(-d '/tmp/workerfiles') { mkdir '/tmp/workerfiles' or die "Couldn't create worker folder in /tmp"; }
unless(-d $XML_FOLDER ) { mkdir $XML_FOLDER or die "Couldn't create XML folder in $XML_FOLDER"; }

foreach (@MASTERS) { 
	$SOLR_SERVICES{$_} = WebService::Solr->new("http://".$_."/solr"); 

	my $addfile;

	open $addfile, "+>:utf8", "/tmp/workerfiles/adds.$_.$FILE_ID.xml";

	print $addfile "<add>";

	$ADDFILES{$_} = $addfile;

}

#### MAIN SCRIPT LOGIC ####

my $start_time = time;

if ($DRY_RUN) {
	print "== DRY RUN ==\n";
	print "(no action will be taken)\n";
}

init_user_agents();

my $input = $ARGV[0];
unless ($input) {
	print "You must specify an input file\n";
	exit;
}

my $eventsSeen = read_events($input);
if ($eventsSeen == 0) {
	print STDERR "($$) Empty file '$input', exiting\n";
	unlink($input);
	exit(0);
}

foreach (keys %HOST_TO_ID) {

	my $host = $_;

	my $hostTime = time;

	my $remainder = scalar(@{$HOST_TO_ID{$host}});

	my $count = $remainder;

	while ($remainder > 0) {

		my @slice = splice(@{$HOST_TO_ID{$host}}, 0, $PAGES_PER_REQUEST);

		$remainder = scalar(@slice);

		eval { populate_docs($host, @slice); };

		if (my $err = $@) {

			print STDERR  "There was an error: $err\n";

		}

		$remainder = scalar(@{$HOST_TO_ID{$host}});

	}

	print "($$) $count document(s) for $host\n" if $DEBUG;

	foreach (@{$HOST_MISSING_PAGES{$host}}) {
	
		my $key = $_;

		delete_page($key, $host);
	
	}

}

print "Sending files...\n" if $DEBUG;

foreach (@MASTERS) {

	my $master = $_;

	my $delfile = $DELETEFILES{$master};
	my $addfile = $ADDFILES{$master};

	print $delfile "</query></delete>" if defined($delfile);
	print $addfile "</add>";

	close $addfile;
	close $delfile if defined($delfile);

	move "/tmp/workerfiles/deletes.$master.$FILE_ID.xml", "$XML_FOLDER/deletes.$master.$FILE_ID.xml";
	move "/tmp/workerfiles/adds.$master.$FILE_ID.xml", "$XML_FOLDER/adds.$master.$FILE_ID.xml";

}

unlink($input);

my $file = $input;
$file =~ s!^.+/!!;
print "($$) ".$eventsSeen." events processed in ".(time-$start_time)." seconds ($file)\n";


###############################################################################


## handle document querying in batches through the api
sub populate_docs {

	my ($host, @page_ids) = @_;

	$host =~ s/^preview\.//g;

	return unless scalar(@page_ids);
	my $time = time;
	my $req = HTTP::Request->new(GET => "http://$host/wikia.php");
	$req->url->query_form('controller'=>'WikiaSearch', 'method'=>'getPages', 'ids'=>join('|',@page_ids), 'format'=>'json');

	$req->header('Authenticate' => '');
	$req->header('Cookie' => 'wikicities-wikia-colocation=iowa');

	my $res = $WIKI_UA->request($req);
	return (0) unless $res->is_success;
	return (0) if $res->content eq '';
	print "($$) Time to query host (in seconds): ".(time-$time)."\n" if $DEBUG;
	my $data;

	eval { $data = decode_json($res->content) };

	if ($@) {

		print "Could not get pages for $host. Malformed JSON or missing wiki. \n" if $DEBUG;
		return 1;

	}

	push @{$HOST_MISSING_PAGES{$host}}, @{$data->{'missingPages'}} if scalar(@{$data->{'missingPages'}});

	my $count = 0;
	my $start_time = time;
	
	return 1 unless isa($data->{'pages'}, 'HASH');

	foreach (keys %{$data->{'pages'}}) {
	
		my $page = $data->{'pages'}->{$_};

		$HOSTS_TO_WIDS{$host} = $page->{'wid'};

		my $page_prepped = prepare_page($page);

		my $solr_document = WebService::Solr::Document->new(%{$page_prepped});

		# could do boosts here, whatever

		my $xml = $solr_document->to_xml();

		print {$ADDFILES{get_which_master($host)}} $xml ."\n";

		$count++;

	}

	print "($$) pages per second: " . $count/(time - $start_time) . "\n" if $DEBUG;

	return 1;
}


## splits up wiki ids based on the number of shards. allows us to scale out.
sub get_which_master {
	
	my ($host) = @_;

	my $wid = $HOSTS_TO_WIDS{$host} || 0;
	
	return @MASTERS[$wid % scalar(@MASTERS)];

}

sub init_user_agents {
	$WIKI_UA = LWP::UserAgent->new;
	$WIKI_UA->conn_cache(LWP::ConnCache->new());
	$WIKI_UA->proxy('http', 'http://varnish-s1:80');
	$SOLR_UA = LWP::UserAgent->new;
	$SOLR_UA->conn_cache(LWP::ConnCache->new());
	$SOLR_UA->parse_head(0);
	$SOLR_UA->timeout(120);
	$SOLR_UA->requests_redirectable('');
}

## Adds events to host-to-page IDs hash. Returns number of events seen. ##
sub read_events {
	my ($input) = @_;
	my %seen;

	open(INPUT, $input);
	while (<INPUT>) {
		# Skip blank lines
		next if /^\s*$/;

		chomp;

		my $line = $_;
		$line =~ s/}{/}\n{/g;
		my @events = split("\n", $line);

		foreach (@events) {

			my $event;

			eval { $event = decode_json($_) };

			if (my $err = $@) {
				print STDERR "Failed to decode '$_': $err\n";
				next;
			}

			if (!$event->{serverName}) {
				print STDERR "($$) Event has no serverName: $_\n";
				next;
			}

			if (!$event->{pageId}) {
				print STDERR "($$) Event has no pageId: $_\n";
				next;
			}

			my ($host) = $event->{serverName} =~ m!http://(.*)!;
			my $pageid = $event->{pageId};

			next if $seen{$host.$pageid};
			$seen{$host.$pageid} = 1;

			push ( @{$HOST_TO_ID{$host}}, $pageid);
		}
	}
	close(INPUT);

	return scalar keys %seen;
}

### handles formatting the data received from the server ###
sub prepare_page {

	my ($page) = @_;

	my $html = $page->{html};
	
	#strip all newlines
	if ($html){

		$html = utf8::is_utf8($html) ? $html : decode("utf8", $html);

		$html =~ s/\s+/ /g;

		$html =~ s/<span[^>]*editsection[^>]*>.*?<\/span>//g;
	

		#### @TODO: output links to the event stream ###

		$html =~ s/<img[^>]*>//g;
		$html =~ s/<\/img>//g;
	
		$html =~ s/<noscript>.*?<\/noscript>//g;

		# this one in particular sucks a lot, because you can nest divs
		$html =~ s/<div[^>]*picture-attribution[^>]*>.*?<\/div>//g;

		$html =~ s/<ol[^>]*references[^>]*>.*?<\/ol>//g;
		$html =~ s/<sup[^>]*reference[^>]*>.*?<\/sup>//g;

		$html =~ s/<script .*?<\/script>//g;
		$html =~ s/<style .*?<\/style>//g;

		my @paragraphs = $html =~ m/<p[^>]*>.*?<\/p>/g;

		# I would love to just make $html utf8 and not worry anywhere else, but HTML_STRIPPER is too ratchet
		$html = $HTML_STRIPPER->parse($html);
		$HTML_STRIPPER->eof();
		$html =~ s/\s+/ /g;

		my $str = utf8::is_utf8($html) ? $html : decode("utf8", $html);
	
		my $allParagraphText = $HTML_STRIPPER->parse(join(" ", @paragraphs));
		$HTML_STRIPPER->eof();
		$allParagraphText =~ s/\s+/ /;	

		my @paragraphWords = split(/\s+/, $allParagraphText);
		my $beginningText = join(' ', splice(@paragraphWords, 0, 100));
		my $first500 = join(' ', ($beginningText, splice(@paragraphWords, 0, 400)));

		$page->{'beginningText'} = utf8::is_utf8($beginningText) ? $beginningText : decode('utf8', $beginningText);
		$page->{'first500'} = utf8::is_utf8($first500) ? $first500 : decode('utf8', $first500);
		$page->{'html'} = utf8::is_utf8($html) ? $html : decode('utf8', $html);
		$page->{'words'} = scalar(@paragraphWords);

		if ( defined($page->{'backlink_text'}) ) {
		    #here, we are expanding the backlink instances to the appropriate size
		    #we need to 'decompress' them to adequately account for weight by frequency

		    push @MULTIVALUED_UNSTORED_LANGUAGE_FIELDS, 'backlink_text';

		    my @backlinksExpanded;

		    foreach (keys %{$page->{'backlink_text'}}) {
			my $backlink_text = $_;
			my $blt_count = $page->{'backlink_text'}{$backlink_text}; #mmm, blt

			$backlink_text = $HTML_STRIPPER->parse($backlink_text);
			$HTML_STRIPPER->eof(); #todo: make sure this doesn't kill indexing speed

			for (my $i = 0; $i < $blt_count; $i++) {
			    push @backlinksExpanded, $backlink_text;
			}

		    }

		    $page->{'backlink_text'} = @backlinksExpanded;
		}
	}

	delete $page->{'sitename'};

	$page->{'redirect_titles'} = utf8::is_utf8($page->{'redirect_titles'}) ? $page->{'redirect_titles'} : decode("utf8", $page->{'redirect_titles'});
	my @titles = split(/ *\| */, $page->{'redirect_titles'});
	$page->{'redirect_titles'} = \@titles;
	
	# some language codes have extra junk at the end for the OCD-prone
	my $lang = $page->{'lang'};
	$lang =~ s/-.*//;
	$page->{'lang'} = $lang;

	if (grep $_ eq $lang, @SUPPORTED_LANGUAGES) {
	    
	    #todo: refactor into subroutine

	    foreach (@BASIC_LANGUAGE_FIELDS) {
		my $field = $_;
		$page->{$field.'_'.$lang} = $page->{$field};
		delete $page->{$field};
	    }

	    foreach (@UNSTORED_LANGUAGE_FIELDS) {
		my $field = $_;
		$page->{$field.'_us_'.$lang} = $page->{$field};
		delete $page->{$field};
	    }

	    foreach (@MULTIVALUED_LANGUAGE_FIELDS) {
		my $field = $_;
		$page->{$field.'_mv_'.$lang} = $page->{$field};
		delete $page->{$field};
	    }

	    foreach (@MULTIVALUED_UNSTORED_LANGUAGE_FIELDS) {
		my $field = $_;
		$page->{$field.'_us_mv_'.$lang} = $page->{$field};
		delete $page->{$field};
	    }
	}

	return $page;
}


sub handle_redirects {

	my %solrTitleQueries;

	foreach (keys %REDIRECTS_BY_HOST) {

		my $host = $_;

		if ($REDIRECTS_BY_HOST{$host}) {

			push @{$solrTitleQueries{get_which_master($host)}}, "(host:$host AND pageid:(".join(' ', @{$REDIRECTS_BY_HOST{$host}}).")";

		}

	}
	
	my %solrRedirectQueries;

	foreach (keys %solrTitleQueries) {

		my $key = $_;

		my @docs = $SOLR_SERVICES{$key}->query(join(' OR ', $solrTitleQueries{$key}));

		$solrRedirectQueries{$key} = map { '(wid:'.$_->value_for('wid').' AND canonical:"'. escape_for_query($_->value_for('title')).'")' } @docs;

	}

	foreach (keys %solrRedirectQueries) {

		my $key = $_;

		my @docs = $SOLR_SERVICES{$key}->query(join(' OR ', $solrRedirectQueries{$key}));

		print ("($$) Submitting ".scalar(@docs)." redirects for review.");

		submit_events("log_search", map {[$_->value_for('host'), $_->value_for('pageid')]} @docs);

	}
}

sub delete_page {
	my ($pageid, $host) = @_;

	print "($$) Removing ${host} : ${pageid}\n";

	my $master = get_which_master($host);
	my $wid = $HOSTS_TO_WIDS{$host};

	if (! exists $DELETEFILES{$master}) {
		open my $delfile, "+>:utf8", "/tmp/workerfiles/deletes.$master.$FILE_ID.xml";
		print $delfile "<delete><query>";
		$DELETEFILES{$master} = $delfile; 
	}

	print {$DELETEFILES{$master}} '(id:' . $wid . "_$pageid) ";

	push(@{$REDIRECTS_BY_HOST{$host}}, $pageid);
}

sub submit_events {
	my ($category, $msgs) = @_;
	return unless $msgs and ref $msgs;

	my $events = '';
	foreach my $msg (@$msgs) {
		my ($host, $pageid) = @$msg;
		my %event = ( 'serverName' => "http://$host/",
					  'pageId'	 => $pageid );
		$events .= encode_json(\%event) . "\n";
	}
	
	unless ($DRY_RUN) {
		open my $scribe_cat, "|-", "/usr/bin/scribe_cat", $category
			or die "Could not run scribe_cat: $!";
		print $scribe_cat $events;
		close($scribe_cat);
	}
}

sub escape_for_query {
	my $text = shift;
	my $escape_chars = quotemeta( '+-&|!(){}[]^"~*?:\\' );
	$text =~ s{([$escape_chars])}{\\$1}g;
	return $text;
}

sub help {
	my $prog = $0;
	$prog =~ s!^.+/!!;
	
	print qq(
NAME

	$prog - Worker to index content

SYNOPSIS

	$prog [--help] [--verbose] [--dry-run] DATA_FILE

DESCRIPTION

	Worker to be run by solr-indexer.pl to do the work of fetching content and submitting it to solr for indexing.  Takes a DATA_FILE constisting of JSON formatted data.  This data is multiple wiki hosts and page IDs to index.

OPTIONS

	--dry-run
		Run this command without sending anything to solr.

	--verbose
		Print verbose output

	--help
		This help message
);
}
