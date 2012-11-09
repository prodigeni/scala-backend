# imports
use WebService::Solr;

# globals
# lifted from solr-worker.pl (TODO: make this an attribute of a module)
our @SUPPORTED_LANGUAGES = ('ar', 'bg', 'ca', 'cz', 'da', 'de', 'el', 
				'en', 'es', 'eu', 'fa', 'fi', 'fr', 'ga', 
				'gl', 'hi', 'hu', 'hy', 'id', 'it', 'ja', 
				'ko', 'lv', 'nl', 'no', 'pl', 'pt', 'ro', 
				'ru', 'sv', 'sv', 'th', 'tr', 'zh'
	);

push @SUPPORTED_LANGUAGES, 'default'; # support for our default treatment

our $service = WebService::Solr->new("http://localhost:8983/solr"); 

our %options = ('spellcheck' => 'true',
				'q' => 'foo',
				'spellcheck.build' => 'true',
				'spellcheck.dictionary' => ''
	);

#logic
foreach (@SUPPORTED_LANGUAGES) {

    my $languageCode = $_;

    %options->{'spellcheck.dictionary'} = $languageCode;

    print "Building dictionary for $languageCode...";
    my $time = time();
    
    my $result = $service->search('foo', \%options);

    $time = time() - $time;
    $minutes = sprintf('%d', $time/60);
    $seconds = $time % 60;
    print "COMPLETE! ( ${minutes}m ${seconds}s )\n";

}

exit();
