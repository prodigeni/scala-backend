package Wikia::DW::Event::FileParser;

use strict;

use Capture::Tiny ':all';
use POSIX qw(strftime);
use DBI;
use DBD::mysql;
use URI::Escape;
use Wikia::DW::Common;
use Wikia::DW::Event::ParseErrorProcessor;

my $LOCAL_BASE = '/var/lib/mysql/data/NOT_MYSQL/statsdb_s3';

my %MON = ( jan => '01', feb => '02', mar => '03', apr => '04', may => '05', jun => '06',
            jul => '07', aug => '08', sep => '09', oct => '10', nov => '11', dec => '12'  );

my %EVENT_PROCESSORS = (
    ab_treatment         => [   'ABTreatmentProcessor' ],
    AdDriver             => [      'AdDriverProcessor' ],
    api                  => [           'ApiProcessor' ],
    create               => [         'EventProcessor' ],
    delete               => [         'EventProcessor' ],
    edit                 => [         'EventProcessor' ],
    embed_change         => [   'EmbedChangeProcessor' ],
    pageview             => [      'PageviewProcessor' ], #, 'ReferrerProcessor' ],
    search_click_match   => [        'SearchProcessor' ],
    search_click         => [        'SearchProcessor' ],
    search_click_wiki    => [        'SearchProcessor' ],
    search_start_gomatch => [        'SearchProcessor' ],
    search_start_google  => [        'SearchProcessor' ],
    search_start_match   => [        'SearchProcessor' ],
    search_start_nomatch => [        'SearchProcessor' ],
    search_start         => [        'SearchProcessor' ],
    search_start_suggest => [        'SearchProcessor' ],
    trackingevent        => [ 'TrackingeventProcessor', 'LightboxProcessor' ],
    undelete             => [         'EventProcessor' ],
    wikiahubs            => [     'WikiahubsProcessor' ],
    default              => [       'DefaultProcessor' ] # default
);

# NOTE: parameter names prefixed with @ signs are always transformed at time of database load
my %PARAMS = (
    a              => 'article_id',
    apiKey         => '@api_key',
    archive        => 'archive_id',
    beacon         => 'beacon',
    bot            => 'is_bot',
    caller         => 'caller',
    cat            => 'category_id',
    categoryId     => 'category_id',
    catname        => 'category_name',
    cb             => 'cache_buster',
    client_ip      => '@ip',
    clickSource    => 'click_source',
    content        => 'is_content',
    c              => 'wiki_id',
    event          => 'event',
    href           => 'href',
    imageLinks     => 'image_links',
    ip             => '@ip',
    label          => 'label',
    lc             => 'language_code',
    lid            => 'language_id',
    log            => 'log_id',
    mediaType      => 'media_type',
    n              => 'namespace_id',
    page           => 'page_title',
    pg             => 'special_page',  # This is the name of the page if it's a special page
    pos            => 'position',
    redirect       => 'is_redirect',
    rev            => 'rev_id',
    revSize        => 'rev_size',
    rev_ts         => 'rev_timestamp',
    r              => 'referrer',
    server         => 'server',
    sterm          => 'search_term',
    stype          => 'search_type',
    title          => 'title',
    totalWords     => 'total_words',
    treatmentGroup => 'treatment_group_id',
    type           => 'event_type',  # Note: should be same as _EVENT_TYPE, but for some event sources isn't
    url            => 'url',
    utma           => '@visitor_id',
    utmb           => '@visit_id',
    u              => 'user_id',
    varnishTime    => 'varnish_time',
    videoLinks     => 'video_links',
    x              => 'dbname',
    y              => 'dbcluster',
# Internal Use
    _BEACON        => '_BEACON',
    _EVENT_ID      => '_EVENT_ID',
    _EVENT_TS      => '_EVENT_TS',
    _EVENT_TYPE    => '_EVENT_TYPE',
    _YEAR          => '_YEAR',
    _MONTH         => '_MONTH',
    _DAY           => '_DAY',
    _DATE_ID       => '_DATE_ID',
    _TIME          => '_TIME',
    _PSTR          => '_PSTR',
    _LINE          => '_LINE',
    _FILE_ID       => '_FILE_ID'
);


##
sub new {
    my $class = shift;
    my (%params) = @_; 
    my $self = bless {}, ref $class || $class;
    Wikia::DW::Common::log("Initialized $class");
    return $self;
}

sub run {
    my $self = shift;

    my (%params) = @_; 
    $self->{s3file} = $params{s3file};

    # (api|event|special|view)/YYYY/MM/DD/(api|event|special|view).log-HH-MI.xxxxxxxxxx.gz
    ( $self->{source},
      $self->{year},
      $self->{month},
      $self->{day},
      $self->{hour},
      $self->{minute},
      $self->{file_id} ) = $self->{s3file} =~ m!(api|event|special|view)/(\d\d\d\d)/(\d\d)/(\d\d)/(?:api|event|special|view)\.log-(\d\d)-(\d\d)\.(\d+)\.gz!;

    $self->{filename}    = "$self->{source}.log-$self->{hour}-$self->{minute}.$self->{file_id}.gz";

    $self->{localfile_path} = "$LOCAL_BASE/$self->{source}/$self->{year}/$self->{month}/$self->{day}";

    my $rc = system("mkdir -p $self->{localfile_path}");
    die "Couldn't create directory: $self->{localfile_path}\n" if ($rc != 0);

    $self->{localfile}   = "$self->{localfile_path}/$self->{filename}";

    ($self->{localbase}) = $self->{localfile} =~ m/(.+)\.gz/;

    $self->{pkfile}      = "$self->{localbase}.pk";

    $self->{unmapped_params} = {};

    Wikia::DW::Common::log("  s3file ------> $self->{s3file}");
    Wikia::DW::Common::log("  localfile ---> $self->{localfile}");

    if ($self->rsync_file) {
        $self->add_pk;
        $self->parse;
        $self->cleanup;
        $self->log_completion;
    }

    Wikia::DW::Common::log('Done.');
}

sub rsync_file {
    my $self = shift;
    Wikia::DW::Common::log('  syncing file from mq3 local');

#    return 1 if (-e $self->{localfile}); # No need to re-copy the file

    my ($stdout, $stderr, $result) = capture {
        scalar system("rsync -aq --rsh 'ssh -i /home/analytics/.ssh/id_rsa.etl' etl\@mq3:/var/log/track/$self->{s3file} $self->{localfile}");
    };

    if ($result > 0) {
        Wikia::DW::Common::log('    failed');
        Wikia::DW::Common::log('  syncing file from mq3 S3 mount');
        ($stdout, $stderr, $result) = capture {
            scalar system("s3cmd get s3://raw-track-data/$self->{s3file} $self->{localfile}");
        };
        if ($result > 0) {
            Wikia::DW::Common::log('    failed');
            return 0;
        }
    }
    return 1;
}

sub add_pk {
    my $self = shift;
    Wikia::DW::Common::log("  adding primary key");
    system("gunzip -c $self->{localfile} | awk '{ print NR\",\"\$0 }' > $self->{pkfile}");
}

sub parse {
    my $self = shift;
    Wikia::DW::Common::log("  parsing");

    # Setup default Processor for handling parsing errors
    my $parse_error = Wikia::DW::Event::ParseErrorProcessor->new;
    $parse_error->init( { fileparser => $self } );

    $self->{processors} = {
        ParseErrorProcessor => $parse_error
    };

    # Create hash of processors by event_type
    $self->{event_processors} = {};

    # Loop over event file
    open EVENT_FILE, $self->{pkfile} || die "Couldn't open pkfile: $!";
        $self->process_line($_) while (<EVENT_FILE>);
    close EVENT_FILE;

    # Finalize all processors
    $self->finalize_processors;
}

sub process_line {
    my $self = shift;
    my ($line) = @_;
    chomp($line);

    my %event = (
        _FILE_ID => $self->{file_id},
        _LINE    => $line
    );

    ( $event{_EVENT_ID},
      $event{_MONTH},
      $event{_DAY},
      $event{_TIME} ) = $line =~ m!^(\d+),(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d+)\s+(\d+:\d+:\d+)!x;

    # Get localtime, so that we can determine the current year; do some hackery if we seem to be processing around the New Year
    my @dt = localtime();
    $event{_YEAR} = $dt[5] + 1900;

    # If it's Q1 and we're processing data for Q4, presume it's the previous year's data
    $event{_YEAR}-- if ($event{_MONTH} eq 'Dec' || $event{_MONTH} eq 'Nov' || $event{_MONTH} eq 'Oct') && $dt[4] <= 2;

    # Parse by custom regex depending on source (view, event, api, or special)
    if ($self->{source} eq 'view') {
        ( $event{beacon},
          $event{_PSTR} ) = $line =~ m!.+BEACON:\s+(\S+)\s+/__(?:track|onedot)(?:/view)?\?(.+)$!;
        $event{_EVENT_TYPE} = 'pageview';

    } elsif ($self->{source} eq 'event') {
        ( $event{_EVENT_TYPE},
          $event{_PSTR} ) = $line =~ m!.+/__track/event/([^\?]+)\?(.+)$!x;

    } elsif ($self->{source} eq 'api') {
        if ($line =~ m!.+/__track/api/([^\?]+)\?(.+)$!) {
            $event{request_method} = $1;
            $event{_PSTR} = $2;
            $event{_EVENT_TYPE} = 'api';

            $event{_PSTR} =~ s/apiKey=\(null\)/apiKey=\\N/;

            if ($event{_PSTR} =~ m!&url=(.*?)(&|$)!i) {
                my $url = uri_unescape($1);
                my $action;
                if ($url =~ /[?&]action=(.+?)(&|$)/i) {
                    $action = $1;
                } elsif ($url =~ /[?&](artist|song)=(.+?)(&|$)/i) {
                    $action = "lyrics"; # Special case: if they have music parameters, we automatically assume action=lyrics (backward compatibility for LyricWiki API calls that predate the MW API).
                } elsif ($url =~ /^\/api.php$/i) {
                    $action = "[documentation]"; # request w/no params just returns documentation
                } else {
                    $action = "[invalid]";
                }

                # Event type (the unique name for the function call)
                if ($action =~ /^lyrics$/i) {
                    if ($url =~ /[?&]func=(.+?)(&|$)/i) {
                        $event{api_function} = $1;
                    } elsif ($url =~ /[?&]song=(.+?)(&|$)/i) {
                        $event{api_function} = "getSong";
                    } elsif ($url =~ /[?&]artist=(.+?)(&|$)/i) {
                        $event{api_function} = "getArtist";
                    } else {
                        $event{api_function} = "[invalid]";
                    }

                } elsif ($action =~ /^query$/i) {
                    # Note: multiple queries can be combined into one API request, so we alphabetize things to combine duplicates.
                    my @queryEvents = ();
                    while ($url =~ m/[?&](prop|list|meta|titles)=([^&]+)/ig) {
                        if ($1 eq "titles") { # titles is a special case.
                            push @queryEvents, "query:$1";
                        } else {
                            push @queryEvents, "query:$1:" . join('|', sort(split(/\|/, $2)));
                        }
                    }

                    if (scalar(@queryEvents) > 0) {
                        $event{api_function} = join("|**|", @queryEvents);
                    } else {
                        $event{api_function} = "[invalid]";
                    }
                } else {
                    $event{api_function} = $action;
                }

                # api_type (which API {LW, Core MW, Nirvana, Wikia Extensions to MW, Unknown})
                my $API_TYPE_LYRICWIKI = "LyricWiki";
                my $API_TYPE_NIRVANA   = "Nirvana";
                my $API_TYPE_CORE_MW   = "Core MediaWiki";
                my $API_TYPE_WIKIA_MW  = "Wikia Extensions to MediaWiki";
                my $API_TYPE_UNKNOWN   = "Unknown";

                if ($action =~ /^lyrics$/i) {
                    $event{api_type} = $API_TYPE_LYRICWIKI;
                } elsif ($action =~ /^nirvana$/i) {
                    $event{api_type} = $API_TYPE_NIRVANA;
                } elsif ($event{api_function} =~ /:wk/i) { # many Wikia query extensions start with "wk"
                    $event{api_type} = $API_TYPE_WIKIA_MW;
                } elsif($event{api_function} =~ /^imageserving$/){
					$event{api_type} = $API_TYPE_WIKIA_MW;
                } elsif($event{api_function} =~ /^opensearch$/){
					$event{api_type} = $API_TYPE_CORE_MW;
				} else {
					# TODO: Identify remaining MediaWiki and Wikia extensions by event_type
					$event{api_type} = $API_TYPE_UNKNOWN;
                }

                # TODO: Start to roll out API keys to all of our internal uses of the API.
                # called_by_wikia - (1 = yes, 0 = no, -1 = Unknown) - whether it was Wikia code using the API or not
                $event{called_by_wikia} = -1;
              }
          }
    } else { # special
        if ($line =~ m/special\?/) {
            ( $event{beacon},
              $event{_PSTR} ) = $line =~ m!.+BEACON:\s+(\S+)\s+/__track/special\?(.+)$!x;
            ($event{_EVENT_TYPE}) = $line =~ m!type=([^&]+)!x;
        } else { # m!special/<event_type>\?!
            ( $event{beacon},
              $event{_EVENT_TYPE},
              $event{_PSTR} ) = $line =~ m!.+BEACON:\s+(\S+)\s+/__track/special/([^\?]+)\?(.+)$!x;
        }
    } # End of regex parsing by source

    # Log as parse error, if any of these are missing
    if (!( defined $event{_EVENT_ID}   &&
           defined $event{_YEAR}       &&
           defined $event{_MONTH}      &&
           defined $event{_DAY}        &&
           defined $event{_TIME}       &&
           defined $event{_EVENT_TYPE} &&
           defined $event{_PSTR} )) {
        $self->{processors}->{ParseErrorProcessor}->process_event(\%event);
        return;
    }

    $event{_MONTH_ID} = $MON{lc($event{_MONTH})};
    $event{_DATE_ID}  = sprintf("%04d%02d%02d",   $event{_YEAR}, $event{_MONTH_ID}, $event{_DAY});
    $event{_DATE}     = sprintf("%04d-%02d-%02d", $event{_YEAR}, $event{_MONTH_ID}, $event{_DAY});
    $event{_EVENT_TS} = "$event{_DATE} $event{_TIME}";

    $event{_PSTR} =~ s/&amp;/&/g;

    # Split _PSTR to extract event attributes (params)
    foreach (split('&', $event{_PSTR})) {
        my ($k,$v) = split('=');
        $v = defined $v ? $v : '';
        if (defined $k && exists $PARAMS{$k}) {
            $event{$PARAMS{$k}} = $v
        } elsif (defined $k) {
            if (!exists $self->{unmapped_params}->{$k}) {
                Wikia::DW::Common::debug("  unmapped param: '$k'");
                $self->{unmapped_params}->{$k}++;
            }
            $event{$k} = $v
        }
    }

    $event{beacon} = '----------' unless (exists $event{beacon} && $event{beacon} ne '');

    $self->initialize_processors($event{_EVENT_TYPE}) unless exists $self->{event_processors}->{$event{_EVENT_TYPE}};

    # Send event to each of the processors for the particular event type
    foreach my $processor (@{ $self->{event_processors}->{$event{_EVENT_TYPE}} }) {
        $processor->process_event(\%event);
    }
}


sub cleanup {
    my $self = shift;
    Wikia::DW::Common::log('  cleaning up');
    unlink($self->{localfile}) || die "Couldn't remove localfile: $!";
    unlink($self->{pkfile})    || die "Couldn't remove pkfile: $!";
}

sub log_completion {
    my $self = shift;

    my $statsdb_dbh = Wikia::DW::Common::statsdb;

    $statsdb_dbh->do(
        "INSERT INTO statsdb_etl.etl_files (
            source,
            file_id,
            s3_file,
            loaded_at
        ) VALUES (
            '$self->{source}',
            $self->{file_id},
            '$self->{source}/$self->{year}/$self->{month}/$self->{day}/$self->{filename}',
            now()
        ) ON DUPLICATE KEY UPDATE loaded_at = now()"
    );

    $statsdb_dbh->do('COMMIT');

    $statsdb_dbh->disconnect;
}

sub initialize_processors {
    my ($self, $event_type) = @_;
    Wikia::DW::Common::log("  adding processors for event_type: $event_type");

    $self->{event_processors}->{$event_type} = [];

    foreach my $processor (@{ exists $EVENT_PROCESSORS{$event_type} ? $EVENT_PROCESSORS{$event_type} : $EVENT_PROCESSORS{default} }) {
        if (!exists $self->{processors}->{$processor}) {
            Wikia::DW::Common::log("    - $processor");
            my $pkg = "Wikia::DW::Event::$processor";
            eval "use $pkg";
            my $new_processor = $pkg->new;
            $new_processor->init( { fileparser => $self } );
            $self->{processors}->{$processor} = $new_processor
            
        }
        push @{ $self->{event_processors}->{$event_type} }, $self->{processors}->{$processor};
    }
}

sub finalize_processors {
    my $self = shift;

    Wikia::DW::Common::log('  finalizing processors');

    # Finalize the ParseErrorProcessor first
    $self->{processors}->{ParseErrorProcessor}->finalize;
    delete $self->{processors}->{ParseErrorProcessor};

    foreach my $processor (sort keys %{ $self->{processors} }) {
        $self->{processors}->{$processor}->finalize;
        delete $self->{processors}->{$processor};
    }
}

1;
