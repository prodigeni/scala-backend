package Wikia::DW::EC2::LogSorter;

use strict;

use Capture::Tiny ':all';
use POSIX qw(strftime);
use DateTime;
use DBI;
use DBD::mysql;
use URI::Escape;
use Wikia::DW::Common;

my $S3_BASE    = 's3://raw-track-data';
my $LOCAL_BASE = '/ebs/raw-track-data';

my %MON = ( jan => '01', feb => '02', mar => '03', apr => '04', may => '05', jun => '06',
            jul => '07', aug => '08', sep => '09', oct => '10', nov => '11', dec => '12'  );

sub new {
    my $class = shift;
    my (%params) = @_; 
    my $self = bless {}, ref $class || $class;
    $self->{logfiles} = {};
    Wikia::DW::Common::log("Initialized $class");
    return $self;
}

sub process {
    my $self = shift;

    my (%params) = @_; 
    $self->{source}  = $params{source};
    $self->{file_id} = $params{file_id};

    # Find the date represented by the epoch
    $self->{file_dt}   = DateTime->from_epoch( epoch => $self->{file_id} );

    # <source>/YYYY/MM/DD
    $self->{file_dir}  = sprintf( '%s/%02d/%02d/%02d/',
                                  $self->{source},
                                  $self->{file_dt}->year,
                                  $self->{file_dt}->month,
                                  $self->{file_dt}->day );

    $self->{s3_file_dir}    = "$S3_BASE/$self->{file_dir}";
    $self->{local_file_dir} = "$LOCAL_BASE/$self->{file_dir}";

    if (! -e $self->{local_file_dir}) {
      system("mkdir -p $self->{local_file_dir}") || die "Couldn't create local dir: $self->{local_file_dir}\n";
    }

    # <source>.log-<HH>-<MM>.<epoch>.gz
    $self->{file_name} = sprintf( '%s.log-%02d-%02d.%i.gz',
                                  $self->{source},
                                  $self->{file_dt}->hour,
                                  $self->{file_dt}->minute,
                                  $self->{file_id} );

    # <source>/YYYY/MM/DD/<source>.log-<HH>-<MM>.<epoch>.gz
    $self->{file_path}  = "$self->{file_dir}/$self->{file_name}";

    $self->{s3_file_path}    = "$self->{s3_dir}/$self->{file_name}";

    $self->{local_file_path} = "$self->{local_dir}/$self->{file_name}";

    my ($stdout, $stderr, $result) = capture {
        my $s3cmd = "s3cmd --force --no-progress get $self->{s3_file_path} $self->{local_file_path}";
        Wikia::DW::Common::log("  s3cmd get");
        scalar system($s3cmd);
    };

    die "FAILED: s3cmd get\n" if ($stderr); # TODO: something better here

    Wikia::DW::Common::log("  sorting");

    # Loop over event file
    open EVENT_FILE, "zcat $self->{local_file_path} |" || die "Couldn't zcat local file: $!";
        $self->process_line($_) while (<EVENT_FILE>);
    close EVENT_FILE;

    # Finalize all processors
    $self->finalize_processors;

    Wikia::DW::Common::log('Done.');
}

sub process_line {
    my $self = shift;
    my $line = shift;
    chomp($line);

    my %event = (
        _FILE_ID => $self->{file_id}
    );

    ( $event{_MONTH},
      $event{_DAY},
      $event{_TIME},
      $event{_LINE} ) = $line =~ m!^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d+)\s+(\d\d:\d\d:\d\d)\s+(.+)!x;

    # Throw away the event, if any of these are missing
    if (!( defined $event{_MONTH} &&
           defined $event{_DAY}   &&
           defined $event{_TIME} )) {
        return;
    }

    $event{_YEAR} = $self->{file_dt}->year;
    $event{_YEAR}++ if ($event{_MONTH} eq 'Jan' || $event{_MONTH} eq 'Feb' || $event{_MONTH} eq 'Mar') && $self->{file_dt}->month >= 9;
    $event{_YEAR}-- if ($event{_MONTH} eq 'Dec' || $event{_MONTH} eq 'Nov' || $event{_MONTH} eq 'Oct') && $self->{file_dt}->month <= 3;

    $event{_MONTH_ID} = $MON{lc($event{_MONTH})};
    $event{_DATE}     = sprintf("%04d-%02d-%02d", $event{_YEAR}, $event{_MONTH_ID}, $event{_DAY});

    # YYYY-MM-DD HH:MI::SS
    $event{_EVENT_TS} = "$event{_DATE} $event{_TIME}";

    my $event_dt = strftime($event{_EVENT_TS});

    my $epoch = $event_ts->epoch;

    my $epoch_15min = floor($epoch / (15*60)) * (15*60);

    if (! exists $self->{logfiles}->{$epoch_15min}) {
        #
        my $epoch_dt = DateTime->from_epoch($epoch_15min);
        # Download S3 log file
        Wikia::EC2::S3::download(sprintf("s3://transformed-track-data/%s.%d.%04d.%02d.%02d.%02d.%02d.%02d.%d",
                                         $self->{source},
                                         15,  # 15 minute file
                                         $epoch_dt->year,
                                         $epoch_dt->month,
                                         $epoch_dt->day,
                                         $epoch_dt->hour,
                                         $epoch_dt->minute,
                                         $epoch_dt->second),
    }
}

1;
