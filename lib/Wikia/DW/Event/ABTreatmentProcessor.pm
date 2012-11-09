package Wikia::DW::Event::ABTreatmentProcessor;

use strict;

use base qw( Wikia::DW::Event::BaseProcessor );

use Date::Parse;
use Wikia::DW::Common;

sub load_config {
    my $self = shift;
    $self->SUPER::load_config;
    $self->{CUSTOM_LOAD_PARAMS}  = [ 'beacon', 'treatment_group_id', '@ip' ];
    $self->{CUSTOM_LOAD_COLUMNS} = $self->{CUSTOM_LOAD_PARAMS};
    $self->{UNESCAPE}            = [ 'varnish_time' ];
    $self->{PROCNAME}            = 'ab_treatment';
    $self->{TRANSFORM}           = \&transform;
}

sub transform {
    my $event = shift;
    # DateFormat:  Thu, 21 Dec 2000 16:01:07 +0200
    my ($varnish_ss,
        $varnish_mm,
        $varnish_hh,
        $varnish_day,
        $varnish_month,
        $varnish_year,
        $varnish_zone) = strptime($event->{varnish_time});

    if (defined $varnish_year) {
        my ($ss,$mm,$hh,$day,$month,$year,$zone) = strptime($event->{_EVENT_TS} . '+00:00');

        if ($varnish_year  <= $year  &&
            $varnish_month <= $month &&
            $varnish_day   <= $day) {
            # Use varnish_time as the event time
            $event->{_EVENT_TS} = sprintf( "%04d-%02d-%02d %02d:%02d:%02d",
                                           $varnish_year  + 1900,
                                           $varnish_month + 1,
                                           $varnish_day,
                                           $varnish_hh,
                                           $varnish_mm,
                                           $varnish_ss );
        }
    }
    return 1;
}

sub load_stagedb_sql {
    my $self = shift;
    my   @load_columns = @{$self->{INTERNAL_LOAD_COLUMNS}};
    push @load_columns,  @{$self->{CUSTOM_LOAD_COLUMNS}};
    return
"  LOAD DATA LOCAL INFILE '$self->{loadfile}'
  INTO TABLE $self->{STAGEDB}.${\($self->tablename)}
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' (" . join(',', @load_columns) . ")
   SET source  = '$self->{fileparser}->{source}',
       file_id = $self->{fileparser}->{file_id},
       ip      = INET_ATON(\@ip)"
}

1;
