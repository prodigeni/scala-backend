#!/usr/bin/perl -w

use strict;

use Getopt::Long;

use Time::Local;

# Where to find the purge log
use constant PURGE_LOG   => '/etc/sv/image-purger/log/main/current';

# How many seconds old the last log line can be before an alert is sent
use constant ALERT_SECS  => 5;

# Who to email on alerts
use constant ALERT_EMAIL => ['garth@wikia-inc.com'];

# Set some defaults
my ($alert_secs, @alert_email);

GetOptions('secs|s=s'  => \$alert_secs,
		   'email|e=s' => \@alert_email);

$alert_secs ||= ALERT_SECS();
@alert_email = @{ ALERT_EMAIL() } unless @alert_email;

my $cmd = 'tail -1 '.PURGE_LOG();
my $last_line = `$cmd`;

my ($Y, $M, $D, $h, $m, $s) = $last_line =~ /(\d{4})-(\d{2})-(\d{2})_(\d{2}):(\d{2}):(\d{2})/;
my $secs = timelocal($s, $m, $h, $D, $M-1, $Y-1900);

my $delta = time - $secs;

# See if we need to alert
if ($delta > $alert_secs) {
	email_alert(\@alert_email, $delta);
}

################################################################################

sub email_alert {
	my ($alert_email, $delta) = @_;
	my $subj = "Last purge activity ${delta}s ago";
	my $msg = "No activity in the purge log in $delta second(s).  See log at:\n\n";
	$msg .= "\t".PURGE_LOG()."\n";

	foreach my $to (@$alert_email) {
		open(MAIL, "| mail -r 'purge-fastly\@wikia-inc.com' -s \"$subj\" $to")
			or die "Can't open pipe to 'mail': $!\n";
		print MAIL $msg;
		close(MAIL);
	}
}
