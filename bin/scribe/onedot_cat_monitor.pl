#!/usr/bin/perl -w

use strict;

use Getopt::Long;
use Time::Local;
use Sys::Hostname;

use constant EMAIL => 'garth@wikia-inc.com';
use constant THRESH => 10*60;

my $email  = EMAIL();
my $thresh = THRESH();
my $verbose;
GetOptions('email=s'  => \$email,
		   'thresh=s' => \$thresh,
		   'verbose'  => \$verbose,
		   );

my $last_activity = `grep "onedot_cat.pl running" /var/log/syslog | tail -1`;
notify($email) unless $last_activity;

my ($Y, $M, $D, $h, $m, $s) = $last_activity =~ /^\D*(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})/;
if ($Y) {
	my $secs = timelocal($s, $m, $h, $D, $M-1, $Y);
	my $delta = time - $secs;
	
	print "Time delta is $delta\n" if $verbose;
	notify($email, $delta) if $delta > $thresh;
} else {
	notify($email);
}

################################################################################

sub notify {
	my ($email, $age) = @_;

	print "Sending email to $email: delta is ".($age || -1)."\n" if $verbose;
	
	my $host = hostname();
	my $subj = "[onedot_cat] $host";
	$subj .= $age ? " behind ${age}s" : " not running";
	my $body = "Threshhold is ${thresh}s";
	email($subj, $body, $email);

	exit;
}

sub email {
	my ($subj, $msg, $to) = @_;
	$to ||= Wikia::Deploy::NOTIFY_EMAIL();
	
	open(MAIL, "| mail -r 'onedot\@wikia-inc.com' -s \"$subj\" $to")
		or die "Can't open pipe to 'mail': $!\n";
	print MAIL $msg;
	close(MAIL);
}
