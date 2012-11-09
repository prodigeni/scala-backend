#!/usr/bin/perl

my $YML = undef;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

BEGIN {
	$YML = "$Bin/../../../wikia-conf/DB.localhost.yml" if ($Bin =~ /moli/) ;
}

use DBI;
use Wikia::LB;
use Wikia::DB;
use Wikia::Config;
use Wikia::Utils;

use Getopt::Long;
use Data::Dumper;

#read long options

my $lb = Wikia::LB->instance;
$lb->yml( $YML ) if defined $YML;

my $to_file = 1;
sub usage() {
    my $name = "active_users.pl";
    print "$name [--help] [--cat=category] [--lang=language]\n\n";
    print "\thelp\t\t-\tprint this text\n";
    print "\tlang=X\t\t-\tstats of language\n";
    print "\tcat=Y\t\t-\tstats for category\n";
    print "\tmonth=YYYYMM\t\t-\t\n";
}

GetOptions(	'help' => \$help, 'lang=s' => \$lang, 'cat=s' => \$cat, 'month=s' => \$month );

if ( ! ($lang || $cat || $month) ) {
	print STDERR "Use option --help to know how to use script \n";
	exit;
}
my @where = ();
if ($help) {
	&usage(); exit;
}

my $process_start_time = time();

my $oConf = new Wikia::Config( { logfile => "/tmp/active_users.log", csvfile => "/tmp/active_users.csv" } );
$oConf->log ("Daemon started ...", $to_file);

my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::STATS )} );
my $dbh = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED )} );

my $where = [
	'rev_timestamp between ' . $dbh->quote(Wikia::Utils->first_datetime($month)) . ' and ' . $dbh->quote(Wikia::Utils->last_datetime($month)),
	'(event_type = 1 or event_type = 2)'
];
if ( $cat ) {
	push @$where, "wiki_cat_id = '".$cat."'";
}
if ( $lang ) {
	my $oLang = $dbh->get_lang_by_code($lang);
	push @$where, "wiki_lang_id = '".$oLang->{lang_id}."'";
}
my $options = ['group by wiki_id'];
my $sth_w = $dbs->select_many("wiki_id, count(distinct(user_id)) as active_users, count(0) as edits", "events", $where, $options);
if ($sth_w) {
	$oConf->output_csv("Wikia;Active users;Edits");
	while(my ($wiki_id, $active_users, $edits) = $sth_w->fetchrow_array()) {
		my $server = $dbh->get_server($wiki_id);
		$oConf->output_csv($server . ";" . $active_users . ";" . $edits);
	}
	$sth_w->finish();
}

$dbs->disconnect() if ($dbs);
$dbh->disconnect() if ($dbh);

my $process_end_time = time();
@ts = gmtime($process_end_time - $process_start_time);
$oConf->log ("\nscript processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]));
$oConf->log("done", $to_file);

1;
