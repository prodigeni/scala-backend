#!/usr/bin/perl

use strict;
my $YML = undef;
use FindBin qw/$Bin/;
use lib "$Bin/../lib";

BEGIN {
	$YML = "$Bin/../../wikia-conf/DB.localhost.yml" if ($Bin =~ /moli/) ;
}

use DBI;
use Wikia::LB;
use Wikia::DB;

use Getopt::Long;
use Data::Dumper;
use PHP::Serialization qw/serialize unserialize/; 

#read long options

sub usage()
{
    my $name = "clear_user_preferences.pl"; 
    print "\thelp\t\t-\tprint this text\n";
    print "\toption=Y\t\t-\toption to find in user preferences";
    print "\tuser=X[,Y[,Z...]]\t\t-\trun for user login = X,Y,Z ...\n";
}

my ( $help, $user, $option, $limit ) = ();

GetOptions(	'help' => \$help, 'user=s' => \$user, 'option=s' => \$option, 'limit=s' => \$limit );

if ( (!$help) && (!$option) ) {
	print STDERR "Use option --help to know how to use script \n";
	exit;
}

my @where = ();
if ($help) { &usage(); exit; }

my $log = 1;
my $update = 1;

#----
my $lb = Wikia::LB->instance;
$lb->yml( $YML ) if $YML;
my $dbh = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED )} );
#----

my @where_db = ("user_options like '%" . $option . "%'");
if ($user) {
	my @use_users = split /,/,$user;
	push @where_db, "user_name in (".join(",", map { $dbh->quote($_) } @use_users).")";
}
my $whereclause = join(" and ", @where_db);

print "get list of users \n";
my $dbList = $dbh->get_users(\@where_db, 'user_options', $limit);
my %databases = %{$dbList};
#----
# get data from databases
#----
$dbh->disconnect();

my $process_start_time = time();
my $main_loop = 0;
my %RESULTS = ();
foreach my $num (sort ( map { sprintf("%012u",$_) } (keys %databases) ))
{
	#--- set city;
	my $user_id = int $num;
	#--- set start time
	my $start_sec = time();
	print "Processed (".$user_id.") \n";

	#logic
	my $user_options = $databases{$user_id};
	
	my @txt_opt = split(/\n/, $user_options);
	my $new_options = [];
	foreach (@txt_opt) {
		my ($key, $value) = split(/=/);
		if ( $key ne $option ) {
			push @$new_options, "$key=$value";
		}
	}

	my $new_options_txt = join("\n", @$new_options);

	print Dumper($new_options_txt);
	
	my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, 'stats', Wikia::LB::EXTERNALSHARED )} );	
	my %data = ( "user_options" => $new_options_txt );
	my @conditions = (
		" user_id = $user_id "
	);
	my $res = $dbw->update( 'user', \@conditions, \%data);

	my $end_sec = time();
	my @ts = gmtime($end_sec - $start_sec);
	print $user_id . " processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]) . "\n";
	$main_loop++;
}

my $process_end_time = time();
my @ts = gmtime($process_end_time - $process_start_time);
print "\nscript processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]) . "\n";
print "done \n";

1;
