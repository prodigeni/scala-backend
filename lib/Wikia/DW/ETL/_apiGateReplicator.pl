#!/usr/bin/perl
#
# Author: Sean Colombo
# Date: 20120403
#
# Script to help with the temporary dumping of data to load into production for ApiGate stats.
####

##### CONFIG #####
# Get the Wikia backend libs to help w/connecting to prod databases.
BEGIN{ push @INC, "/usr/wikia/backend/lib/"; }
use Wikia::LB;
use Wikia::DB;

# Used for getting database connections to production wikis (to update the recorded image sizes).
my $wikiaLoadBalancer = Wikia::LB->instance;
$wikiaLoadBalancer->debug(1);

my $SECONDS_IN_DAY = (60*60*24);
my $NUMBER_OF_DAYS_TO_LOAD_BACK = 1; # only need today and yesterday usually (might have to bump this up if we need to re-load data that was botched or missed).
##### CONFIG #####


# Figure out todays date and yesterday's date (possibly more dates if configured to look back further).
my @datesToPull = ();
push(@datesToPull, time ); # today
for(my $cnt=0; $cnt < $NUMBER_OF_DAYS_TO_LOAD_BACK; $cnt++){
	push(@datesToPull, ( time - ($SECONDS_IN_DAY * ($cnt+1)) ) );
}


# Process each day, one at a time.
foreach my $timeStamp (sort(@datesToPull)){
	processDay( $timeStamp );
}

print "Done.\n";



####
# Given a timestamp, returns the corresponding date in YYYY-mm-dd format.
####
sub dateStrFromTime($){
	my $timeStamp = shift;

	(my $sec,my $min,my $hour,my $mday,my $mon,my $year,my $wday,my $yday,my $isdst) = localtime($timeStamp);
	$mon++; # make January start at 1 instead of 0
	$year += 1900;
	if($mon < 10){ $mon = "0$mon"; }
	if($mday < 10){ $mday = "0$mday"; }

	return "$year-$mon-$mday";
} # end dateStrFromTime()

####
# Given a timestamp that falls in a day, pulls the data from the beginning of that day through the end of that day
# and exports it from the data warehouse & imports it into production.
####
sub processDay($){
	my $timeStamp = shift;
	my $nextDay = $timeStamp + $SECONDS_IN_DAY;

	my $startDate = dateStrFromTime($timeStamp);
	my $endDate = dateStrFromTime($nextDay);
	
	my $fileName = "/tmp/rollup_api_events_$startDate.csv";

	my $queryString = "SELECT period_id, time_id, HEX(api_key) AS api_key, null AS api_type, null AS api_function, null AS ip, null AS wiki_id, SUM(events) AS events";
	$queryString .= " FROM rollup_api_events";
	$queryString .= " WHERE time_id >= '$startDate' and time_id < '$endDate' AND period_id in (1, 3, 60) GROUP BY time_id,period_id,api_key";
	my $cmd = "/usr/wikia/backend/bin/dw/query2csv --query \"$queryString\" --outfile \"$fileName\"";
	print "Running command:\n$cmd\nIf there is any output from query2csv, it will show up here (may take a minute):\n";
	print `$cmd`;
	
	#print "Sleeping. Press enter to create next file.\n";
	#my $pause = <STDIN>;

	# Read-write connection to Wikicities for checking whether databases exist prior to connecting to them.
	# NOTE: connects for each day because the connections are made to disconnect after a short timeout which might be smaller than the time it takes to dump a day's data.
	my $wikicitiesDbh = $wikiaLoadBalancer->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::EXTERNALSHARED);

	# Load the data from the file right into the database (with REPLACE since part of the day might have already been loaded).
	my $loadDataQuery = "LOAD DATA LOCAL INFILE '$fileName' REPLACE INTO TABLE rollup_api_events FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY'\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES";
	if($wikicitiesDbh->do( $loadDataQuery ) == 0){
		print "ERROR: Could not load data in file '$fileName' with query:\n$loadDataQuery\n$DBI::errstr\n";
	} else {
		print "Data for $startDate loaded.\n";
		
		print "Deleting temp file...\n";
		unlink($fileName);
		print "Done.\n";
	}
} # end processDay()
