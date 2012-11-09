#!/usr/bin/perl

use strict;

use Modern::Perl;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use Wikia::LB;
use Wikia::DB;
use Wikia::Config;

use Getopt::Long;
use Data::Dumper;

sub usage()
{
    my $name = "multilookup.pl";
    print "$name [--help] [--skip=db[,db2[,...]]] [--usedb=db[,db2[,...]]] [--all]\n\n";
    print "\thelp\t\t-\tprint this text\n";
    print "\tall\t\t-\tgenerate all reading stats\n";
    print "\tskip\t\t-\tcomma-separated list of dbs to skip\n";
    print "\tusedb\t\t-\tcomma-separated list of dbnames to use\n";
}

my ( $skip_dbs, $gen_all, $usedbs, $help ) = ();

GetOptions(	'help' => \$help, 'skip=s' => \$skip_dbs, 'usedb=s' => \$usedbs, 'all' => \$gen_all );

if ( (!$skip_dbs) && (!$gen_all) && (!$usedbs) && (!$help) ) {
	print STDERR "Use option --help to know how to use script \n";
	exit;
}
my @where = ();
my $INSERTS = 25;
if ($help) { &usage(); exit; }

#
# get admin connection to database
#
my $dbs = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS );
my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED, Wikia::LB::ADMIN );

my @where_db = ("city_public=1");
if ($skip_dbs) {
	my @skip_dbs = split /,/,$skip_dbs;
	push @where_db, "city_dbname not in (".join(",", map { $dbh->quote($_) } @skip_dbs).")";
}
if ($usedbs) {
	my @use_dbs = split /,/,$usedbs;
	push @where_db, "city_dbname in (".join(",", map { $dbh->quote($_) } @use_dbs).")";
}
my $whereclause = join(" and ", @where_db);

my $process_start_time = time();
my $main_loop = 0;
my @keys = ( 'ml_city_id', 'ml_ip', 'ml_count', 'ml_ts' );

say "get list of Wikis";
my $sth = $dbh->prepare("select city_id, city_dbname from wikicities.city_list where $whereclause order by city_id");
if ( $sth->execute() ) {
	while( my $row = $sth->fetchrow_hashref ) {
		my $loop = 0;
		my $index = 0;
		my $start_sec = time();
		my @ips = ();
		say $row->{'city_dbname'} . " processed (".$row->{'city_id'}.")" ;
		my $dbl = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, 'stats', $row->{'city_dbname'} );
		if ( $dbl ) {
			my $dbx = new Wikia::DB( {"dbh" => $dbl } );
			if ( $dbx->table_exists( 'page' ) ) {
				my $sth_w = $dbl->prepare("select rc_ip, INET_ATON(rc_ip) as rc_ip_int, count(0) as cnt, max(date_format(rc_timestamp, '%Y-%m-%d %H:%i:%s')) as max_time from recentchanges where rc_ip is not null group by rc_ip");
				if ( $sth_w->execute() ) {
					while(my ($rc_ip, $rc_ip_int, $cnt, $max_time) = $sth_w->fetchrow_array()) {
						if ( $rc_ip ne "" ) {
							$index++ if ( ( $loop > 0 ) && ( $loop % $INSERTS == 0 ) ) ;
							my @data = ( $row->{'city_id'}, $rc_ip_int, $cnt, $max_time );
							push @{$ips[$index]}, "'" . join( "', '", @data ) . "'";
							
							$loop++;
						}
					}
					$sth_w->finish();
				}
			} else {
				say "Invalid Wikia: " . $row->{'city_id'};
			}
		}
		
		say "Found " . scalar(@ips) . " packages to insert ";
		
		foreach ( @ips ) {
			my $values = join ( '), (', @{$_} ) ;
			if ( $values ) {
				my $sql = "INSERT IGNORE INTO specials.multilookup ( " . join(",", @keys) . " ) VALUES ( ";
				$sql .= $values ;
				$sql .= " ) ON DUPLICATE KEY UPDATE ml_count=values(ml_count), ml_ts=values(ml_ts) ";	
				$dbs->do($sql);
			}
		}
		
		my $end_sec = time();
		my @ts = gmtime($end_sec - $start_sec);
		say $row->{'city_dbname'} . " processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);
		$main_loop++;
	}
	$sth->finish;
}

my $process_end_time = time();
my @ts = gmtime($process_end_time - $process_start_time);
say "\nscript processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);
say "done";

1;
