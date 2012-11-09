#!/usr/bin/perl

my $YML = undef;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

$YML = "$Bin/../../../wikia-conf/DB.localhost.yml" if ($ENV{'DEVEL'});
my $database = 'wikicities';
$database = '_wikicities_' if ($ENV{'DEVEL'});

#print "YML = $YML \n";
use DBI;
use Wikia::LB;
use Wikia::DB;
use Wikia::Config;
use Wikia::Utils;
use Wikia::User;
use Pod::Usage;

use Getopt::Long;
use Data::Dumper;
use Scalar::Util 'looks_like_number';

#read long options

my $lb = Wikia::LB->instance;
$lb->yml( $YML ) if defined $YML;

my $INSERTS = 250;
my $tables = '';
my $keys = '';
my $to_file = 0;

GetOptions(	'help' => \$help, 'tables=s' => \$tables, 'keys=s' => \$keys );
pod2usage(-exitval => 1 ) if ( $help );
pod2usage(2) unless ( $tables && $keys );
my @where = ();

my $process_start_time = time();

my $oConf = new Wikia::Config( { logfile => "/tmp/migrate_tables.log" } );
$oConf->log ("Daemon started ...", $to_file);

sub getRecordsFromTable {
	my ( $db, $table, $key ) = @_;

	my $start = time();
	
	my $records = {};
	my $where = [];
	my $options = [];
	
	my $dbh = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, "stats", $db )} );
	my $sth = $dbh->select_many( "*", $table, $where, $options );
	if ( $sth ) {
		while( my $hash = $sth->fetchrow_hashref ) {
			my @_keys = ();
			foreach my $k ( @$key ) {
				push @_keys, $hash->{$k};
			}
			$inx = join('_', @_keys);
			$records->{$inx} = $hash unless ( $records->{$hash->{$inx}} );
		}
		$sth->finish();
	}
	my $end = time();
	my @ts_usr = gmtime($end - $start);
	$oConf->log ("\tRead " . scalar keys (%$records) . " records from " . $db . " : ".sprintf ("%d hours %d minutes %d seconds",@ts_usr[2,1,0]), $to_file);
	
	return $records;
}

sub shouldReplaceRecord {
	my ( $data, $data_to_add ) = @_;
	
	my $result = 0;
	if ( scalar keys %$data ) {
		foreach my $inx ( keys %$data ) {
			if ( looks_like_number( $data->{$inx} ) && looks_like_number( $data_to_add->{$inx} ) ) {
				#print "data ($inx) = " .  $data->{$inx} . ", to add = " . $data_to_add->{$inx} . " \n";
				if ( $data->{$inx} != $data_to_add->{$inx} ) {
					$result = 1;
				}
			} else {
				#print "data ($inx) = " .  $data->{$inx} . ", to add = " . $data_to_add->{$inx} . " \n";
				if ( $data->{$inx} ne $data_to_add->{$inx} ) {
					$result = 1;
				}
			}
			last if ( $result == 1 );
		}
	}
	#print "result = $result \n";	
	
	return $result;
}

my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS )} );
my @_tables = split /,/,$tables;
my @_keys = split /,/, $keys;
for ( my $i = 0; $i < scalar @_tables; $i++ ) {
	# data from central DB
	$start_sec = time();
	
	my $table = $_tables[$i];
	my @key = split /\+/, $_keys[$i] ;
	
	my $update_table = $database . '.' . $table;
	
	my $central_data = getRecordsFromTable( Wikia::LB::EXTERNALSHARED, $table, \@key );
	
	# data from stats DB
	my $stats_data = getRecordsFromTable( Wikia::LB::STATS, $update_table, \@key );

	my @insertKeys = ();
	my $index = 0;
	my @insertData = ();
	my @update = ();

	$dbs->execute('begin');

	if ( scalar(keys %$central_data) > 0 ) {
		my $loop = 0;
		foreach my $k ( sort keys %$central_data ) {
			if ( $central_data->{$k} ) {
				my %data = ();
				
				foreach my $inx ( sort keys %{$central_data->{$k}} ) {
					$data{$inx} = $central_data->{$k}->{$inx};
				}
			
				@insertKeys = keys %data if ( $loop == 0 );
				
				$index++ if ( ( $loop > 0 ) && ( $loop % $INSERTS == 0 ) ) ;
				
				if ( shouldReplaceRecord($central_data->{$k}, $stats_data->{$k} ) ) {
					push @{$insertData[$index]}, join(",", map { $dbs->quote($_) } values %data);
					
					my @conditions = (); my @key_values = split /\_/, $k; 
					for ( my $inx = 0; $inx < @key; $inx++ ) {
						push @conditions, $key[$inx] . ' = ' . $key_values[$inx] ;
					}
					my $res = $dbs->delete($update_table, \@conditions);					
					$loop++;
				}
			
				delete $stats_data->{$k};
			}
		}
	}
	
	$oConf->log ("insert  " . scalar(@insertData). " records", $to_file);
	if ( scalar @insertData ) {	
	
		foreach ( @insertData ) {
			my $values = join ( '), (', @{$_} ) ;
			if ( $values ) {
				my $sql = "INSERT IGNORE INTO " . $update_table . " ( " . join(',', @insertKeys) . " ) VALUES ( ";
				$sql .= $values ;
				$sql .= " ) ";

				$dbs->execute($sql);
			}
		}
	}
	undef(@insertData);
	
	$oConf->log ("delete " . scalar keys (%$stats_data). " records", $to_file);
	# remove inactive users;
	if ( scalar keys %$stats_data ) {
		foreach my $id ( keys %$stats_data ) {
			my @conditions = ();
			my @key_values = split /\_/, $id; 
			
			for ( my $i = 0; $i < @key; $i++ ) {
				push @conditions, $key[$i] . ' = ' . $key_values[$i] ;
			}
			my $res = $dbs->delete($update_table, \@conditions);
		}
	}
	undef($central_data);
	undef($stats_data);

	$dbs->execute('commit');

	my $end_sec = time();
	my @ts = gmtime($end_sec - $start_sec);
	$oConf->log( $table . " processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]), $to_file);

	sleep(1);
}
#---
$dbs->disconnect() if ($dbs);

my $process_end_time = time();
@ts = gmtime($process_end_time - $process_start_time);
$oConf->log ("\nscript processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]), $to_file);
$oConf->log("done", $to_file);
1

__END__

=head1 NAME

migrate_tables.pl

=head1 SYNOPSIS

migrate_tables.pl [--help] [--tables=T1[,..,TN]] [--keys=K1[,..,KN]]

  --help            brief help message
  --tables=T1[,..,TN]   comma separated list of tables to migrate
  --keys=K1[,..,KN] comma separated list of primary keys of tables ( use "+" if primary key contains more then one column )


=head1 OPTIONS
=over 8
=item B<--help>
Print a brief help message and exits.

=item B<--tables>
Comma separated list of tables to migrate

=item B<--keys>
Comma separated list of primary keys of tables ( use "+" if primary key contains more then one column )
=back

=head1 DESCRIPTION

B<This programm> copies records from --tables to the wikicities database on statsdb

=cut

