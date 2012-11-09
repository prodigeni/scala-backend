#!/usr/bin/perl
package EventsFix;

use strict;
use warnings;
use Data::Dumper;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

my $YML = undef;

use Wikia::Utils;
use Wikia::LB;
use Wikia::DB;
use Wikia::Settings;
use Wikia::Config;

use Switch;
use Getopt::Long;
use Data::Dumper;
use base qw/Class::Accessor::Fast/;

$|++;
my $workers = 10;
my $month = "";
my $usedbs = "";
my $todbs = "";
my $dry = 0;
my $debug = 0;
GetOptions(
	'workers=s' 	=> \$workers,
	'month=s'		=> \$month,
	'usedb=s'		=> \$usedbs,
	'todbs=s'	=> \$todbs,
	'dry=s'			=> \$dry,
	'debug'			=> \$debug
);

my $oConf = new Wikia::Config( {logfile => "/tmp/fixevents.log", csvfile => "/home/moli/$month/fixevents.sql"} );

sub new {
    my ($class, @args) = @_;
    my $self  = $class->SUPER::new;
    bless $self, $class;
}

sub rec_exists($$;$) {
	my ($self, $row, $type) = @_;

	$type = 0 unless $type;
	my $lb = Wikia::LB->instance;
	$lb->yml( $YML ) if defined $YML;				
	my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::METRICS )} );
	
	my @options = ();
	my @where = ( 
		"wiki_id = " . $dbs->quote($row->{city_id}),
		"page_id = " . $dbs->quote($row->{page_id})
	);
	if ( $type && $type == 3 ) {
		push @where, "event_type = 3";
	} else {
		push @where, "rev_id = " . $dbs->quote($row->{rev_id});
		push @where, "log_id = " . $dbs->quote($row->{log_id});
	}
	my $oRow = $dbs->select(
		" count(0) as cnt ",
		" metrics.event ",
		\@where,
		\@options
	);
	my $cnt = $oRow->{cnt};
	
	$dbs->disconnect() if ($dbs);
	
	return $cnt > 0;
}

sub count_rec_exists($$) {
	my ($self, $row) = @_;

	my $lb = Wikia::LB->instance;
	$lb->yml( $YML ) if defined $YML;				
	my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::METRICS )} );
	
	my @options = ();
	my @where = ( 
		"wiki_id = " . $dbs->quote($row->{city_id}),
		"page_id = " . $dbs->quote($row->{page_id})
	);

	my $oRow = $dbs->select(
		" count(0) as cnt ",
		" metrics.event ",
		\@where,
		\@options
	);
	my $cnt = $oRow->{cnt};
	
	$dbs->disconnect() if ($dbs);
	
	return $cnt == 0;
}

sub fetch_data($;$) {
	my ($self, $dbs, $city_id, $start_date, $end_date) = @_;
	my @res = ();
	#---
	my @db_fields = ('wiki_id', 'page_id', 'rev_id', 'log_id', 'user_id', 'page_ns', 'is_content', 'is_redirect', 'rev_timestamp', 'event_type');
	
	my $q = "SELECT " . join( ',', @db_fields ) . " from metrics.event where wiki_id = '".$city_id."' and rev_timestamp between '".$start_date."' and '".$end_date."' order by page_id" ;
	my $sth_w = $dbs->prepare($q);
	if ($sth_w->execute() ) {
		my %results;
		@results{@db_fields} = ();
		$sth_w->bind_columns( map { \$results{$_} } @db_fields );
		
		@res = (\%results, sub {$sth_w->fetch() }, $sth_w, $dbs);
	}
	
	return @res;	
}

sub check_records($$) {
	my ( $self, $dbs, $city_id, $dbname, $start_date, $end_date ) = @_;
	
	my $mConf = new Wikia::Config( {logfile => "/tmp/notexists.log", csvfile => "/home/moli/$month/pages.sql" } );
	
	my $lb = Wikia::LB->instance;
	$lb->yml( $YML ) if defined $YML;

	my $dbh = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, $dbname )} );
			
	my $select = "page_id, rev_id, date_format(rev_timestamp, '%Y-%m-%d %H:%i:%s') as rev_timestamp, rev_user as user_id, 0 as log_id, $city_id as city_id";
	my $from = "`".$dbname."`.`page`, `".$dbname."`.`revision`";
	my @where = (
		"page_id = rev_page",
		"date_format(rev_timestamp, '%Y-%m-%d %H:%i:%s') between ".$dbh->quote($start_date)." and ".$dbh->quote($end_date)
	);
	my @options = ("order by page_id");

	my $result = {'ok' => 0, 'invalid' => 0 };
	my $sth = $dbh->select_many($select, $from, \@where, \@options);
	if ($sth) {
		while(my $values = $sth->fetchrow_hashref()) {		
			my $exists = $self->rec_exists($values);
			
			if ( $exists ) {
				$result->{ok}++;
			} else {
				$result->{invalid}++;
				# connect to db 
				my $lb = Wikia::LB->instance;
				$lb->yml( $YML ) if defined $YML;				
				my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::EXTERNALSHARED )} );
				my $server_name = $dbr->get_server($city_id);
				my $rows = {
					'ev_id'	 => 1,
					'city_id' => $city_id,
					'page_id' => $values->{page_id},
					'rev_id'  => $values->{rev_id},
					'log_id'  => $values->{log_id},
					'city_server' => $server_name,
					'ev_date' => $values->{rev_timestamp},
					'priority' => 1				
				};
				my $options = undef;
				my $sql = $dbr->insert('metrics.dirty_event', 0, $rows, $options, 1, 1);
				$mConf->output_csv($sql . ";");
				$dbr->disconnect if ($dbr);				
			}
		}
		$sth->finish();
	}	
	
	$dbh->disconnect() if ($dbh);
	return $result;
}

sub check_archive($$) {
	my ( $self, $dbs, $city_id, $dbname, $start_date, $end_date ) = @_;
	
	my $mConf = new Wikia::Config( {logfile => "/tmp/archive_notexists.log", csvfile => "/home/moli/$month/archive.sql" } );
	
	my $lb = Wikia::LB->instance;
	$lb->yml( $YML ) if defined $YML;

	my $dbh = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, $dbname )} );
			
	my $select = "ar_page_id as page_id, 0 as rev_id, date_format(ar_timestamp, '%Y-%m-%d %H:%i:%s') as rev_timestamp, ar_user as user_id, log_id, $city_id as city_id";
	my $from = "`".$dbname."`.`archive`, `".$dbname."`.`logging`";
	my @where = (
		"ar_title = log_title",
		"ar_namespace = log_namespace",
		"date_format(ar_timestamp, '%Y-%m-%d %H:%i:%s') between ".$dbh->quote($start_date)." and ".$dbh->quote($end_date),
		"log_action = 'delete'"		
	);
	my @options = ("order by rev_timestamp");

	my $result = {'ok' => 0, 'invalid' => 0 };
	my $sth = $dbh->select_many($select, $from, \@where, \@options);
	if ($sth) {
		while(my $values = $sth->fetchrow_hashref()) {		
			my $exists = $self->rec_exists($values, 3);
			my $count_exists = $self->count_rec_exists($values);
			
			if ( $exists || $count_exists ) {
				$result->{ok}++;
			} else {
				# connect to db 
				my $lb = Wikia::LB->instance;
				$lb->yml( $YML ) if defined $YML;				
				my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::EXTERNALSHARED )} );
				my $server_name = $dbr->get_server($city_id);
				my $rows = {
					'ev_id'	 => 3,
					'city_id' => $city_id,
					'page_id' => $values->{page_id},
					'rev_id'  => $values->{rev_id},
					'log_id'  => $values->{log_id},
					'city_server' => $server_name,
					'ev_date' => $values->{rev_timestamp},
					'priority' => 1			
				};
				my $options = undef;
				my $sql = $dbr->insert('metrics.dirty_event', 0, $rows, $options, 1, 1);
				$mConf->output_csv($sql . ";");
				$dbr->disconnect if ($dbr);	
				$result->{invalid}++;			
			}
		}
		$sth->finish();
	}	
	
	$dbh->disconnect() if ($dbh);
	return $result ;
}

sub parse {
	my ($self, $row) = @_;

=params
  'ev_id' => INT,
  'city_id' => INT,
  'city_server' => STRING,
  'ev_date' => DATETIME,
  'page_id' => INT,
  'rev_id' => INT,
  'log_id' => INT
=cut	

	# check time
	my $process_start_time = time();
	
	# default result;
	my $ok = 0;
	my ($exists, $processed, $invalid, $notfound) = 0;
	if ( defined($row) && UNIVERSAL::isa($row,'HASH') ) {
		# connect to db 
		my $lb = Wikia::LB->instance;
		$lb->yml( $YML ) if defined $YML;

		my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::METRICS )} );

		# decode JSON string
		my $baseurl = "%s/api.php?action=query&prop=wkevinfo&pageid=%d&%s=%d&token=%s&meta=siteinfo&siprop=wikidesc&format=json";

		my ( $uid, $id, $id_value ) = ();
		# check values
		if ( defined ( $row->{wiki_id} ) ) { 
			my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::EXTERNALSHARED )} );
			$row->{city_server} = $dbr->get_server($row->{wiki_id});
			$dbr->disconnect if ($dbr);
			# server name and identifier of page is not set
			if ( !$row->{city_server} || !$row->{page_id} ) {
				$invalid++;
				print "\tInvalid parameters: " . Dumper($row) . "\n" if ( $debug );
				next;
			}

			if ( $row->{log_id} ) {
				$id = 'logid';
				$uid = 'log_id';
				$id_value = $row->{log_id};
			} elsif ( $row->{rev_id} ) {
				# set MW Api params
				$id = 'revid'; 
				$uid = 'rev_id';
				$id_value = $row->{rev_id};
			}
			
			# get data from MW API
			if ( defined $id && defined $id_value ) {
				my $settings = Wikia::Settings->instance;
				my $t = $settings->variables();
										
				my $url = sprintf($baseurl, $row->{city_server}, $row->{page_id}, $id, $id_value, $t->{ "wgTheSchwartzSecretToken" });
				print "Call MW API: " . $url . "\n" if ( $debug );

				my $params = {
					'action' => 'query',
					'prop' => 'wkevinfo',
					'pageid' => $row->{page_id},
					$id => $id_value,
					'token' => $t->{ "wgTheSchwartzSecretToken" },
					'meta' => 'siteinfo',
					'siprop' => 'wikidesc',
					'format' => 'json'
				};	
				my $response = Wikia::Utils->call_mw_api($row->{city_server}, $params, 0, 0); 
				if ( !defined $response ) {
					my $login = {
						'username' => $t->{ "wgWikiaBotUsers" }->{ "staff" }->{ "username" },
						'password' => $t->{ "wgWikiaBotUsers" }->{ "staff" }->{ "password" }
					};
					$response = Wikia::Utils->call_mw_api($row->{city_server}, $params, $login, $row->{priority}); 
				}
				
				my $nms = {};

				if ( $response->{query} ) {
					my $revision = $response->{query}->{revision};
					my $wiki = $response->{query}->{wikidesc};
					my $page = $response->{query}->{page};
					my $is_content = Wikia::Utils->intval($revision->{iscontent});
					my $is_redirect = Wikia::Utils->intval($revision->{isredirect});
					$is_content = ( $is_content == 1 ) ? 'Y' : 'N';
					$is_redirect = ( $is_redirect == 1 ) ? 'Y' : 'N';				

=test
					print "wiki = " . ( $wiki->{id} eq $row->{'wiki_id'} ) . "\n";
					print "page = " . ( $page->{id} eq $row->{'page_id'} ) . "\n";
					print "rev = " . ( Wikia::Utils->intval($revision->{revid}) == Wikia::Utils->intval($row->{'rev_id'}) ) . "\n";
					print "log = " . ( Wikia::Utils->intval($revision->{revid}) == Wikia::Utils->intval($row->{'rev_id'}) ) . "\n";
					print "user = " . ( $revision->{userid} eq $row->{'user_id'} ) . "\n";
					print "ns = " . ( $page->{namespace} eq $row->{'page_ns'} ) . "\n";
					print "content = " . ( $is_content eq $row->{'is_content'} ) . "\n";
					print "redir = " . ( $is_redirect eq $row->{'is_redirect'} ) . "\n";
					print "ts = " . ( $revision->{timestamp} eq $row->{'rev_timestamp'} ) . "\n";
=cut					

					if ( 
						( $revision ) &&
						( $wiki ) && 
						( $page ) &&
						( Wikia::Utils->intval($wiki->{id}) == Wikia::Utils->intval($row->{'wiki_id'}) ) &&
						( Wikia::Utils->intval($page->{id}) == Wikia::Utils->intval($row->{'page_id'}) ) &&
						( Wikia::Utils->intval($revision->{$id}) == Wikia::Utils->intval($row->{$uid}) ) &&
						( Wikia::Utils->intval($revision->{userid}) == Wikia::Utils->intval($row->{'user_id'}) ) &&
						( Wikia::Utils->intval($page->{namespace}) == Wikia::Utils->intval($row->{'page_ns'}) ) &&
						( $is_content eq $row->{'is_content'} ) &&
						( $is_redirect eq $row->{'is_redirect'} ) &&
						( $revision->{timestamp} eq $row->{'rev_timestamp'} ) 
					) {
						print "url: $url ok \n" if ($debug);
						$exists++;
					} 
					elsif ( !$revision ) {
						$notfound++
					}
					elsif ( !$wiki ) {
						$notfound++;
					}
					elsif ( !$page ) {
						$notfound++;
					}
					else {
						my %data = ();
						
						if ( $revision->{userid} ne $row->{'user_id'} ) {
							$data{'user_id'} = $revision->{userid};
						}
						
						if ( $page->{namespace} ne $row->{'page_ns'} ) {
							$data{'page_ns'} = $page->{namespace};
						}
						
						if ( $is_content ne $row->{'is_content'} ) {
							$data{'is_content'} = $is_content;
						}
						
						if ( $is_redirect ne $row->{'is_redirect'} ) {
							$data{'is_redirect'} = $is_redirect;
						}
						
						if ( $revision->{timestamp} ne $row->{'rev_timestamp'} ) {
							$data{'rev_timestamp'} = $revision->{timestamp};
						}					

						my $where = [
							"wiki_id = '" . Wikia::Utils->intval($wiki->{id}) . "'",
							"page_id = '" . Wikia::Utils->intval($page->{id}) . "'",
							$uid ." = '". Wikia::Utils->intval($id_value) . "'" 
						];
						if ( !$dry) {
							#my $ins = $dbs->update( 'events', $where, \%data );	
						} else {
							my $str = ""; my $rows = [];
							foreach my $key ( keys %data ) {
								push @$rows, " $key = '".$data{$key} . "' ";
							}
							if ( scalar @$rows ) {
								$str = "update events set " . join (",", @$rows) . " where " . join (" and ", @$where );
								$oConf->output_csv($str . ";");
							}
						}
						$invalid++;
					}
				} else {
					$notfound++;
				}
				undef($response);
				
				if ( $notfound ) {
					$row->{city_id} = $row->{wiki_id};
					my $count_exists = $self->rec_exists($row, 3);
					if ( !$count_exists ) {
						my $where = [
							"wiki_id = '" . Wikia::Utils->intval($row->{wiki_id}) . "'",
							"page_id = '" . Wikia::Utils->intval($row->{page_id}) . "'",
							$uid ." = '". Wikia::Utils->intval($id_value) . "'" 
						];
						
						my $str = "delete from events where " . join (" and ", @$where);
						$oConf->output_csv($str . ";");
					}
				}
			}
		}
		print sprintf("result: wiki: %0d, page: %0d, rev_id: %0d, log_id: %0d, exists:%0d, invalid:%0d, notfound:%0d \n", 
			$row->{wiki_id},
			$row->{page_id}, 
			$row->{rev_id}, 
			$row->{log_id},
			Wikia::Utils->intval($exists), 
			Wikia::Utils->intval($invalid), 	
			Wikia::Utils->intval($notfound)
		);		
	}

	my $process_end_time = time();
	my @ts = gmtime($process_end_time - $process_start_time);

	print "row processed: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);

	return $ok;
}

package main;

use Thread::Pool::Simple;
use Data::Dumper;

print "Starting daemon ... \n";
# check time
my $script_start_time = time();

my $oEStats = new EventsFix();

# load balancer
my $lb = Wikia::LB->instance;
$lb->yml( $YML ) if defined $YML;

# connect to wikicitiee
my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::EXTERNALSHARED )} );

# connect to the stats db
my $dbs = $lb->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::METRICS );
print "Fetch data ($month records) \n";

my $start_date = Wikia::Utils->first_datetime($month);
my $end_date = Wikia::Utils->last_datetime($month);

my @where_db = ("city_public = 1", "city_url not like 'http://techteam-qa%'");
if ($usedbs) {
	if ( $usedbs && $usedbs =~ /\+/ ) {
		# dbname=+177
		$usedbs =~ s/\+//i;
		push @where_db, "city_id >= " . $usedbs;
	} elsif ( $usedbs && $usedbs =~ /\-/ ) {
		# dbname=+177
		$usedbs =~ s/\-//i;
		push @where_db, "city_id <= " . $usedbs;
	} else { 
		my @use_dbs = split /,/,$usedbs;
		push @where_db, "city_dbname in (".join(",", map { $dbr->quote($_) } @use_dbs).")";
	}
}
if ($todbs) {
	push @where_db, "city_id <= " . $todbs;
}

$oConf->log("get list of wikis from city list", 1);
my ($databases) = $dbr->get_wikis(\@where_db);
#$dbr->disconnect();

foreach my $num (sort ( map { sprintf("%012u",$_) } (keys %{$databases}) ))
{
	#--- set city;
	my $city_id = int $num;
	#--- set start time
	my $start_sec = time();
	print $databases->{$city_id} . " processed (".$city_id.")";

	# check events
	my ($res, $fetch, $sth) = $oEStats->fetch_data($dbs, $city_id, $start_date, $end_date);

	if (defined($fetch) && defined($res)) {
		my $loop = 1;
		print "Starting daemon ... \n";
		while($fetch->()) {
			print sprintf ("%0d record: wikia: %0d, page: %0d, rev: %0d, log: %0d\n", $loop, $city_id, $res->{page_id}, $res->{rev_id}, $res->{log_id} );
			$oEStats->parse($res);
			$loop++;
		}
		$sth->finish() if ($sth);
	}
	
	# check page & revision
	my $result = $oEStats->check_records($dbs, $city_id, $databases->{$city_id}, $start_date, $end_date);
	print "page & revision: " . $result->{ok} . ", " . $result->{invalid} ." \n";
	
	# check archive 
	$result = $oEStats->check_archive($dbs, $city_id, $databases->{$city_id}, $start_date, $end_date);
	print "archive: " . $result->{ok} . ", " . $result->{invalid} ." \n";
}
$dbs->disconnect() if ( $dbs );
$dbr->disconnect() if ( $dbr );

my $script_end_time = time();
my @ts = gmtime($script_end_time - $script_start_time);

print "Process done: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);
1;
