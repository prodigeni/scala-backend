#!/usr/bin/perl
package EventFixStats;

use strict;
use warnings;
use Data::Dumper;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

my $YML = undef;
$YML = "$Bin/../../../wikia-conf/DB.moli.yml" if -e "$Bin/../../../wikia-conf/DB.moli.yml" ;

use Wikia::Utils;
use Wikia::LB;
use Wikia::DB;
use Wikia::Settings;

use Switch;
use Getopt::Long;
use Data::Dumper;
use base qw/Class::Accessor::Fast/;

$|++;
my $workers = 10;
my $limit = 1000;
my $debug = 0;
GetOptions(
	'workers=s' 	=> \$workers,
	'limit=s'		=> \$limit,
	'debug'			=> \$debug
);

sub new {
    my ($class, @args) = @_;
    my $self  = $class->SUPER::new;
    bless $self, $class;
}

sub rec_exists($$$) {
	my ($self, $dbs, $row) = @_;
	
	my @options = ();
	my @where = ( 
		"wiki_id = " . $dbs->quote($row->{city_id}),
		"page_id = " . $dbs->quote($row->{page_id}),
		"rev_id = " . $dbs->quote($row->{rev_id}),
		"log_id = " . $dbs->quote($row->{log_id})
	);
	my $oRow = $dbs->select(
		" count(0) as cnt ",
		" events ",
		\@where,
		\@options
	);
	my $cnt = $oRow->{cnt};
	
	return $cnt > 0;
}

sub fetch_data($;$) {
	my ($self, $dbs, $limit) = @_;
	my @res = ();
	#---
	my @db_fields = ('wiki_id', 'page_id', 'rev_id');
	my $q = "SELECT " . join( ',', @db_fields ) . " from events where page_ns = 6 and rev_id > 0 and media_type = 0 limit " . $limit ;
	my $sth_w = $dbs->prepare($q);
	if ($sth_w->execute() ) {
		my %results;
		@results{@db_fields} = ();
		$sth_w->bind_columns( map { \$results{$_} } @db_fields );
		
		@res = (\%results, sub {$sth_w->fetch() }, $sth_w, $dbs);
	}
	
	return @res;	
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

		my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS )} );

		# decode JSON string
		my $baseurl = "%s/api.php?action=query&prop=wkevinfo&pageid=%d&%s=%d&token=%s&meta=siteinfo&siprop=wikidesc&format=json";

		my ( $id, $id_value ) = ();
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

			# set MW Api params
			$id = 'revid'; 
			$id_value = $row->{rev_id};
			
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

					my $where = [
						"wiki_id = '" . Wikia::Utils->intval($wiki->{id}) . "'",
						"page_id = '" . Wikia::Utils->intval($page->{id}) . "'",
						"rev_id = '". Wikia::Utils->intval($row->{rev_id}) . "'" 
					];

					my %data = (
						"media_type" => Wikia::Utils->intval($revision->{media_type})
					);
					
					my $ins = $dbs->update( 'events', $where, \%data );					
					$processed++;
					$ok = 1;
				}
				undef($response);
			}
		} 
	}

	my $process_end_time = time();
	my @ts = gmtime($process_end_time - $process_start_time);

	print sprintf("result: key:%0d, %0d, %0d API calls\n", 
		$row->{page_id}, 
		$row->{rev_id}, 
		Wikia::Utils->intval($processed), 
	);
	print "row processed: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);

	return $ok;
}

package main;

use Thread::Pool::Simple;
use Data::Dumper;

print "Starting daemon ... \n";
# check time
my $script_start_time = time();

my $oEStats = new EventFixStats();

my $pool = Thread::Pool::Simple->new(
	min => 1,
	max => $workers,
	load => 4,
	do => [sub {
		my $data = shift;
		eval($oEStats->parse($data));
	}],
	monitor => sub {
		print "done \n";
	}
);

# load balancer
my $lb = Wikia::LB->instance;
$lb->yml( $YML ) if defined $YML;

# connect to the stats db
my $dbs = $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS );
print "Fetch data ($limit records) \n";
my ($res, $fetch, $sth) = EventFixStats->fetch_data($dbs, $limit);

if (defined($fetch) && defined($res)) {
	my $loop = 1;
	print "Starting daemon ... \n";
	while($fetch->()) {
		print sprintf ("%0d record: %0d, %0d, %0d\n", $loop, $res->{city_id}, $res->{page_id}, $res->{rev_id} ) if ( $debug );
		#'ev_id', 'city_id', 'page_id', 'rev_id', 'log_id', 'city_server', 'ev_date'
		my $tid = $pool->add($res);
		print "Thread $tid started \n" if ($debug);
		$loop++;
	}
	$sth->finish() if ($sth);
}
$dbs->disconnect() if ( $dbs );

print "Wait until all threads finish ... \n";
$pool->join();

my $script_end_time = time();
my @ts = gmtime($script_end_time - $script_start_time);

print "Process done: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);
1;
