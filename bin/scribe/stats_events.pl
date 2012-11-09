#!/usr/bin/perl
package EventStats;

use common::sense;
use Data::Dumper;
use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

my $YML = undef;
$YML = "$Bin/../../../wikia-conf/DB.moli.yml" if -e "$Bin/../../../wikia-conf/DB.moli.yml" ;

use Wikia::Scribe;
use Wikia::Utils;
use Wikia::LB;
use Wikia::ExternalLB;
use Wikia::DB;
use Wikia::Settings;
use Wikia::User;
use Wikia::SimpleQueue;
use Wikia::Log;

use Switch;
use Getopt::Long;

use Data::Dumper;
use base qw/Class::Accessor::Fast/;

$|++;
my $check_users = 0;
my $workers = 10;
my $limit = 100;
my $debug = 0;
my $interval = 60;
GetOptions(
	'workers=s' 	=> \$workers,
	'limit=s'		=> \$limit,
	'debug'			=> \$debug,
	'interval=s'	=> \$interval
);
my $limit_edits = 1000;
my $adopt_flag = 64;

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

sub nbr_edits($$$) {
	my ($self, $dbs, $wiki_id) = @_;

	my @options = ();
	my @where = (
		"wiki_id = " . $dbs->quote($wiki_id)
	);
	my $oRow = $dbs->select(
		" sum(edits) as cnt ",
		" specials.events_local_users ",
		\@where,
		\@options
	);
	my $cnt = $oRow->{cnt};

	return $cnt;
}

sub page_is_removed($$$;$) {
	my ($self, $dbs, $row, $event) = @_;

	my @options = ();
	my @where = (
		"wiki_id = " . $dbs->quote($row->{city_id}),
		"page_id = " . $dbs->quote($row->{page_id})
	);
	if ( ( defined $event ) && ( $event > 0 ) ) {
		push @where, " event_type = $event "	;
	}
	my $oRow = $dbs->select(
		" count(0) as cnt ",
		" events ",
		\@where,
		\@options
	);
	my $cnt = $oRow->{cnt};

	return $cnt;
}

sub check_is_user_invalid ($$$) {
	my ( $self, $dbs, $user_id ) = @_;
	
	my @options = ();
	my @where = (
		"user_id = " . $dbs->quote( $user_id )
	);
	my $oRow = $dbs->select(
		" count(0) as cnt ",
		" ignored_users ",
		\@where,
		\@options
	);
	my $cnt = $oRow->{cnt};
	
	return ( $cnt == 0 ) ? 1 : 0 ;
}

sub check_wikia_page_exists($$) {
	my ( $self, $row ) = @_;

	my $lb = Wikia::LB->instance;
	$lb->yml( $YML ) if defined $YML;
	my $oWikia = $row->{wikia};

	my $cnt = 0;
	if ( $oWikia ) {
		my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', $oWikia->{city_dbname} )} );

		my @options = ();
		my @where = (
			"page_id = " . $dbr->quote($row->{page_id})
		);
		my $oRow = $dbr->select(
			" count(0) as cnt ",
			" page ",
			\@where,
			\@options
		);
		$cnt = $oRow->{cnt};

		if ( $cnt > 0 && $row->{rev_id} > 0 ) {
			@options = ();
			@where = (
				"rev_id = " . $dbr->quote($row->{rev_id})
			);
			$oRow = $dbr->select(
				" count(0) as cnt ",
				" revision ",
				\@where,
				\@options
			);
			$cnt = $oRow->{cnt};
		} elsif ( $cnt > 0 && $row->{log_id} > 0 ) {
			@options = ();
			@where = (
				"log_id = " . $dbr->quote($row->{rev_id}),
				"log_action = 'delete'"
			);
			$oRow = $dbr->select(
				" count(0) as cnt ",
				" logging ",
				\@where,
				\@options
			);
			$cnt = $oRow->{cnt};
		}
		$dbr->disconnect() if ($dbr);
	}

	return $cnt;
}

sub fetch_data($;$) {
	my ($self, $dbs, $limit, $interval) = @_;
	my @res = ();
	#---
	my @db_fields = ('ev_id', 'city_id', 'page_id', 'rev_id', 'log_id', 'city_server', 'ev_date', 'adddate(ev_date, interval 1 hour) as max_date', 'priority');
	my $q = "SELECT " . join( ',', @db_fields ) . " from scribe_events where ev_date < now() - INTERVAL " . $interval . " MINUTE  order by ev_date desc limit " . $limit ;
	my $sth_w = $dbs->prepare($q);
	if ($sth_w->execute() ) {
		my %results;
		@results{@db_fields} = ();
		$sth_w->bind_columns( map { \$results{$_} } @db_fields );

		@res = (\%results, sub {$sth_w->fetch() }, $sth_w, $dbs);
	}

	return @res;
}

sub remove_scribe_event {
	my ($self, $dbs, $row) = @_;
	# delete from scribe events
	my $conditions = {
	  'ev_id'		=> Wikia::Utils->intval( $row->{ev_id} ),
	  'city_id' 	=> Wikia::Utils->intval( $row->{city_id} ),
	  'city_server' => $row->{city_server},
	  'ev_date' 	=> $row->{ev_date},
	  'page_id'		=> Wikia::Utils->intval( $row->{page_id} ),
	  'rev_id'		=> Wikia::Utils->intval( $row->{rev_id} ),
	  'log_id'		=> Wikia::Utils->intval( $row->{log_id} )
	};
	my $q = $dbs->delete( Wikia::Scribe::SCRIBE_EVENTS_TABLE, $conditions);
}

sub daily_user_edits {
	my ( $self, $dbs, $wiki, $page, $revision, $row ) = @_;

	# daily user edits
	my @ts = split(/\s/, $revision->{timestamp});
	my $ts_date = $ts[0]; $ts_date =~ s/\-//g;

	my %data = (
		"wiki_id"		=> Wikia::Utils->intval($wiki->{id}),
		"page_id"		=> Wikia::Utils->intval($page->{id}),
		"page_ns"		=> Wikia::Utils->intval($page->{namespace}),
		"user_id" 		=> Wikia::Utils->intval($revision->{userid}),
		"wiki_lang_id" 	=> Wikia::Utils->intval($wiki->{langid}),
		"wiki_cat_id" 	=> Wikia::Utils->intval($wiki->{catid}),
		"user_is_bot"	=> $revision->{userisbot} ? 'Y' : 'N',
		"is_content" 	=> ( $row->{is_content} == 1 ) ? 'Y' : 'N',
		"is_redirect" 	=> ( $row->{is_redirect} == 1 ) ? 'Y' : 'N',
		"edits"			=> ( $row->{ev_id} eq Wikia::Scribe::DELETE_CATEGORY ) ? -1 : 1,
		"editdate"		=> $ts_date
	);
	my @ins_options = ( " ON DUPLICATE KEY UPDATE edits = edits + values(edits) " );
	my $res = $dbs->insert( 'edits_daily_users', "", \%data, \@ins_options, 1 );

	return $res;
}

sub daily_edited_pages {
	my ( $self, $dbs, $wiki, $page, $revision, $row ) = @_;

	# daily edited pages
	my @ts = split(/\s/, $revision->{timestamp});
	my $ts_date = $ts[0]; $ts_date =~ s/\-//g;

	my %data = (
		"wiki_id"		=> Wikia::Utils->intval($wiki->{id}),
		"page_id"		=> Wikia::Utils->intval($page->{id}),
		"page_ns"		=> Wikia::Utils->intval($page->{namespace}),
		"wiki_lang_id" 	=> Wikia::Utils->intval($wiki->{langid}),
		"wiki_cat_id" 	=> Wikia::Utils->intval($wiki->{catid}),
		"is_content" 	=> ( $row->{is_content} == 1 ) ? 'Y' : 'N',
		"is_redirect" 	=> ( $row->{is_redirect} == 1 ) ? 'Y' : 'N',
		"total_words"	=> ( $row->{ev_id} eq Wikia::Scribe::DELETE_CATEGORY ) ? 0 : Wikia::Utils->intval($revision->{words}),
		"edits"			=> ( $row->{ev_id} eq Wikia::Scribe::DELETE_CATEGORY ) ? -1 : 1,
		"editdate"		=> $ts_date
	);
	my @ins_options = ( " ON DUPLICATE KEY UPDATE edits = edits + values(edits), total_words = values(total_words) " );
	my $res = $dbs->insert( 'edits_daily_pages', "", \%data, \@ins_options, 1 );

	return $res;
}

sub local_users {
	my ( $self, $dbs, $wiki, $revision, $row ) = @_;

	my $user = new Wikia::User( db => $wiki->{db}, id => $revision->{userid} );
	my $res = undef;
	if ( $user ) {
		my $cmd = "perl $Bin/../checkPID.pl --script=\"$Bin/events_local_users.pl --fromid=" . $wiki->{id} . " --toid=" . $wiki->{id} . " --user=" . $revision->{userid} . "\"" ;
		#say "Execute: $cmd";
		system( $cmd ); 

		# adopt Wiki or not - this is the question
		if ( defined $wiki->{flags} && ( $wiki->{flags} & $adopt_flag ) ) {
			my $nbr_edits = $self->nbr_edits($dbs, $wiki->{id});
			my $remove_flag = 0;
			# check number of edits
			if ( $nbr_edits > $limit_edits ) {
				$remove_flag = 1;
			} else {
				if ( grep /^\Qsysop\E$/, @{$user->groups} ) {
					$remove_flag = 1;
				}
			}

			if ( $remove_flag == 1 ) {
				# remove flag
				my $lb = Wikia::LB->instance;
				$lb->yml( $YML ) if defined $YML;
				my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::EXTERNALSHARED )} );
				my @conditions = (
					"city_id = " . $dbw->quote($wiki->{id})
				);
				my %data = (
					"city_flags" => $wiki->{flags} &~ $adopt_flag
				);

				my $q = $dbw->update('city_list', \@conditions, \%data);
				$dbw->disconnect() if ($dbw);
			}
		}
	}

	return $res;
}

sub user_cluster {
	my ( $self, $wiki, $revision ) = @_;

	if ( defined $wiki->{position} && ( $wiki->{position} ne '' ) ) {
		my $user = new Wikia::User( db => "wikicities", id => $revision->{userid} );
		my $res = undef;
		if ( $user ) {
			my $exists = $user->user_exists_cluster( $wiki->{position} );
			if ( !$exists ) {
				$user->copy_to_cluster( $wiki->{position} );
			}
		}
	}
}

sub update_dataware {
	my ( $self, $wiki, $page, $revision, $row ) = @_;

	my $lb = Wikia::LB->instance;
	$lb->yml( $YML ) if defined $YML;
	my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::DATAWARESHARED )} );

	my $res = undef;
	if ( $row->{log_id} == 0 ) {
		my $lctitle = $page->{lctitle} || "\L$page->{title}";
		my $is_redirect = Wikia::Utils->intval($revision->{isredirect});
		my %data = (
			"page_wikia_id"    => Wikia::Utils->intval($wiki->{id}),
			"page_id"          => Wikia::Utils->intval($page->{id}),
			"page_namespace"   => Wikia::Utils->intval($page->{namespace}),
			"page_title"       => Wikia::Utils->fixutf($page->{title}),
			"page_title_lower" => $lctitle,
			"page_latest"      => Wikia::Utils->intval($page->{latest}),
			"page_status"      => ( $is_redirect == 1 ) ? 1 : 0,
			"page_counter"     => 0,
			"page_edits"       => 0,
		);

		my @ins_options = ( " ON DUPLICATE KEY UPDATE page_status = values(page_status), page_latest = values(page_latest) " );
		$res = $dbw->insert( 'pages', "", \%data, \@ins_options, 1 );
	} elsif ( $row->{log_id} > 0 ) {
		my %data = (
			"page_wikia_id"    => Wikia::Utils->intval($wiki->{id}),
			"page_id"          => Wikia::Utils->intval($page->{id}),
			"page_namespace"   => Wikia::Utils->intval($page->{namespace})
		);
		$res = $dbw->delete( 'pages', \%data);
	}

	return $res;
}

=item queue_job

send information to job queue that there is wikia for processing. Currently
uses mysql database

=cut
sub queue_job {
	my ( $self, $dbs, $wiki ) = @_;

	my $queue = Wikia::SimpleQueue->instance( name => "spawnjob" );
	$queue->push( $wiki->{ "id" } );
	say "Inform job queue about change in city=${ \$wiki->{ id } }";
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
	my $last_revision = "";
	my %scribeKeys = reverse %{$Wikia::Scribe::scribeKeys};
	my ($exists, $processed, $invalid, $notfound) = 0;
	if ( defined($row) && UNIVERSAL::isa($row,'HASH') ) {
		# connect to db
		my $lb = Wikia::LB->instance;
		$lb->yml( $YML ) if defined $YML;
		my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS )} );

		# decode JSON string
		my $baseurl = "%s/api.php?action=query&prop=wkevinfo&pageid=%d&%s=%d&token=%s&meta=siteinfo&siprop=wikidesc&format=json";

		# allowed keys
		my $allowed_keys = [values %{$Wikia::Scribe::scribeKeys}];

		my ( $id, $id_value ) = ();
		# check values
		if ( defined ( $row->{ev_id} ) && ( Wikia::Utils->in_array( $row->{ev_id}, $allowed_keys ) ) ) {
			# server name and identifier of page is not set
			if ( !$row->{city_server} || !$row->{page_id} ) {
				$invalid++;
				print "\tInvalid parameters: " . Dumper($row) . "\n" if ( $debug );
				next;
			}

			if ( $self->rec_exists($dbs, $row) ) {
				$exists++; $ok = 1;
				print "\tRecord exists \n" if ( $debug );
			} else {
				# set MW Api params
				if (
					$scribeKeys{ $row->{ev_id} } eq Wikia::Scribe::EDIT_CATEGORY ||
					$scribeKeys{ $row->{ev_id} } eq Wikia::Scribe::CREATEPAGE_CATEGORY ||
					$scribeKeys{ $row->{ev_id} } eq Wikia::Scribe::UNDELETE_CATEGORY
				) {
					$id = 'revid';
					$id_value = $row->{rev_id};
				}
				elsif ( $scribeKeys{ $row->{ev_id} } eq Wikia::Scribe::DELETE_CATEGORY ) {
					$id = 'logid';
					$id_value = $row->{log_id};
				}
				else {
					print "Invalid category: " . $row->{ev_id} . "\n" if ( $debug );
					$id = $id_value = undef;
					$invalid++;
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
					my $response = Wikia::Utils->call_mw_api($row->{city_server}, $params, 0, $row->{priority});
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

						if ( $revision && $wiki && $page ) {
							my $res ;
							
							# don't count tests users
							my $add = ( $check_users ) ? $self->check_is_user_invalid( $dbs, $revision->{userid} ) : 1;
							
							$row->{is_content} = $is_content;
							$row->{is_redirect} = $is_redirect;
														
							if ( $add > 0 ) {
								my %data = (
									"wiki_id" 		=> Wikia::Utils->intval($wiki->{id}),
									"page_id" 		=> Wikia::Utils->intval($page->{id}),
									"rev_id" 		=> Wikia::Utils->intval($row->{rev_id}),
									"log_id" 		=> Wikia::Utils->intval($row->{log_id}),
									"user_id" 		=> Wikia::Utils->intval($revision->{userid}),
									"user_is_bot" 	=> $revision->{userisbot} ? 'Y' : 'N',
									"page_ns" 		=> Wikia::Utils->intval($page->{namespace}),
									"is_content" 	=> ( $is_content == 1 ) ? 'Y' : 'N',
									"is_redirect" 	=> ( $is_redirect == 1 ) ? 'Y' : 'N',
									"-ip" 			=> ( $revision->{user_ip} ) ? "INET_ATON('".$revision->{user_ip}."')" : 0,
									"rev_timestamp" => $revision->{timestamp},
									"image_links" 	=> Wikia::Utils->intval($revision->{imagelinks}),
									"video_links" 	=> Wikia::Utils->intval($revision->{video}),
									"total_words" 	=> Wikia::Utils->intval($revision->{words}),
									"rev_size" 		=> Wikia::Utils->intval($revision->{size}),
									"wiki_lang_id" 	=> Wikia::Utils->intval($wiki->{langid}),
									"wiki_cat_id" 	=> Wikia::Utils->intval($wiki->{catid}),
									"event_type" 	=> ( $revision->{isnew} == 1 ) ? $Wikia::Scribe::scribeKeys->{Wikia::Scribe::CREATEPAGE_CATEGORY} : $row->{ev_id},
									"media_type"	=> Wikia::Utils->intval($revision->{media_type})
								);
								$res = $dbs->insert( 'events', "", \%data, "", 1 );

								# list users
								if ( $revision->{userid} > 0  ) {
									$res = $self->local_users( $dbs, $wiki, $revision, $row );
								}

								# daily user edits
								#$res = $self->daily_user_edits($dbs, $wiki, $page, $revision, $row );

								# daily edited pages
								#$res = $self->daily_edited_pages( $dbs, $wiki, $page, $revision, $row );
							}

							# queue job
							$self->queue_job( $dbs, $wiki );

							# user on clusters
							$res = $self->user_cluster( $wiki, $revision );

							# update dataware.pages
							$res = $self->update_dataware( $wiki, $page, $revision, $row );

							$last_revision = $revision->{timestamp};

							$processed++;
							$ok = 1;
						} else {
							# should return TRY_LATER ?
							print "Cannot call MW API: " . $url . " - response: " . Dumper($response->{query}) . "\n" if ( $debug );
							print "Page or revision not found\n\t1. " .  Dumper($row) . "\n\t2. api: $url\n" if ( $debug );
							# check page is removed
							my $is_removed = $self->page_is_removed($dbs, $row, 3);
							if ( $is_removed > 0 ) {
								$processed++;
								$ok = 1;
							} else {
								$is_removed = $self->page_is_removed($dbs, $row);
								my $wikia_page_exists = $self->check_wikia_page_exists($row);
								if ( $is_removed == 0 && $wikia_page_exists == 0 ) {
									$processed++;
									$ok = 1;
								} else {
									$notfound++;
								}
							}
						}
						undef($revision);
						undef($wiki);
						undef($page);
					} else {
						# should return TRY_LATER ?
						print "Invalid MW APi response: " . Dumper($response) . "\n" if ( $debug );
						print "invalid message: \n\t1. " . Dumper($row) . " \n\t2. api: $url\n" if ($debug);
						print "Invalid MW API response: $url \n";
						$invalid++;
						$ok = 0;
					}
					undef($response);
					undef(%scribeKeys);
				}
			}
		} else {
			print "Invalid message category: " . Dumper($row) . "\n" if ( $debug );
			$invalid++;
		}
		undef($allowed_keys);

		if ( $invalid || $notfound ) {
			# check Wiki is closed
			if ( defined $row->{wikia} && $row->{wikia}->{city_public} == 0 ) {
				print "Wiki " . $row->{city_id} . " has been closed \n";
				$ok = 1;
			} elsif ( !defined $row->{wikia} ) {
				$ok = 1;
			}
		}

		# remove record from scribe event table
		if ( $ok == 1 ) {
			# update city_list
			if ( defined $row->{wikia} && $last_revision ) {
				if ( $last_revision gt $row->{wikia}->{city_last_timestamp} ) {
					# update last timestamp
					# load balancer
					my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::EXTERNALSHARED )} );
					my @conditions = (
						"city_id = " . $dbw->quote($row->{city_id})
					);
					my %data = (
						"city_last_timestamp"	=> $last_revision
					);

					my $q = $dbw->update('city_list', \@conditions, \%data);
					$dbw->disconnect() if ($dbw);
				}
			}

			# remove scribe_event
			$self->remove_scribe_event($dbs, $row);
		}
	}

	my $process_end_time = time();
	my @ts = gmtime($process_end_time - $process_start_time);

	print sprintf("result: key:%0d, %0d exists in DB, %0d API calls, %0d not found, %0d invalid messages\n",
		$row->{ev_id},
		Wikia::Utils->intval($exists),
		Wikia::Utils->intval($processed),
		Wikia::Utils->intval($notfound),
		Wikia::Utils->intval($invalid)
	);
	print "row processed: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);
	
	# update log #bugid: 6713
	if ( $ok ) {
		my $log = Wikia::Log->new( name => "eventd" );
		$log->update();
	}

	return $ok;
}

package main;

use Thread::Pool::Simple;
use Data::Dumper;

print "Starting daemon ... \n";
# check time
my $script_start_time = time();

my $oEStats = new EventStats();

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
my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::EXTERNALSHARED ) } );
print "Fetch data ($limit records) \n";
my ($res, $fetch, $sth) = EventStats->fetch_data($dbs, $limit, $interval);

if (defined($fetch) && defined($res)) {
	my $loop = 1;
	print "Starting daemon ... \n";
	my $wikis = {};
	while($fetch->()) {
		print sprintf ("%0d record: %s, %0d, %0d, %0d, %s\n", $loop, $res->{ev_id}, $res->{city_id}, $res->{page_id}, $res->{rev_id}, $res->{city_server}, $res->{priority}) if ( $debug );
		#'ev_id', 'city_id', 'page_id', 'rev_id', 'log_id', 'city_server', 'ev_date'
		if ( !$wikis->{ $res->{city_id} } ) {
			my $oWikia = $dbr->id_to_wikia($res->{city_id});
			if ( $oWikia && UNIVERSAL::isa($oWikia, 'HASH') ) {
				$wikis->{ $res->{city_id} } = $oWikia;
			}
		}
		$res->{wikia} = $wikis->{ $res->{city_id} };
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

$dbr->disconnect() if ( $dbr );

print "Process done: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);
1;
