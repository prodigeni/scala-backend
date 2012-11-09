package Wikia::Events;

use common::sense;
use FindBin qw/$Bin/;
use Data::Dumper;
use Time::HiRes qw(gettimeofday tv_interval);
use Moose;
use Moose::Util::TypeConstraints;
use MooseX::Types::JSON qw( JSON );
use JSON::XS;
#use HttpSQS;
use Redis;
use Thread::Pool::Simple;
use utf8;

use Wikia::Scribe;
use Wikia::Log;
use Wikia::DB;
use Wikia::LB;
use Wikia::User;
use Wikia::WikiFactory;
use Wikia::SimpleQueue;
use Wikia::Utils qw( intval datetime TS_DB);

use constant ADOPT_FLAG	=> 64;
use constant ADOPT_LIMIT_EDITS => 1000;
use constant LOCAL_USER_PATH => '/usr/wikia/source/backend/bin/scribe/events_local_users.pl';

has "host"   => (
	is			=> "rw",
	isa			=> "Str",
	required	=> 1,
	default		=> '127.0.0.1' #'10.10.10.153'
);
has "port"   => (
	is			=> "rw",
	isa			=> "Int",
	required	=> 1,
	default 	=> 6379 #1218
);
has "queue"  => (
	is			=> "rw",
	isa			=> "Redis",
	default 	=> sub {
		my $self = shift;
		return Redis->new( 
			server => sprintf("%s:%s", $self->host, $self->port)
		);
	}
);
has "queue_id" => (
	is 			=> "rw",
	isa			=> "Str"
);
has "workers" => ( 
	is 			=> "rw", 
	isa 		=> "Int", 
	default		=> 10
);
has "insert" => ( 
	is 			=> "rw", 
	isa 		=> "Int", 
	default		=> 50
);
has "debug" => (
	is 			=> "rw",
	isa			=> "Int",
	default		=> 0
);
has "allowed_keys" => ( 
	is 			=> "rw", 
	isa 		=> "ArrayRef", 
	default 	=> sub { 
		my @scribeKeys = keys %{$Wikia::Scribe::scribeKeys};
		return \@scribeKeys;
	}
); 
has "record"     => (
	is			=> "rw",
	isa			=> JSON,
	trigger 	=> sub {
		my ($self, $record) = @_;
		$self->row( decode_json( $record ) );
		$self->run_tasks();
	}
);
has "iowa"       => (
	is 			=> "rw",
	isa			=> "Int",
	default		=> 0,
	trigger		=> sub {
		my $self = shift;
		if ( $self->iowa > 0 ) {
			$self->table( sprintf( "%s.%s", Wikia::LB::METRICS, 'event' ) );
		}
	}
);
has "row"        => (
	is          => "rw",
	isa         => "HashRef"
);
has "dbs"        => (
	is			=> "rw",
	lazy_build  => 1 
);
has "dba"        => (
	is			=> "rw",
	lazy_build  => 1 
);
has "dbc"        => (
	is			=> "rw",
	lazy_build  => 1 
);
has "dbw"		 => (
	is 			=> "rw",
	lazy_build	=> 0
);
has "db_stats"   => (
	is			=> "rw",
	isa			=> "Int",
	trigger		=> sub { (shift)->dbs(); }
);
has "db_dataware" => (
	is			=> "rw",
	isa			=> "Int",
	trigger		=> sub { (shift)->dba(); }
);
has "db_central" => (
	is			=> "rw",
	isa			=> "Int",
	trigger		=> sub { (shift)->dbc(); }
);
has "wikia"       => (
	is          => "rw",
	isa         => "Wikia::WikiFactory"
);
has "user"       => (
	is          => "rw",
	isa         => "Wikia::User"
);
has "table"		 => (
	is			=> "rw",
	isa			=> "Str",
	default		=> sprintf( "%s.%s", Wikia::LB::STATS, "events" )
);
has "usejobs"    => (
	is			=> "rw",
	isa			=> "HashRef"
);
has "useusers"    => (
	is			=> "rw",
	isa			=> "HashRef"
);

__PACKAGE__->meta->make_immutable;

sub interval_time {
	my ( $self, $t_start ) = @_;
	
	return tv_interval( $t_start, [ $self->current_time() ] );
}

sub current_time {
	my $self = shift;
	return gettimeofday();
}

sub job {
	my ( $self, $tid, $qid ) = @_;
	
	say "Job $tid is running" if ( $self->debug );
	my $len = $self->queue->llen( $qid );
	my $status = $self->queue->info;
	say "status = " . Dumper( $status ) if ( $self->debug ) ;
	
	if ( $len > 0 ) {
		# read queue 
		say "$qid: parse " . $len . " unread messages ";
		$self->queue_id( $qid );
		$self->usejobs( {} );
		while ( my $record = $self->queue->lpop( $qid ) ) {
			# parse message
			$self->record( $record );			
		}
		
		# reset queue
		say "$qid: after parse: " . $self->queue->llen( $qid ) . " messages";	
	}
}

sub run_tasks {
	my $self = shift;

	$self->wikia( Wikia::WikiFactory->new( city_id => $self->row->{ cityId } ) );
	if ( !$self->wikia->city_dbname ) {
		say "Invalid Wikia identifier: " . $self->row->{ cityId };
		return 0;
	}

	# user object
	if ( $self->row->{userId} ) {
		$self->user( Wikia::User->new( db => $self->wikia->city_dbname, id => $self->row->{userId} ) );
	}
		
	# connect to statsdb 
	$self->db_stats(1);
	# put record to db
	$self->_put_to_db();
	if ( $self->iowa == 0 ) {
		# update listusers table
		$self->_update_list_users();
		# update city flags ( used by wiki adopt )
		$self->_update_city_flag();
		# update spawnjob queue
		# $self->_queue_job();
		# copy user record to db cluster
		#$self->_user_cluster();
		# update dataware table
		$self->_update_dataware();
		$self->_update_dataware_image_review();
		# update last_timestamp in city_list
		$self->_update_last_timestamp();
	}
}

sub run {
	my $self = shift;

	my $t_start = [ $self->current_time() ];

	# parse all queues
	my $pool = Thread::Pool::Simple->new(
		min => 4,
		max => $self->workers,
		load => 6,
		do => [sub {
			$self->job( @_ );
		}],
		post => sub {
			say "done";
		},
		passid => 1,
	);
	
	map { $pool->add( $_ ) } @{$self->allowed_keys};
	$pool->join();
	
	say "Messages processed: " . $self->interval_time( $t_start );

	return 1;
}

sub _build_dbs {
	my $self = shift;
	
	my $db = ( $self->db_stats ) ? Wikia::LB::DB_MASTER : Wikia::LB::DB_SLAVE;
	say "Connect to " . $db . " (" . $self->db_stats . ") on statsdb" if ( $self->debug );
	my $dbs = new Wikia::DB( {"dbh" => Wikia::LB->instance->getConnection( $db, undef, ( $self->iowa == 0 ) ? Wikia::LB::STATS : Wikia::LB::METRICS )} );
	$self->dbs( $dbs );
}

sub _build_dba {
	my $self = shift;
	
	my $db = ( $self->db_dataware ) ? Wikia::LB::DB_MASTER : Wikia::LB::DB_SLAVE;
	say "Connect to " . $db . " on dataware" if ( $self->debug );
	my $dba = new Wikia::DB( {"dbh" => Wikia::LB->instance->getConnection( $db, undef, Wikia::LB::DATAWARESHARED )} );
	$self->dba( $dba );	
}

sub _build_dbc {
	my $self = shift;
	
	my $db = ( $self->db_central ) ? Wikia::LB::DB_MASTER : Wikia::LB::DB_SLAVE;
	say "Connect to " . $db . " on wikicities" if ( $self->debug );
	my $dbc = new Wikia::DB( {"dbh" => Wikia::LB->instance->getConnection( $db, undef, Wikia::LB::EXTERNALSHARED )} );
	$self->dbc( $dbc );	
}

sub _build_dbw {
	my $self = shift;
	
	say "Connect to Wikia: " . $self->wikia->city_dbname if ( $self->debug );
	my $dbw = new Wikia::DB( {"dbh" => Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, $self->wikia->city_dbname )} );
	$self->dbw( $dbw );	
}

sub _invalid_users {
	my ( $self ) = @_;
	
	my $options = [];
	my $where   = [ "user_id = " . $self->dbs->quote( $self->row->{ userId } ) ];
	my $oRow = $self->dbs->select( " count(0) as cnt ", " ignored_users ", $where, $options );
	my $cnt = $oRow->{cnt} || 0;
	
	return $cnt > 0;
}

sub _put_to_db {
	my $self = shift;
	
	say "Put record to event table" if ( $self->debug );
	
	my %data = (
		"wiki_id" 		=> $self->row->{ cityId },
		"page_id" 		=> $self->row->{ pageId },
		"rev_id" 		=> $self->row->{ revId },
		"log_id" 		=> ( $self->row->{ logId } || 0 ),
		"user_id" 		=> ( $self->row->{ userId } || 0 ),
		"user_is_bot" 	=> $self->row->{ userIsBot } ? 'Y' : 'N',
		"page_ns" 		=> $self->row->{ pageNamespace },
		"is_content" 	=> ( ($self->row->{ isContent }||0) == 1 ) ? 'Y' : 'N',
		"is_redirect" 	=> ( ($self->row->{ isRedirect }||0) == 1 ) ? 'Y' : 'N',
		"-ip" 			=> ( $self->row->{ userIp } ) ? "INET_ATON('".$self->row->{ userIp }."')" : 0,
		"rev_timestamp" => $self->row->{ revTimestamp },
		"image_links" 	=> ( $self->row->{ imageLinks } || 0 ),
		"video_links" 	=> ( $self->row->{ videoLinks } || 0 ),
		"total_words" 	=> ( $self->row->{ totalWords } || 0 ),
		"rev_size" 		=> ( $self->row->{ revSize } || 0 ),
		"wiki_lang_id" 	=> ( $self->row->{ languageId } || 0 ),
		"wiki_cat_id" 	=> ( $self->row->{ categoryId }->{cat_id} || 0 ),
		"event_type" 	=> $Wikia::Scribe::scribeKeys->{ $self->queue_id },
		"media_type"	=> ( $self->row->{ mediaType } || 0 ),
	);
	
	#if ( $self->iowa ) {
		$data{ 'rev_date' } = Wikia::Utils->datetime( $self->row->{ revTimestamp }, Wikia::Utils::TS_DB )->ymd( '-' );
		$data{ 'beacon_id' } = $self->row->{ beaconId };		
	#}
	
	my $res = $self->dbs->insert( $self->table, "", \%data, "", 1 );

	return 1;
}

sub _update_list_users {
	my $self = shift;
		
	if ( $self->user && $self->row->{userId} ) {
		say "Update local users table (for user: " . $self->row->{userId} . ")";
		my @args = (
		"/usr/bin/perl",
		"$Bin/../checkPID.pl --dry ",
		"--script=\"" . LOCAL_USER_PATH . " --fromid=" . $self->row->{cityId} . " --toid=" . $self->row->{cityId} . " --user=" . $self->row->{userId} . "\""
		);
		my $path = join ' ', @args;
		say "Run $path" if ( $self->debug );
		system( $path ) == 0 or say "system " . Dumper(@args) . " failed: $?";
	}
	
	return 1;
}

sub _count_wiki_edits {
	my $self = shift;

	say "Count number of edits for Wiki: " . $self->row->{ cityId } if ( $self->debug );
	
	my $options = [];
	my $where = [ "wiki_id = " . $self->dbs->quote( $self->row->{ cityId } ) ];
	my $row = $self->dbs->select( " sum(edits) as cnt ", " specials.events_local_users ", $where, $options );
	return $row->{cnt};
}

sub _count_page_edits {
	my $self = shift;
	
	return 0 unless ( $self->row->{ pageId } );
	
	$self->_build_dbw();
	say "Count number of edits for page " . $self->row->{ pageId } . " on Wikia : " . $self->row->{ cityId } if ( $self->debug );
	
	my $options = [];
	my $where = [ "rev_page = " . $self->dbw->quote( $self->row->{ pageId } ) ];
	my $row = $self->dbw->select( " count(0) as cnt ", " revision ", $where, $options );
	return $row->{cnt};
}

sub _update_city_flag {
	my $self = shift;
	
	if ( !defined $self->wikia->city_flags ) {
		return 0;
	}
	
	if ( ! $self->user ) {
		return 0;
	}
	
	if ( $self->wikia->city_flags & ADOPT_FLAG ) {
		my $nbr_edits = $self->_count_wiki_edits();
		if ( ( $nbr_edits > ADOPT_LIMIT_EDITS ) || ( grep /^\Qsysop\E$/, @{$self->user->groups} ) ) {
			say "Update city_flags: " . $self->wikia->city_flags;	
			$self->db_central(1);
			my $conditions = [ "city_id = " . $self->dbc->quote($self->row->{ cityId }) ];
			my $data = { "city_flags" => $self->wikia->city_flags &~ ADOPT_FLAG };
			my $q = $self->dbc->update( 'city_list', $conditions, $data );
		}
	}
	
	return 1;	
}

sub _queue_job {
	my $self = shift;

	if ( $self->usejobs->{ $self->row->{cityId} } ) {
		return 0;
	}

	my $queue = Wikia::SimpleQueue->instance( name => "spawnjob" );
	$queue->push( $self->row->{ cityId } );
	say "Inform job queue about change in city=${ \$self->row->{ cityId } }";
	$self->usejobs->{ $self->row->{cityId} } = 1;
}

sub _user_cluster {
	my $self = shift;

	if ( defined $self->wikia->city_cluster && $self->wikia->city_cluster ne '' && $self->wikia->city_cluster ne 'c1' ) {
		if ( $self->user ) {
			say "Update user record on cluster: " . $self->wikia->city_cluster;			
			my $exists = $self->user->user_exists_cluster( $self->wikia->city_cluster );
			if ( !$exists ) {
				$self->user->copy_to_cluster( $self->wikia->city_cluster );
			}
		}
	}
}

sub _update_dataware {
	my $self = shift;

	if ( !defined $self->row->{ pageTitle } ) {
		return 0;
	}

	$self->db_dataware(1);

	my $res = undef;
	if ( ! defined $self->row->{ logId } || $self->row->{ logId } == 0 ) {
		my $edits = $self->_count_page_edits();
		my $data = {
			"page_wikia_id"    => int $self->row->{ cityId },
			"page_id"          => int $self->row->{ pageId },
			"page_namespace"   => int $self->row->{ pageNamespace },
			"page_title_lower" => lc($self->row->{pageTitle}),
			"page_title"       => $self->row->{ pageTitle },
			"page_status"      => 0,
			"page_is_content"  => ( $self->row->{ isContent } == 1 ) ? 1 : 0,
			"page_is_redirect" => ( $self->row->{ isRedirect } == 1 ) ? 1 : 0,
			"page_edits"       => int $edits,
			"page_latest"      => int $self->row->{ revId },
			"page_last_edited" => $self->row->{ revTimestamp }
		};
		say "Update dataware";
		my $update = " ON DUPLICATE KEY UPDATE ";
		$update .= "page_status = values(page_status), ";
		$update .= "page_latest = values(page_latest), ";
		$update .= "page_edits = values(page_edits), ";
		$update .= "page_last_edited = values(page_last_edited), ";
		$update .= "page_is_content = values(page_is_content), ";
		$update .= "page_is_redirect = values(page_is_redirect) ";
		my $ins_options = [ $update ];
		$res = $self->dba->insert( 'pages', "", $data, $ins_options, 1 );
	} elsif ( defined $self->row->{ logId } && $self->row->{ logId } > 0 ) {
		if ( defined ( $self->row->{ cityId } ) && defined ( $self->row->{ pageId } ) ) {
			my $data = {
				"page_wikia_id"    => int $self->row->{ cityId },
				"page_id"          => int $self->row->{ pageId }
			};
			say "Remove record from dataware";
			$res = $self->dba->delete( 'pages', $data );
		}
	}

	return $res;
}

sub _update_dataware_image_review {
	my $self = shift;

	if ( int $self->row->{ pageNamespace } == 6  && $self->row->{ pageTitle} =~ /\.png|bmp|gif|jpg|jpeg|ico|svg/i ) { 

		if ( !defined $self->row->{ pageTitle } ) {
			return 0;
		}
	
		$self->db_dataware(1);
	
		my $res = undef;
		my $state = 0;
		my $flags = 0;
		
		if ( defined $self->row->{ logId } && $self->row->{ logId } > 0) {
			# mark as deleted state
			$state = 3; 
		}

		# See if any rows are marked as top_200 and lazily set the remaining rows
		my $wiki_id = $self->row->{cityId};
		my $row = $self->dba->query(qq(
			SELECT COUNT(*) = 1 AS top_200
			FROM (SELECT *
				  FROM image_review
				  WHERE wiki_id = $wiki_id
				    AND top_200 IS TRUE
				    LIMIT 1
				 ) t1));

		my $data = {
			wiki_id		=> $wiki_id,
			page_id		=> $self->row->{pageId},
			revision_id	=> $self->row->{revId},
			user_id		=> $self->row->{userId},
			last_edited	=> $self->row->{revTimestamp},
			state		=> $state,
			flags		=> $flags,
			top_200		=> ($row->{top_200} || 0),
		};
	
		say "Update image_review dataware";
	
		my $update = " ON DUPLICATE KEY UPDATE ";
		$update .= "last_edited = values(last_edited), ";
		$update .= "state = values(state), ";
		$update .= "revision_id = values(revision_id), ";
		$update .= "user_id = values(user_id) ";
	
		my $ins_options = [ $update ];
	
		$res = $self->dba->insert( 'image_review', "", $data, $ins_options, 1 );
	
		return $res;
	} else {
		return 0;
	}
}

sub _update_last_timestamp {
	my $self = shift;
	
	# update city_list
	if ( 
		( defined $self->wikia->city_last_timestamp && $self->row->{ revTimestamp } && $self->row->{ revTimestamp } gt $self->wikia->city_last_timestamp ) || 
		( !defined $self->wikia->city_last_timestamp )
	) {
		$self->db_central(1);
		my $conditions = [ "city_id = " . $self->dbc->quote( $self->row->{ cityId } ) ];
		my $data = { "city_last_timestamp" => $self->row->{ revTimestamp } };

		$self->dbc->update('city_list', $conditions, $data) or say "cannot update city_list table";
		say "Update last timestamp for Wiki: " . $self->row->{ cityId };		
	}	
}

1;
