package Wikia::Onedot;

use common::sense;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";

use Wikia::Utils qw( note );
use Wikia::Memcached;
use Wikia::DB;
use Wikia::LB;
use Wikia::Log;

use DateTime;
use TokyoCabinet;
use Moose;
use Data::Dumper;

has dbr => 					( is => "rw", lazy_build => 0 ); 
has dbw => 					( is => "rw", lazy_build => 0 ); 
has dbc => 					( is => "rw", lazy_build => 0 ); 
has day => 					( is => "rw", isa => "Str", default => sub { DateTime->now()->ymd('') } );
has week => 				( is => "rw", isa => "Str", default => sub { sprintf( "%d%d", DateTime->now()->year(), DateTime->now()->week_number() ) } );
has debug => 				( is => "rw", isa => "Int", required => 1 );
has params =>				( is => "rw", isa => "HashRef", lazy_build => 0 );
has db_path =>				( is => "ro", isa => "Str", default => sub { "/tokyo_data/stats/" } );
has ns_db_file => 			( is => "rw", isa => "Str", default => sub { "onedot_namespaces.tch" } );
has user_db_file => 		( is => "rw", isa => "Str", default => sub { "onedot_users.tch" } );
has page_db_file => 		( is => "rw", isa => "Str", default => sub { "onedot_pages.tch" } );
has tags_db_file => 		( is => "rw", isa => "Str", default => sub { "onedot_tags.tch" } );
has wikia_db_file => 		( is => "rw", isa => "Str", default => sub { "onedot_wikia.tch" } );
has weekly_user_db_file =>	( is => "rw", isa => "Str", default => sub { "onedot_weekly_users.tch" } );
has weekly_wikia_db_file => ( is => "rw", isa => "Str", default => sub { "onedot_weekly_wikia.tch" } );
has tags => 				( is => "rw", isa => "HashRef", lazy_build => 0 );
has lang => 				( is => "rw", isa => "Int", lazy_build => 0);
has jobs => 				( is => "rw", isa => "Int",	default => 25 );
has insert => 				( is => "rw", isa => "Int", default => 250 );
has limit =>				( is => "rw", isa => "Int", default => 15000 );
has data =>					( is => "rw", isa => "HashRef", lazy_build => 0 );

## Options for raw logging
# Enable/disable
has raw_logging =>          ( is => "rw", isa => "Bool", default => 0 );

# The memory buffer to use and its upper size limit
has raw_buffer =>           ( is => "rw", isa => "ArrayRef" );
has max_buffer_size =>      ( is => "ro", isa => "Int", default => 5_000 );

# The raw log directory, current log file, when it was created and how long before rotating it
has raw_log_dir =>          ( is => "ro", isa => "Str", default => "/tokyo_data/raw" );
has raw_log_name =>         ( is => "rw", isa => "Str", default => \&_new_raw_log_name );
has raw_log_created =>      ( is => "rw", isa => "Int" );
has log_rotate_secs =>      ( is => "ro", isa => "Int", default => 5*60 );
has max_log_size =>         ( is => "ro", isa => "Int", default => 9_000_000 );


sub  _build_dbw {
	my $self = shift;
	return $self->dbw if $self->dbw;

	my $lb = Wikia::LB->instance;

	my $dbh = $lb->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS );
	$dbh->{ "mysql_auto_reconnect" } = 1;
	
	my $dbw = new Wikia::DB( { "dbh" => $dbh } );
	$self->dbw( $dbw ) if $dbw;
}

sub  _build_dbc {
	my $self = shift;
	return $self->dbc if $self->dbc;

	my $lb = Wikia::LB->instance;

	my $dbh = $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED );
	$dbh->{ "mysql_auto_reconnect" } = 1;
	
	my $dbc = new Wikia::DB( { "dbh" => $dbh } );
	$self->dbc( $dbc ) if $dbc;
}

sub _build_day {
	my $self = shift;
	
	my $day = DateTime->now()->ymd('');
	$self->day($day);
}

sub _build_week {
	my $self = shift;
	
	my $week = sprintf( "%d%02d", DateTime->now()->year(), DateTime->now()->week_number() );
	$self->week($week);
}

sub _build_params {
	my ($self, $params) = @_;
	
	$self->params( $params ) if ( $params );
}

use Cache::LRU;
our $LRU = Cache::LRU->new(size => 50_000);
our $HIT = 0;
our $TOT = 0;
sub build_tags {
	my $self = shift;

	$TOT++;
	my $oMemc = Wikia::Memcached->instance->memc();
	my $memkey = sprintf( "perl:onedot:tags:%0d", $self->params->{'c'} );	
	my ($mem_miss, $mc_miss);

	# Are these tags in memory?
	my $tags = $LRU->get($memkey);

	# Fall back to memcache
	if ($tags) {
		$HIT++;
	} else {
		$mem_miss = 1;
		$tags = $oMemc->get($memkey) if $oMemc;
	}

	# Fall back to the datahbase
	if (!$tags) {
		$mc_miss = 1;

		$self->_build_dbc();
		if ($self->dbc) {
			$tags = $self->dbc->get_wiki_tags($self->params->{'c'});
			$tags ||= 'no';
		} else {
			note "WARNING: Could not open connection to DB";
		}
	}

	# Set any caches that had a miss
	$LRU->set($memkey => $tags) if $LRU and $mem_miss;
	$oMemc->set($memkey, $tags, 3 * 60 * 60) if $oMemc and $mc_miss;

	# If 'tags' is no, express that as undef for the rest of the processing
	$self->tags($tags eq 'no' ? undef : $tags);
	
	return $tags;
}

sub build_lang {
	my ( $self ) = @_;
	
	my $lang = undef;
	if ($self->params->{'lid'}) {
		$lang = int $self->params->{'lid'};
		$lang = $self->lang($lang);
	} else {
		my $WMem = Wikia::Memcached->instance;
		my $oMemc = $WMem->memc();
		my $memkey = sprintf( "perl:onedot:langcode:%s", $self->params->{'lc'} );	
		
		# load from memcache
		$lang = $oMemc->get( $memkey );

		unless ( $lang ) {
			# load from database
			$self->_build_dbc();
			if ( $self->dbc ) {
				my $city_lang = $self->dbc->get_lang_by_code( $self->params->{'lc'} );
				$lang = int($city_lang->{lang_id});
				if ( $lang ) {
					$oMemc->set( $memkey, $lang, 5 * 60 * 60 );
				}
			}
		}
	}
	
	$lang = 75 unless ( $lang ) ;
	$self->lang($lang);
	
	return $lang;
}

sub _put_local_db {
	my $self = shift;
	my ($table, $mkey, $value) = @_;

	# Return immediately if we weren't given a proper value
	if (ref $value ne 'HASH') {
		note "Invalid record";
		return 0;
	}

	my $res = 1;
	
	# open the database
	my $path = $self->db_path.'/'.$table;
	my $hdb = TokyoCabinet::HDB->new();
	
	note "== Creating '$path' ==" unless -e $path;
	if ( ! $hdb->open( $path, $hdb->OWRITER | $hdb->OCREAT ) ) {
		my $ecode = $hdb->ecode();
		note sprintf( "open %s error: %s", $path, $hdb->errmsg($ecode) );
		return 0;
	}

	# retrieve records	
	my $data = $hdb->get( $mkey );
	my $record = defined $data ? Wikia::Utils->json_decode($data) : {};
	$record = {} unless ref($record) eq 'HASH';

	foreach my $key ( keys %$value ) {
		my $val = $value->{$key};
		if ( $key =~ s/^\+// ) {
			$record->{$key} ||= 0;
			$record->{$key} += int($val);
		} elsif ( $key =~ s/^\<// ) {
			$record->{$key} ||= '';
			$record->{$key} = $val if ( $record->{$key} lt $val );
		} elsif ( $key =~ s/^\>// ) {
			$record->{$key} ||= '';
			$record->{$key} = $val if ( $record->{$key} gt $val );
		} else {
			$record->{$key} ||= '';
			$record->{$key} = $val;
		}
	}

	# encode 
	$data = Wikia::Utils->json_encode($record);

	# store records
	if ( $data ) {
		if ( ! $hdb->put( $mkey, $data )  ) {
			my $ecode = $hdb->ecode();
			note sprintf( "put error: %s, data: %s", $hdb->errmsg($ecode), $data);
			$res = 0;
		}
	}
		
	if(!$hdb->close()){
		my $ecode = $hdb->ecode();
		note sprintf ("close error: %s", $hdb->errmsg($ecode));
		$res = 0;
	}

	if ( $res == 0 ) {
		note "## Write error: removing '$path' ##";
		unlink ($path);
	}
 	
 	return $res;
}

sub _stats_local_db {
	my ( $self, $table ) = @_;
	
	my $res = 1;
	my $record = {};
	my $hdb = TokyoCabinet::HDB->new();
	
	# open the database
	my $path = sprintf( "%s/%s", $self->db_path, $table );

	note "== Creating '$path' ==" unless -e $path;
	if ( ! $hdb->open( $path, $hdb->OWRITER | $hdb->OCREAT ) ) {
		my $ecode = $hdb->ecode();
		note sprintf( "open %s error: %s", $path, $hdb->errmsg($ecode) );
		$hdb->close();
		$res = 0;
	} else {
		note sprintf( "Table %s: size: %0d, records: %0d", $table, $hdb->fsiz(), $hdb->rnum() );
		$res = $hdb->rnum();
		
		if(!$hdb->close()){
			my $ecode = $hdb->ecode();
			note sprintf ("close error: %s\n", $hdb->errmsg($ecode));
			$res = 0;
		}
	}
	
	if ( $res == 0 ) {
		note "## Open error: removing '$path' ##";
		unlink($path);
	}

 	return $res;
}

sub save_in_memory {
	my $self = shift;
	my ($mkey, $id, $value) = @_;	
	my $store = $self->{data}->{$mkey}->{$id} ||= {};

	foreach my $key ( keys %$value ) {
		my $val = $value->{$key};
		if ( $key =~ /\+/ ) {
			$store->{$key} ||= 0;
			$store->{$key} += int($val);
		} elsif ( $key =~ /\</ ) {
			$store->{$key} ||= '';
			$store->{$key} = $val if $store->{$key} lt $val;
		} elsif ( $key =~ /\>/ ) {
			$store->{$key} ||= '';
			$store->{$key} = $val if $store->{$key} gt $val;
		} else {
			$store->{$key} ||= '';
			$store->{$key} = $val;
		}
	}
	
	return 1;	
}

sub _collect_wikia {
	my $self = shift;

	# primary key
	my $id = sprintf( "%d_%d", $self->params->{'c'}, $self->day );

	# save to local key/value db
	my $value = {
		'+cnt' 		=> 1,
		'day'		=> $self->day,
		'<lastview'	=> $self->params->{'lv'}
	};

	return $self->save_in_memory('wikia', $id, $value);
}

sub _collect_articles {
	my $self = shift;

	# primary key
	my $id = sprintf( "%d_%d_%d", $self->params->{'c'}, $self->params->{'a'}, $self->day );

	# save to local key/value db
	my $value = {
		'+cnt' 		=> 1,
		'day'		=> $self->day,
		'namespace' => $self->params->{'n'},
		'<lastview'	=> $self->params->{'lv'}
	};

	return $self->save_in_memory('page', $id, $value);
}

sub _collect_namespaces {
	my $self = shift;

	# primary key
	my $id = sprintf( "%d_%d_%d", $self->params->{'c'}, $self->params->{'n'}, $self->day );

	# save to local key/value db
	my $value = {
		'+cnt' 		=> 1,
		'lang'		=> $self->lang,
		'<lastview'	=> $self->params->{'lv'}
	};

	return $self->save_in_memory('namespace', $id, $value);
}

sub _collect_users {
	my $self = shift;
	
	if ( $self->params->{'u'} == 0 ) {
		# don't collect data for anons users
		return 1;
	}
		
	# primary key
	my $id = sprintf( "%d_%d_%d_%d", $self->params->{'c'}, $self->params->{'u'}, $self->params->{'a'}, $self->day );

	# save to local key/value db
	my $value = {
		'namespace'	=> $self->params->{'n'},
		'+cnt' 		=> 1,
		'<lastview'	=> $self->params->{'lv'}
	};
	
	return $self->save_in_memory('users', $id, $value);
}

sub _collect_tags {
	my $self = shift;
	
	$self->build_tags();
	
	if ( scalar keys %{$self->tags} == 0 ) {
		note "No tags found for Wiki: " . $self->params->{'c'} if $self->debug;
		return 1;
	} 
	
	my $res = 0;
	my $loop = 1;
	my $count = scalar keys %{$self->tags};
	foreach my $tag_id ( keys %{$self->tags} ) {
		my $id = sprintf( "%d_%d_%d_%d", $self->params->{'c'}, $tag_id, $self->params->{'n'}, $self->day );

		# save to local key/value db
		my $value = {
			'+cnt' 		=> 1,
			'lang'		=> $self->lang,
			'<lastview'	=> $self->params->{'lv'}
		};
		
		$res += $self->save_in_memory('tags', $id, $value); 
		$loop++;
	}
	 
	return $res;
}

sub _collect_weekly_users {
	my $self = shift;
	
	if ( $self->params->{'u'} == 0 ) {
		# don't collect data for anons users
		return 1;
	}
		
	# primary key
	my $id = sprintf( "%d_%d_%d", $self->params->{'c'}, $self->params->{'u'}, $self->week );

	# save to local key/value db
	my $value = {
		'+cnt' 		=> 1,
		'<lastview'	=> $self->params->{'lv'}
	};

	$self->save_in_memory('weekly_user', $id, $value);
}

sub _collect_weekly_wikia {
	my $self = shift;
		
	# primary key
	my $id = sprintf( "%d_%d", $self->params->{'c'}, $self->week );

	# save to local key/value db
	my $value = {
		'+cnt' 		=> 1,
		'<lastview'	=> $self->params->{'lv'}
	};

	$self->save_in_memory('weekly_wikia', $id, $value);
}

sub _collect_raw {
	my $self = shift;
	
	# Create a line like key=value&key2=value2&...
	my $line = join '&',
			   map { ($_||'').'='.($self->params->{$_}||'') }
			   keys %{ $self->params };

	# Buffer this line in memory
	my $buffer = $self->raw_buffer || [];

	# Make sure to save the buffer reference if its a new array
	$self->raw_buffer($buffer) unless @$buffer > 0;
	
	push @$buffer, $line;

	# See if we need to flush the raw data
	if (scalar(@$buffer) >= $self->max_buffer_size) {
		
		# First check to see if we need to rotate the log, either because
		# its too large or too old.
		if ((-s $self->raw_log_name > $self->max_log_size) ||
                    (time() - $self->raw_log_created > $self->log_rotate_secs)) {
			my $renamed_log = $self->raw_log_name;
			$renamed_log =~ s/\.current$//;
 
			# Runsv seems to kill us randomly but we retain variables; new children forked from
			# a main parent where this object was instanciated?  Dumb. Either way, don't rotate
			# anything if the file doesn't exist.
 			if (-e $self->raw_log_name) {
				note "Rotating '$renamed_log.current' to '$renamed_log'";
				unless (rename($self->raw_log_name, $renamed_log)) {
					note "\t\tError renaming '".$self->raw_log_name."' to '$renamed_log': $!";
					$self->raw_logging(0);
					return 0;
				}
			}
			
			$self->raw_log_name($self->_new_raw_log_name);
		}

		$self->_flush_raw_data();
	}
	return 1;
}

sub _flush_raw_data {
	my $self = shift;

	note "Flushing raw buffer to disk";
	return 1 unless $self->raw_buffer && scalar @{ $self->raw_buffer };

	# Open the raw log for appending
	my $log;
	unless (open($log, '>>', $self->raw_log_name)) {
		note "\t\tError appending to '".$self->raw_log_name."': $!";
		$self->raw_logging(0);
		return 0;
	}
		
	print $log join("\n", @{ $self->raw_buffer });
	close($log);
	
	$self->raw_buffer([]);

	return 1;
}

sub _new_raw_log_name {
	my $self = shift;

	# Return when this is called by the default setter if we don't have raw_logging turned on
	return '' unless $self->raw_logging;

	my @t = localtime();
	
	# First look for a current file
	my $dh;
	unless (opendir($dh, $self->raw_log_dir)) {
		note "\t\tError opening '".$self->raw_log_dir."': $!";
		$self->raw_logging(0);
		return '';
	}
	
	my $file;
	my @dirs = readdir($dh);
	closedir($dh);
	
	foreach $file (sort {$a cmp $b} @dirs) {
		next unless $file =~ /^onedot-(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\.current$/;
		
		# Stop at the first .current file we find
		my $created = DateTime->new(year   => $1,
									month  => $2,
									day    => $3,
									hour   => $4,
									minute => $5,
									second => $6,
								   )->epoch();

		$self->raw_log_created($created);
		note "Writing to existing log file '".$self->raw_log_dir.'/'.$file."'";
		return $self->raw_log_dir.'/'.$file;
	}
	
	# Create a YYYYMMDDhhmmss timestamp
	my $ts = sprintf("%4d%02d%02d%02d%02d%02d", $t[5]+1900, $t[4]+1, @t[3,2,1,0]);
	
	$self->raw_log_created(time);
	note "Writing to new log file '".$self->raw_log_dir."/onedot-$ts.current'";
	return $self->raw_log_dir."/onedot-$ts.current";
}

sub collect {
	my $self = shift;
	
	# wiki ID
	if ( !$self->params->{'c'} ) { 
		note "\tEmpty city Id";
		return 0;
	}
	unless ( $self->params->{'c'} =~ /^[+-]?\d+$/ ) { 
		note "\tInvalid city Id: " . $self->params->{'c'};
		return 0;
	}	
	
	# namespace ID
	if ( !defined $self->params->{'n'} ) {
		note "\tUndefined page namespace";
		return 0;
	}
	unless ( $self->params->{'n'} =~ /^[+-]?\d+$/ ) { 
		note "\tInvalid page namespace: " . $self->params->{'n'};
		return 0;		
	}

	# language code
	if ( !defined $self->params->{'lc'} ) {
		note "\tInvalid language code";
		return 0;
	}

	# article ID
	unless ( $self->params->{'a'} ) {
		$self->params->{'a'} = -1;
	}
	unless ( $self->params->{'a'} =~ /^[+-]?\d+$/ ) { 
		note "\tInvalid article ID: " . $self->params->{'a'};
		return 0;		
	}	
	
	# user ID
	unless ( $self->params->{'u'} ) {
		$self->params->{'u'} = 0;
	}
	unless ( $self->params->{'u'} =~ /^[+-]?\d+$/ ) { 
		note "\tInvalid user ID: " . $self->params->{'u'};
		return 0;		
	}	
	
	# save wikia
	if ( ! $self->_collect_wikia() ) {
		note "\tCannot save pviews per wikia: " . Wikia::Utils->json_encode($self->params);
		return 0;		
	}

	# save articles
	if ( ! $self->_collect_articles() ) {
		note "\tCannot save pviews per articles: " . Wikia::Utils->json_encode($self->params);
		return 0;		
	}	

	$self->build_lang();

	# save namespaces
	if ( !$self->_collect_namespaces() ) {
		note "\tCannot save pviews per namespaces: " . Wikia::Utils->json_encode($self->params);
		return 0;		
	}
	
	# save users
	if ( !$self->_collect_users() ) {
		note "\tCannot save users pviews: " . Wikia::Utils->json_encode($self->params);
		return 0;		
	}	
	
	# save tags
	if ( !$self->_collect_tags() ) {
		note "\tCannot save pviews per tags: " . Wikia::Utils->json_encode($self->params);
		return 0;		
	}	
	
	# save weekly user pv 
	if ( !$self->_collect_weekly_users() ) {
		note "\tCannot save weekly user pv: " . Wikia::Utils->json_encode($self->params);
		return 0;		
	}
	
	# save weekly wikia pv 
	if ( !$self->_collect_weekly_wikia() ) {
		note "\tCannot save weekly wikia pv: " . Wikia::Utils->json_encode($self->params);
		return 0;		
	}	

	# save the raw data
	if ($self->raw_logging) {
		if ( !$self->_collect_raw() ) {
			note "\tFailed to log raw data: " . Wikia::Utils->json_encode($self->params);
			return 0;
		}
	}

	return 1;
}

sub save_mysql {
	my ($self, $data) = @_;
	
	# connect to db 
	return 0 unless $self->dbw;
		
	my $table = "";
	my @columns = ();
	if ( $data->{table} eq $self->user_db_file ) {
		@columns = ( 'pv_city_id', 'pv_user_id', 'pv_page_id', 'pv_namespace', 'pv_use_date', 'pv_views', 'pv_ts' );
		$table = 'page_views_user';
	} 
	elsif ( $data->{table} eq $self->ns_db_file ) {
		@columns = ( 'pv_city_id', 'pv_use_date', 'pv_namespace', 'pv_views', 'pv_city_lang', 'pv_ts' );
		$table = 'page_views';
	} 
	elsif ( $data->{table} eq $self->tags_db_file ) {
		@columns = ( 'city_id', 'tag_id', 'use_date', 'city_lang', 'namespace', 'pv_views', 'ts' );
		$table = 'page_views_tags';
	} 
	elsif ( $data->{table} eq $self->page_db_file ) {
		@columns = ( 'pv_city_id', 'pv_page_id', 'pv_namespace', 'pv_use_date', 'pv_views', 'pv_ts');
		$table = 'page_views_articles';
	}
	elsif ( $data->{table} eq $self->wikia_db_file ) {
		@columns = ( 'pv_city_id', 'pv_use_date', 'pv_views', 'pv_ts');
		$table = 'page_views_wikia';
	}	
	elsif ( $data->{table} eq $self->weekly_user_db_file ) {
		@columns = ( 'pv_city_id', 'pv_user_id', 'pv_week', 'pv_views', 'pv_ts');
		$table = 'page_views_weekly_user';
	}
	elsif ( $data->{table} eq $self->weekly_wikia_db_file ) {
		@columns = ( 'pv_city_id', 'pv_week', 'pv_views', 'pv_ts');
		$table = 'page_views_weekly_wikia';
	}	
	
	note sprintf("insert %0d multi-records into %s table \n", scalar @{$data->{rows}}, $table );
	if ( scalar(@columns) && scalar (@{$data->{rows}}) ) {
		# insert
		foreach my $k ( @{$data->{rows}} ) {
			my $values = join(",", map { $_ } @$k);
			if ( $values ) {
				my $sql = "INSERT IGNORE INTO " . $table. " ( " . join(",", @columns) . " ) values ";
				$sql .= $values;
				$sql .= " ON DUPLICATE KEY UPDATE pv_views = pv_views + values(pv_views) ";
				note $sql . "\n" if ( $self->debug );
				$sql = $self->dbw->execute($sql);
				#usleep(500000);
			}
		}
	}
}

sub make_insert_params {
	my ( $self, $table, $key, $value ) = @_;
	
	my @page = ();
	
	if ( $table eq $self->wikia_db_file ) {
		my ( $wiki, $day ) = split(/\_/, $key);
		@page = ($wiki, $day, $value->{cnt}, $value->{lastview} );
	} 
	elsif ( $table eq $self->page_db_file ) {
		my ( $wiki, $page, $day ) = split(/\_/, $key);
		@page = ($wiki, $page, $value->{namespace}, $day, $value->{cnt}, $value->{lastview} );
	} 
	elsif ( $table eq $self->ns_db_file ) {
		my ( $wiki, $ns, $day ) = split(/\_/, $key);
		@page = ( $wiki, $day, $ns, $value->{cnt}, $value->{lang}, $value->{lastview} );
	}
	elsif ( $table eq $self->user_db_file) {
		my ( $wiki, $user, $page, $day ) = split(/\_/, $key);
		@page = ( $wiki, $user, $page, $value->{namespace}, $day, $value->{cnt}, $value->{lastview} );	
	} 
	elsif ( $table eq $self->tags_db_file ) {
		my ( $wiki, $tag, $ns, $day ) = split(/\_/, $key);
		@page = ( $wiki, $tag, $day, $value->{lang}, $ns, $value->{cnt}, $value->{lastview} );	
	} 
	elsif ( $table eq $self->weekly_user_db_file ) {
		my ( $wiki, $user, $week ) = split(/\_/, $key);
		@page = ( $wiki, $user, $week, $value->{cnt}, $value->{lastview} );	
	}
	elsif ( $table eq $self->weekly_wikia_db_file ) {
		my ( $wiki, $week ) = split(/\_/, $key);
		@page = ( $wiki, $week, $value->{cnt}, $value->{lastview} );	
	}
		
	return \@page;
}

sub _save_from_local_db {
	my ($self, $table) = @_;
	
	my $data = [];
	my $y = 0;
	my $x = 0;
	my $hdb = TokyoCabinet::HDB->new();
	
	# open the database
	my $path = sprintf( "%s/%s", $self->db_path, $table );
	note sprintf("Parse table %s", $table );

	note "== Creating '$path' ==" unless -e $path;	
	if ( ! $hdb->open( $path, $hdb->OWRITER | $hdb->OCREAT ) ) {
		my $ecode = $hdb->ecode();
		note sprintf( "open error: %s", $hdb->errmsg($ecode) );
		return 0;
	}
	
	$self->_build_dbw();
	
	$hdb->iterinit();
	while( defined( my $key = $hdb->iternext() ) ) {
		my $value = $hdb->get($key);
		if ( defined($value) ) {
			my $record = Wikia::Utils->json_decode($value);
			next if ( !defined $record ) ;
			next if ( !UNIVERSAL::isa($record,'HASH') );
					
			# make insert params
			my $page = $self->make_insert_params($table, $key, $record);
			# loop of inserts;
			next unless $page;
			#
			$data->[$y] = [] unless ( $data->[$y] );
			$y++ if ( ( $x > 0 ) && ( scalar ( @{$data->[$y]} )  % $self->insert ) == 0 ) ;
			
			# push in data array
			my $row = join( ',', map { $self->dbw->quote($_) } @$page );
			if ( $row ) {
				push @{$data->[$y]} , "(" . $row . ")";
				$x++;
			}
			#
			undef($page);
			undef($record);
		}
	}
	
	# remove all records;
	$hdb->vanish();
	
	# close the database
	if ( !$hdb->close() ) {
		note "close error";
	}	
	
	note "## Removing file '$path' ##";
	unlink($path);
	
	# save to db
	note sprintf("Found %d records (from %s table) to insert to db", scalar @$data, $table);
	
	if ( scalar @$data == 0 ) {
		note "No records to move ";
		return 1;
	}
	
	note "data = " . Dumper(@$data) if ( $self->debug );
	
	my $res = { 'rows' => $data, 'table' => $table };
	$self->save_mysql($res);
	
	return 1;
}

sub _read_wikia {
	my $self = shift;
	
	return $self->_save_from_local_db( $self->wikia_db_file );
}

sub _read_articles {
	my $self = shift;
	
	return $self->_save_from_local_db( $self->page_db_file );
}

sub _read_namespaces {
	my $self = shift;
	
	return $self->_save_from_local_db( $self->ns_db_file );
}

sub _read_users {
	my $self = shift;
	
	return $self->_save_from_local_db( $self->user_db_file );
}

sub _read_tags {
	my $self = shift;
	
	return $self->_save_from_local_db( $self->tags_db_file );
}

sub _read_weekly_users {
	my $self = shift;
	
	return $self->_save_from_local_db( $self->weekly_user_db_file );
}

sub _read_weekly_wikia {
	my $self = shift;
	
	return $self->_save_from_local_db( $self->weekly_wikia_db_file );
}

sub _exec_script {
	my ($self, $type, $table) = @_;

	my $prefix = time();
	
	my $path = sprintf( "%s/%s", $self->db_path, $table );
	my $new_tch = sprintf("%s_%s", $prefix, $table);
	my $backup_path = sprintf( "%s/%s", $self->db_path, $new_tch );	
	note sprintf("Move table %s to %s", $path, $new_tch);
	my @copy = ( $path, $backup_path );
	system 'cp', @copy;
	#my $cmd = "/usr/bin/perl " . $FindBin::Bin . "/onedot.pl --option=move --file=$new_tch --table=$type >> /tmp/onedot_move.log";
	#my $cmd = "/usr/bin/perl " . $FindBin::Bin . "/../fixes/onedot_fix.pl";
	#note "run $cmd";
	#system "$cmd &";
	note "## Removing '$path' ##";
	unlink($path);	
}

sub prepare_to_move {
	my $self = shift;

	# wikia
	$self->_exec_script('wikia', $self->wikia_db_file);
	
	# articles
	$self->_exec_script('articles', $self->page_db_file);
	
	# namespaces
	$self->_exec_script('namespace', $self->ns_db_file);
	
	# users
	$self->_exec_script('users', $self->user_db_file);

	# tags
	$self->_exec_script('tags', $self->tags_db_file);
	
	# weekly users
	$self->_exec_script('weekly_users', $self->weekly_user_db_file);
	
	# weekly wikia
	$self->_exec_script('weekly_wikia', $self->weekly_wikia_db_file);	
	
	# run script to move it to database
	my $cmd = "/usr/bin/perl " . $FindBin::Bin . "/../checkPID.pl --script=\"" . $FindBin::Bin . "/../fixes/onedot_fix.pl\"";
	note "run $cmd";
	system "$cmd";
	note "done";
}

sub show_stats {
	my $self = shift;
	
	$self->_stats_local_db( $self->wikia_db_file );
	$self->_stats_local_db( $self->ns_db_file );
	$self->_stats_local_db( $self->user_db_file );	
	$self->_stats_local_db( $self->tags_db_file );
	$self->_stats_local_db( $self->weekly_user_db_file );
	$self->_stats_local_db( $self->weekly_wikia_db_file );
	
	# number of records for this table
	my $records = $self->_stats_local_db( $self->page_db_file );
	
	return $records;	
}

sub check_is_parsed {
	my $self = shift;
	
	my $WMem = Wikia::Memcached->instance;
	my $oMemc = $WMem->memc();
	my $memkey = "perl:onedot:package";
	my $data = sprintf("%d_%d_%s_%d", int($self->params->{'n'}), int($self->params->{'a'}), $self->params->{'lv'}, int($self->params->{'c'}));

	# load from memcache
	my $res = $oMemc->get( $memkey ) if ( $oMemc );
	my $exists = ( defined $res ) ? $res eq $data : 0;

	$oMemc->set( $memkey, $data, 60 * 60 );
	
	note "This package was".($exists ? "" : " not ")."parsed few minutes ago ";
	return $exists;
}

sub db_filename {
	my $self = shift;
	my ($mkey) = @_;

	# Map the mkey value to the DB filename
	if    ( $mkey eq 'wikia' )        { return $self->wikia_db_file }
	elsif ( $mkey eq 'page' )         { return $self->page_db_file }
	elsif ( $mkey eq 'namespace' )    { return $self->ns_db_file }
	elsif ( $mkey eq 'users' )        { return $self->user_db_file }
	elsif ( $mkey eq 'tags' )         { return $self->tags_db_file }
	elsif ( $mkey eq 'weekly_user' )  { return $self->weekly_user_db_file}
	elsif ( $mkey eq 'weekly_wikia' ) { return $self->weekly_wikia_db_file }
}

sub write_to_local_db {
	my $self = shift;
 
 	# Don't wait around for child processes created here
 	$SIG{CHLD} = "IGNORE";
 
	# Fork this off to process asynchronously.  Return the PID if we're the parent
	my $pid = fork();
	return $pid if $pid;

	foreach my $mkey ( keys %{$self->{data}} ) {
		my $records = $self->{data}->{$mkey};
		my $num = scalar keys %$records;

		note "Found $num records in $mkey";
		next unless $num > 0;

		my $db_name = $self->db_filename($mkey);

		# save in local DB
		foreach my $id ( keys %$records ) {
			my $value = $records->{$id};
			$self->_put_local_db($db_name, $id, $value);
		}
	}

	# display local DB stats
	note "Show local db stats ";
	my $records = $self->show_stats();
	
	if ( $records > 500000 ) {
		note "Move data from local DB to Mysql";
		my $response = $self->prepare_to_move();
	}

	# We're the child and we're done
	exit(0);
}

our ($start_period, $TOTAL, $TOTAL_PROCESSED);

sub Log {
	my ($self, $messages) = @_;

	# Initialize these value if they haven't been set yet
	unless ($start_period) {
		$start_period = get_start_period();
		$TOTAL = 0;
		$TOTAL_PROCESSED = 0;
	}

	# check time
	my $process_start_time = time();
	
	# default result;
	my $ok = 1;
	my ($processed, $notfound, $invalid) = (0, 0, 0);

	my $loop = 1;
	if ( defined($messages) && UNIVERSAL::isa($messages,'ARRAY') ) {
		$self->_build_day();
		$self->_build_week();
		
		note "Number of messages: " . scalar @$messages;
		# inserts array 
		my $records = {};
		my $data = {};
		# get messages from Scribe
		$self->data($data);
		foreach ( @$messages ) {
			$TOTAL++;

			# from scribe
			my $s_key = $_->{category};
			my $s_msg = $_->{message};

			note sprintf("\t%d. %s: %s", $loop, $s_key, $s_msg) if ( $self->debug );

			# decode message
			my $oMW = undef;
			
			eval {
				$oMW = Wikia::Utils->json_decode( $_->{message} );
			};
			
			# check response
			if ( UNIVERSAL::isa($oMW, 'HASH') && ( $s_key =~ /^(?:log_view|onedot)$/ ) ) { 
				# check method name
				if ( defined $oMW->{method} ) {
					# log
					note sprintf("\t%d. %s: %s", $loop, $s_key, $oMW->{method}) if ( $self->debug );
					
					# instance of data;
					$self->_build_params($oMW->{params});
					
					# Use the first message to determine if we've seen this set of messages before
					if ( $loop == 1 ) {
						note "Check record was parsed ";
						# We store the unique values of the first message in memcached.  If we
						# retrieve the last set of values and they are the same as this set,
						# we'll stop and return now.
						if ( $self->check_is_parsed() ) {
							my $response = $self->prepare_to_move();
							note "This package was parsed few minutes ago ";
							return Scribe::Thrift::ResultCode::OK();
						}
					}
					
					# call method 
					my $response = 0;
					if ( $oMW->{method} eq 'collect' ) {
						note "Collect data in local DB" if ( $self->debug );
						$response = $self->collect();						
					} elsif ( $oMW->{method} eq 'move' ) {
						note "Collect data in local DB";
						$response = $self->collect();						
						note "Move data from local DB to Mysql";
						$response = $self->prepare_to_move();
					}
					
					if ( $response == 0 ) {
						$invalid++;
					} else {
						# 
						$TOTAL_PROCESSED++;
						$processed++;
					}
				} else {
					$notfound++;
				}		
				
				$loop++;
			}
		}

		my $num_rows = scalar @$messages;		

		if (($num_rows > 0) && (scalar keys %{$self->{data}})) {
			$self->write_to_local_db;
		}

		# Reconnect to the DB after processing each set of messages
		$self->dbc(undef);
	}
	
	my $process_end_time = time();
	my @ts = gmtime($process_end_time - $process_start_time);
	
	note sprintf("result: %0d records, %0d invalid messages", $processed, $invalid );
	note "$loop messages processed: " . sprintf ("%d hours %d minutes %d seconds", @ts[2,1,0]);

	note "\tServed $HIT of $TOT tag requests from memory.";
	note sprintf("\tMemory tag cache: Entries: %d / Hit rate: %.1f", scalar keys %{$LRU->{_entries}}, 100*($HIT/$TOT)).'%' if $TOT;

	note "ok = $ok \n" if ( $self->debug );

	# Note how many messages we've processed every 15min
	if (time-$start_period > 900) {
		my @t  = localtime($start_period);
		my @t2 = localtime($start_period+900);
		printf("%02d:%02d - %02d:%02d : %d received, %d sent, %d dropped\n", @t[2,1], @t2[2,1], $TOTAL, $TOTAL_PROCESSED, $TOTAL-$TOTAL_PROCESSED);
		$start_period = get_start_period();
		$TOTAL = 0;
		$TOTAL_PROCESSED = 0;
	}


	# update log #bugid: 6713
	if ( $ok ) {
		my $log = Wikia::Log->new( name => "viewsd" );
		$log->update();
	}
			
	#return ($ok) ? Scribe::Thrift::ResultCode::OK : Scribe::Thrift::ResultCode::TRY_LATER;	
	return Scribe::Thrift::ResultCode::OK();
}

sub get_start_period {
	my $t = time;

	# Return the most recent 15min period
	return $t-($t % 900);
}

DESTROY {
	my $self = shift;
	
	# Make sure we write out our buffer before destroying ourselves
	if ($self->raw_logging) {
		$self->_flush_raw_data();
	}
}

no Moose;
1;
