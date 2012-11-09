package Wikia::EventFixStats;
use strict;
use base qw(Class::Accessor);

use Switch;
use DateTime;
use Data::Dumper;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use Wikia::Utils;
use Wikia::LB;
use Wikia::DB;

our $YML = undef;
$YML = "$Bin/../../../wikia-conf/DB.moli.yml" if -e "$Bin/../../../wikia-conf/DB.moli.yml" ;

use constant MEDIATYPE_BITMAP 		=> 1;
use constant MEDIATYPE_DRAWING		=> 2;
use constant MEDIATYPE_AUDIO		=> 3;
use constant MEDIATYPE_VIDEO		=> 4;
use constant MEDIATYPE_MULTIMEDIA	=> 5;
use constant MEDIATYPE_OFFICE		=> 6;
use constant MEDIATYPE_TEXT			=> 7;
use constant MEDIATYPE_EXECUTABLE	=> 8;
use constant MEDIATYPE_ARCHIVE		=> 9;

use constant EDIT_CATEGORY 			=> 1; 
use constant CREATEPAGE_CATEGORY 	=> 2; 
use constant DELETE_CATEGORY		=> 3;
use constant UNDELETE_CATEGORY		=> 4; 
use constant UPLOAD_CATEGORY		=> 5; 

use constant WIKIA_TYPE				=> 'wikia';
use constant LANG_TYPE				=> 'language';
use constant CAT_TYPE				=> 'category';
use constant CAT_LANG_TYPE			=> 'category_language';
use constant SUMMARY_TYPE			=> 'summary';

__PACKAGE__->mk_accessors(qw/type params/);

sub new {
    my ($class, @args) = @_;
    my $self  = $class->SUPER::new;

    $self->params( @args );

    bless $self, $class;
}

sub aggregate_stats($;$$$) {
	my ($self, $filter, $filter_y, $summary_stats) = @_;

	return undef unless ( $self->params->{type} );
	return undef unless ( $self->params->{start_date} );
	return undef unless ( $self->params->{end_date} );

	my $months = Wikia::Utils->month_between_dates($self->params->{start_date}, $self->params->{end_date});

	my $databases = {};
	print "Prepare rows (per " . $self->params->{type} . ") to execute\n";
	if ( $self->params->{type} eq WIKIA_TYPE ) { 
		$databases = $self->__get_databases($filter);
	}
	elsif ( $self->params->{type} eq LANG_TYPE ) { 
		$databases = $self->__get_languages($filter, 0);
	} 
	elsif ( $self->params->{type} eq CAT_TYPE ) {
		$databases = $self->__get_categories($filter, 0);
	}
	elsif ( $self->params->{type} eq CAT_LANG_TYPE ) {
		$databases = $self->__get_categories_language($filter, $filter_y, 0);
	}
	elsif ( $self->params->{type} eq SUMMARY_TYPE ) { 
		$databases = { '-1' => 'summary' };
	}

	my $result = {};
	print sprintf ("%0d dates and %0d records found\n", scalar @$months, scalar keys %$databases);
	if ( ( scalar @$months ) && ( scalar keys %$databases ) ) {
		foreach ( @$months ) {
			print "month = " . $_."\n";
			my $stats = undef;
			if ( defined($summary_stats) && defined($summary_stats->{$_}) ) {
				#
				if ( $self->params->{type} eq LANG_TYPE ) { 
					$stats = $summary_stats->{$_}->{languages};
				} 
				elsif ( $self->params->{type} eq CAT_TYPE ) {
					$stats = $summary_stats->{$_}->{categories};
				}
				elsif ( $self->params->{type} eq CAT_LANG_TYPE ) {
					$stats = $summary_stats->{$_}->{catlang};
				}
				elsif ( $self->params->{type} eq SUMMARY_TYPE ) { 
					$stats = $summary_stats->{$_}->{all};
				}
			}
			$result->{$_} = $self->__make_stats($databases, $_, $stats);
		}
	}
	
	return $result;
}

sub __make_stats($;$$$) {
	my ($self, $databases, $month, $stats) = @_;

	my $records = scalar keys %{$databases};
	my $summary_stats = {
		'categories' => {
			'articles'		=> {},
			'images'		=> {},
			'imagelinks'	=> {},
			'video'			=> {},
			'videolinks'	=> {},
			'newpages'		=> {}
		},
		'languages' => {
			'articles'		=> {},
			'images'		=> {},
			'imagelinks'	=> {},
			'video'			=> {},
			'videolinks'	=> {},
			'newpages'		=> {}
		},
		'catlang' => {
			'articles'		=> {},
			'images'		=> {},
			'imagelinks'	=> {},
			'video'			=> {},
			'videolinks'	=> {},
			'newpages'		=> {}
		},
		'all' 		=> {
			'articles'		=> 0,
			'images'		=> 0,
			'imagelinks'	=> 0,
			'video'			=> 0,
			'videolinks'	=> 0,
			'newpages'		=> 0			
		},
		'records'	=> $records
	};
	
	foreach my $num ( ($self->params->{type} eq CAT_LANG_TYPE ) ? sort keys %{$databases} : sort ( map { sprintf("%012u",$_) } ( keys %{$databases} ) ) ) {
		#--- set city;
		my $id = ($self->params->{type} eq CAT_LANG_TYPE ) ? $num : int $num;
		#--- set start time
		my $start_sec = time();
		#--- month
		$month = ( $month ) ? $month : DateTime->now()->strftime("%Y%m");
		print sprintf( "Proceed %s (%d) (%d) \n", $databases->{$id}, $id, $month );

		my $row = { 'id' => $id, 'month' => $month };
		my $data_stats = $self->__row_stats($row, $stats); 
		if ( $self->params->{type} eq WIKIA_TYPE ) {
			my $lb = Wikia::LB->instance;
			$lb->yml( $Wikia::EventFixStats::YML ) if ($Wikia::EventFixStats::YML);
			my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED )} );		
			# category	
			my $cat = $dbr->get_wiki_cat($id);
			# language
			my $lang = $dbr->get_wiki_lang($id);
			print "Category for Wiki: $cat, language for Wiki: $lang \n";
			if ( $cat && $lang) {
				#
				$summary_stats->{categories}->{articles}->{$cat} = 
					Wikia::Utils->intval($summary_stats->{categories}->{articles}->{$cat}) + Wikia::Utils->intval($data_stats->{articles}->{all});
				$summary_stats->{categories}->{images}->{$cat} = 
					Wikia::Utils->intval($summary_stats->{categories}->{images}->{$cat}) + Wikia::Utils->intval($data_stats->{media}->{imageupload});
				$summary_stats->{categories}->{imagelinks}->{$cat} = 
					Wikia::Utils->intval($summary_stats->{categories}->{imagelinks}->{$cat}) + Wikia::Utils->intval($data_stats->{media}->{imagelinks});
				$summary_stats->{categories}->{video}->{$cat} = 
					Wikia::Utils->intval($summary_stats->{categories}->{video}->{$cat}) + Wikia::Utils->intval($data_stats->{media}->{videoupload});
				$summary_stats->{categories}->{videolinks}->{$cat} = 
					Wikia::Utils->intval($summary_stats->{categories}->{videolinks}->{$cat}) + Wikia::Utils->intval($data_stats->{media}->{videoembeded});
				$summary_stats->{categories}->{newpages}->{$cat} = 
					Wikia::Utils->intval($summary_stats->{categories}->{newpages}->{$cat}) + Wikia::Utils->intval($data_stats->{articles}->{newday});					
				#
				$summary_stats->{languages}->{articles}->{$lang} = 
					Wikia::Utils->intval($summary_stats->{languages}->{articles}->{$lang}) + Wikia::Utils->intval($data_stats->{articles}->{all});
				$summary_stats->{languages}->{images}->{$lang} = 
					Wikia::Utils->intval($summary_stats->{languages}->{images}->{$lang}) + Wikia::Utils->intval($data_stats->{media}->{imageupload});
				$summary_stats->{languages}->{imagelinks}->{$lang} = 
					Wikia::Utils->intval($summary_stats->{languages}->{imagelinks}->{$lang}) + Wikia::Utils->intval($data_stats->{media}->{imagelinks});
				$summary_stats->{languages}->{video}->{$lang} = 
					Wikia::Utils->intval($summary_stats->{languages}->{video}->{$lang}) + Wikia::Utils->intval($data_stats->{media}->{videoupload});
				$summary_stats->{languages}->{videolinks}->{$lang} = 
					Wikia::Utils->intval($summary_stats->{languages}->{videolinks}->{$lang}) + Wikia::Utils->intval($data_stats->{media}->{videoembeded});
				$summary_stats->{languages}->{newpages}->{$lang} = 
					Wikia::Utils->intval($summary_stats->{languages}->{newpages}->{$lang}) + Wikia::Utils->intval($data_stats->{articles}->{newday});						
				#
				$summary_stats->{catlang}->{articles}->{$cat}->{$lang} = 
					Wikia::Utils->intval($summary_stats->{catlang}->{articles}->{$cat}->{$lang}) + Wikia::Utils->intval($data_stats->{articles}->{all});
				$summary_stats->{catlang}->{images}->{$cat}->{$lang} = 
					Wikia::Utils->intval($summary_stats->{catlang}->{images}->{$cat}->{$lang}) + Wikia::Utils->intval($data_stats->{media}->{imageupload});
				$summary_stats->{catlang}->{imagelinks}->{$cat}->{$lang} = 
					Wikia::Utils->intval($summary_stats->{catlang}->{imagelinks}->{$cat}->{$lang}) + Wikia::Utils->intval($data_stats->{media}->{imagelinks});
				$summary_stats->{catlang}->{video}->{$cat}->{$lang} = 
					Wikia::Utils->intval($summary_stats->{catlang}->{video}->{$cat}->{$lang}) + Wikia::Utils->intval($data_stats->{media}->{videoupload});
				$summary_stats->{catlang}->{videolinks}->{$cat}->{$lang} = 
					Wikia::Utils->intval($summary_stats->{catlang}->{videolinks}->{$cat}->{$lang}) + Wikia::Utils->intval($data_stats->{media}->{videoembeded});
				$summary_stats->{catlang}->{newpages}->{$cat}->{$lang} = 
					Wikia::Utils->intval($summary_stats->{catlang}->{newpages}->{$cat}->{$lang}) + Wikia::Utils->intval($data_stats->{articles}->{newday});						
				#
				$summary_stats->{all}->{articles} = 
					Wikia::Utils->intval($summary_stats->{all}->{articles}) + Wikia::Utils->intval($data_stats->{articles}->{all});
				$summary_stats->{all}->{images} = 
					Wikia::Utils->intval($summary_stats->{all}->{images}) + Wikia::Utils->intval($data_stats->{media}->{imageupload});
				$summary_stats->{all}->{imagelinks} = 
					Wikia::Utils->intval($summary_stats->{all}->{imagelinks}) + Wikia::Utils->intval($data_stats->{media}->{imagelinks});
				$summary_stats->{all}->{video} = 
					Wikia::Utils->intval($summary_stats->{all}->{video}) + Wikia::Utils->intval($data_stats->{media}->{videoupload});
				$summary_stats->{all}->{videolinks} = 
					Wikia::Utils->intval($summary_stats->{all}->{videolinks}) + Wikia::Utils->intval($data_stats->{media}->{videoembeded});
				$summary_stats->{all}->{newpages} = 
					Wikia::Utils->intval($summary_stats->{all}->{newpages}) + Wikia::Utils->intval($data_stats->{articles}->{newday});						
				#		
			}
			$dbr->disconnect() if ($dbr);
		}
		undef($row);

		my $end_sec = time();
		my @ts = gmtime($end_sec - $start_sec);
		print $databases->{$id} . " processed " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);
	}
	
	return $summary_stats;
}

sub __row_stats($;$$) {
	my ($self, $row, $stats) = @_;
	#city, month, cnt

	return 0 unless ( $row->{id} );
	return 0 unless ( $row->{month} );

	$row->{month_from} = Wikia::Utils->first_datetime( $row->{month} );
	$row->{month_to} = Wikia::Utils->last_datetime( $row->{month} );

	my $lb = Wikia::LB->instance;
	$lb->yml( $YML ) if ($YML);

	# make summary stats
	my $res = {
  		'editors'	=> { 'all' => 0, 'content' => 0, '5times' => 0, '100times' => 0 },
		'articles' 	=> { 'all' => 0, 'newday' => 0, 'edits' => 0 },
		'media'		=> { 'imagelinks' => 0, 'videoembeded' => 0, 'imageupload' => 0, 'videoupload' => 0 }
	};

	# db handle 
	my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::STATS )} );

	# all editors stats ==============================
	# 'editors' => 'all'
	#$res->{editors}->{all} = $self->__users_all_ns($dbs, $row);

	# 'editors' => 'content'
	#$res->{editors}->{content} = $self->__users_content_ns($dbs, $row);

	# 'editors' => '5times'
	#$res->{editors}->{'5times'} = $self->__users_content_ns_5times($dbs, $row);

	# 'editors' => '100times'
	#$res->{editors}->{'100times'} = $self->__users_content_ns_100times($dbs, $row);

	#=article stats ==============================
	# 'articles' => 'all'
	#$res->{articles}->{all} =  $self->__articles_all($dbs, $row, $stats);

	# 'articles' => 'newday'
	$res->{articles}->{newday} = $self->__articles_new($dbs, $row, $stats);

	# 'articles' => 'edits'
	#$res->{articles}->{edits} = $self->__articles_edits($dbs, $row);

	#=media stats ==============================
	# 'media' => 'imagelinks'
	#$res->{media}->{imagelinks} = $self->__media_imagelinks($dbs, $row, $stats);

	# 'media' => 'videoembeded'
	#$res->{media}->{videoembeded} = $self->__media_videolinks($dbs, $row, $stats);

	# 'media' => 'image upload'
	#$res->{media}->{imageupload} = $self->__media_imageupload($dbs, $row, $stats);

	# 'media' => 'video upload'
	#$res->{media}->{videoupload} = $self->__media_videoupload($dbs, $row, $stats);

	#$dbs->disconnect if ($dbs);

	my $ins = 0;

	my $table = "";
	my $key_field = "";
	if ( $self->params->{type} eq WIKIA_TYPE ) { 
		$table = "wikia_monthly_stats";
		$key_field = "wiki_id";
	}
	elsif ( $self->params->{type} eq LANG_TYPE ) { 
		$table = "lang_monthly_stats";
		$key_field = "wiki_lang_id";
	} 
	elsif ( $self->params->{type} eq CAT_TYPE ) {
		$table = "cat_monthly_stats";
		$key_field = "wiki_cat_id";
	}
	elsif ( $self->params->{type} eq CAT_LANG_TYPE ) {
		$table = "cat_lang_monthly_stats";
		my @key = split(/\:/, $row->{id});
		$key_field = { "wiki_cat_id" => $key[0], "wiki_lang_id" => $key[1] };
	}
	elsif ( $self->params->{type} eq SUMMARY_TYPE ) { 
		$table = "summary_monthly_stats";
	}

	if ( $table ne '' ) {
		my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, 'stats', Wikia::LB::STATS )} );
		if ( $dbw ) {
		
			# delete all stats
			my @conditions = (
				"stats_date = " . $dbw->quote($row->{month})
			);
			if ( $key_field ) {
				if ( UNIVERSAL::isa($key_field, 'HASH') ) {
					foreach my $column ( keys %$key_field ) {
						push @conditions, "$column = " . $dbw->quote($key_field->{$column}); 
					}
				} else {
					push @conditions, "$key_field = ". $dbw->quote($row->{id}) 
				}
			}
			
			my %data = (
				#"stats_date"		=> Wikia::Utils->intval($row->{month}),
				#"users_all"			=> Wikia::Utils->intval($res->{editors}->{all}),
				#"users_content_ns"	=> Wikia::Utils->intval($res->{editors}->{content}),
				#"users_5times"		=> Wikia::Utils->intval($res->{editors}->{'5times'}),
				#"users_100times"	=> Wikia::Utils->intval($res->{editors}->{'100times'}),
				#"articles"			=> Wikia::Utils->intval($res->{articles}->{all}),
				"articles_daily"	=> Wikia::Utils->intval($res->{articles}->{newday})
				#"articles_edits"	=> Wikia::Utils->intval($res->{articles}->{edits}),
				#"images_links"		=> Wikia::Utils->intval($res->{media}->{imagelinks}),
				#"images_uploaded"	=> Wikia::Utils->intval($res->{media}->{imageupload}),
				#"video_links"		=> Wikia::Utils->intval($res->{media}->{videoembeded}),
				#"video_uploaded"	=> Wikia::Utils->intval($res->{media}->{videoupload})
			);
			
			my $q = $dbw->update($table, \@conditions, \%data);
			
			#$dbw->disconnect if ($dbw);
		}
	}

	return $res;
}

sub __users_all_ns($$$) {
	my ($self, $dbs, $row) = @_;
	# Registered editors
	# A: Total registered editors in current month (all namespaces)

	my $where = [];
	my $options = [];
	switch ( $self->params->{type} ) {
		case WIKIA_TYPE { push @$where, "wiki_id = " . $dbs->quote($row->{id}); }
		case LANG_TYPE 	{ push @$where, "wiki_lang_id = " . $dbs->quote($row->{id}); }
		case CAT_TYPE 	{ push @$where, "wiki_cat_id = " . $dbs->quote($row->{id}); }
		case CAT_LANG_TYPE {
			my @key = split(/\:/, $row->{id});
			push @$where, "wiki_cat_id = " . $dbs->quote($key[0]);
			push @$where, "wiki_lang_id = " . $dbs->quote($key[1]);
		}
	}
	
	push @$where, "rev_timestamp between " . $dbs->quote($row->{month_from}) . " and " . $dbs->quote($row->{month_to});
	push @$where, "( event_type = " . $self->EDIT_CATEGORY . " or event_type = " . $self->CREATEPAGE_CATEGORY . ")";
	push @$where, "user_id != 0";
	
	my $oRow = $dbs->select("count(distinct(user_id)) as cnt", 'events', $where, $options);

	return $self->__make_value($oRow);
}

sub __users_content_ns($$$) {
	my ($self, $dbs, $row) = @_;
	# Registered editors
	# B: Total registered editors in current month (content namespaces)
	
	my $where = [];
	my $options = [];
	switch ( $self->params->{type} ) {
		case WIKIA_TYPE { push @$where, "wiki_id = " . $dbs->quote($row->{id}); }
		case LANG_TYPE 	{ push @$where, "wiki_lang_id = " . $dbs->quote($row->{id}); }
		case CAT_TYPE 	{ push @$where, "wiki_cat_id = " . $dbs->quote($row->{id}); }
		case CAT_LANG_TYPE {
			my @key = split(/\:/, $row->{id});
			push @$where, "wiki_cat_id = " . $dbs->quote($key[0]);
			push @$where, "wiki_lang_id = " . $dbs->quote($key[1]);
		}		
	}
	
	push @$where, "rev_timestamp between " . $dbs->quote($row->{month_from}) . " and " . $dbs->quote($row->{month_to});
	push @$where, "( event_type = " . $self->EDIT_CATEGORY . " or event_type = " . $self->CREATEPAGE_CATEGORY . ")";
	push @$where, "is_content = 'Y'";
	push @$where, "user_id != 0";

	my $oRow = $dbs->select("count(distinct(user_id)) as cnt", 'events', $where, $options);

	return $self->__make_value($oRow);
}

sub __users_content_ns_5times($$$) {
	my ($self, $dbs, $row) = @_;
	# Registered editors
	# C: Number of registered editors who edited more than 5 times in current month (content namespaces)
	
	my $where = [];
	my $options = [
		' GROUP BY user_id ', 
		' having count(user_id) >= 5 ',
		' ORDER BY null '
	];
	switch ( $self->params->{type} ) {
		case WIKIA_TYPE { push @$where, "wiki_id = " . $dbs->quote($row->{id}); }
		case LANG_TYPE 	{ push @$where, "wiki_lang_id = " . $dbs->quote($row->{id}); }
		case CAT_TYPE 	{ push @$where, "wiki_cat_id = " . $dbs->quote($row->{id}); }
		case CAT_LANG_TYPE {
			my @key = split(/\:/, $row->{id});
			push @$where, "wiki_cat_id = " . $dbs->quote($key[0]);
			push @$where, "wiki_lang_id = " . $dbs->quote($key[1]);
		}		
	}

	push @$where, "rev_timestamp between " . $dbs->quote($row->{month_from}) . " and " . $dbs->quote($row->{month_to});
	push @$where, " ( event_type = " . $self->EDIT_CATEGORY . " or event_type = " . $self->CREATEPAGE_CATEGORY . ") ";
	push @$where, "is_content = 'Y'";
	push @$where, "user_id != 0";

	my $sql = $dbs->sql("user_id, count(user_id) as cnt", "events", $where, $options);
	$where = []; $options = [];
	my $oRow = $dbs->select("count(1) as cnt", "($sql) as c", $where, $options);
	return $self->__make_value($oRow);
}

sub __users_content_ns_100times($$$) {
	my ($self, $dbs, $row) = @_;
	# Registered editors
	# D: Number of registered editors who edited more than 100 times in current month (content namespaces)
	
	my $where = [];
	my $options = [
		' GROUP BY user_id ', 
		' having count(user_id) >= 100 ',
		' ORDER BY null '
	];
	switch ( $self->params->{type} ) {
		case WIKIA_TYPE { push @$where, "wiki_id = " . $dbs->quote($row->{id}); }
		case LANG_TYPE 	{ push @$where, "wiki_lang_id = " . $dbs->quote($row->{id}); }
		case CAT_TYPE 	{ push @$where, "wiki_cat_id = " . $dbs->quote($row->{id}); }
		case CAT_LANG_TYPE {
			my @key = split(/\:/, $row->{id});
			push @$where, "wiki_cat_id = " . $dbs->quote($key[0]);
			push @$where, "wiki_lang_id = " . $dbs->quote($key[1]);
		}
	}

	push @$where, "rev_timestamp between " . $dbs->quote($row->{month_from}) . " and " . $dbs->quote($row->{month_to});
	push @$where, " ( event_type = " . $self->EDIT_CATEGORY . " or event_type = " . $self->CREATEPAGE_CATEGORY . ") ";
	push @$where, "is_content = 'Y'";
	push @$where, "user_id != 0";

	my $sql = $dbs->sql("user_id, count(user_id) as cnt", "events", $where, $options);
	$where = []; $options = [];
	my $oRow = $dbs->select("count(1) as cnt", "($sql) as c", $where, $options);
	return $self->__make_value($oRow);
}

sub __articles_all($$$;$) {
	my ($self, $dbs, $row, $stats) = @_;
	# Registered editors
	# E: Number of all content namespaces articles

	my $res = 0;
	if ( defined($stats) ) {
		if ( $self->params->{type} eq LANG_TYPE ) { 
			$res = $stats->{articles}->{$row->{id}};
		} elsif ( $self->params->{type} eq CAT_TYPE ) { 
			$res = $stats->{articles}->{$row->{id}};
		} elsif ( $self->params->{type} eq SUMMARY_TYPE ) {
			$res = $stats->{articles};
		} elsif ( $self->params->{type} eq CAT_LANG_TYPE ) {
			my @key = split(/\:/, $row->{id});
			$res = $stats->{articles}->{$key[0]}->{$key[1]};
		}		
	} else {

		my $where = [];
		my $options = [
			' GROUP BY p2.wiki_id, p2.page_id ',
			' HAVING ( select count(page_id) from events p3 where p3.wiki_id = p2.wiki_id and p3.page_id = p2.page_id and log_id > 0) = 0 ',
			' ORDER BY p2.wiki_id desc, p2.page_id desc, p2.rev_id desc, p2.event_type desc '
		];
		my $cond = "";
		my $use_hint = "";
		if ( $self->params->{type} eq WIKIA_TYPE ) { 
			$cond = "wiki_id = " . $dbs->quote($row->{id}); 
			push @$where, "p2.".$cond; $cond = "p1.".$cond; 
		} elsif ( $self->params->{type} eq LANG_TYPE ) { 
			$cond = "wiki_lang_id = " . $dbs->quote($row->{id}); 
			push @$where, "p2.".$cond; $cond = "p1.".$cond; 
			$use_hint = " use key(articles_lang) ";
		} elsif ( $self->params->{type} eq CAT_TYPE ) { 
			$cond = "wiki_cat_id = " . $dbs->quote($row->{id}); 
			push @$where, "p2.".$cond; $cond = "p1.".$cond; 
			$use_hint = " use key(articles_hub) ";
		} elsif ( $self->params->{type} eq CAT_LANG_TYPE ) {
			my @key = split(/\:/, $row->{id});
			push @$where, "p2.wiki_cat_id = " . $dbs->quote($key[0]);
			push @$where, "p2.wiki_lang_id = " . $dbs->quote($key[1]);
			$cond = "p1.wiki_cat_id = " . $dbs->quote($key[0]) . " and p1.wiki_lang_id = " . $dbs->quote($key[1]);
			$use_hint = " use key(articles_lang) ";
		}

		push @$where, "p2.rev_timestamp <= " . $dbs->quote($row->{month_to});
		my $subquery = $dbs->sql("p2.page_id, p2.wiki_id, max(p2.rev_id) as max_rev", "events p2 $use_hint", $where, $options);

		my $sql = "select count(page_id) as cnt from ";
		$sql .= "(select p1.page_id, p1.is_content, p1.is_redirect, p1.rev_timestamp, p1.event_type from events p1 ";
		$sql .= "inner join ( " . $subquery . " ) as c ";
		$sql .= "on c.page_id = p1.page_id and p1.rev_id = c.max_rev and p1.wiki_id = c.wiki_id ";
		$sql .= ( ( $cond ) ? " where $cond " : "" ) . " ) as d ";
		$sql .= "where d.is_content = 'Y' and d.is_redirect = 'N' ";

		my $oRow = $dbs->query($sql);
		$res = $self->__make_value($oRow);
	}
	
	return $res;
}

sub __articles_new($$$;$) {
	my ($self, $dbs, $row, $stats) = @_;
	# Registered editors
	# F: New articles per day in current month

	my $where = [];
	my $options = [];
	my $cond = "";
	my $res = 0;
	# date of proper events
	if ( defined($stats) ) {
		if ( $self->params->{type} eq LANG_TYPE ) {
			$res = $stats->{newpages}->{$row->{id}};
		} elsif ($self->params->{type} eq CAT_TYPE) { 
			$res = $stats->{newpages}->{$row->{id}};
		} elsif ($self->params->{type} eq SUMMARY_TYPE) {
			$res = $stats->{newpages};
		} elsif ( $self->params->{type} eq CAT_LANG_TYPE ) {
			my @key = split(/\:/, $row->{id});
			$res = $stats->{newpages}->{$key[0]}->{$key[1]};
		}
	} else {	
		if ( $row->{month} < '201009' && $self->params->{type} eq WIKIA_TYPE ) {
			push @$where, "c1.wiki_id = " . $dbs->quote($row->{id}); 
			push @$where, "c1.rev_timestamp between " . $dbs->quote($row->{month_from}) . " and " . $dbs->quote($row->{month_to});
			push @$where, "c1.is_redirect = 'N'";
			push @$where, "c1.is_content = 'Y'";
			push @$where, "c1.page_id not in (select distinct c2.page_id from events c2 where c2.rev_timestamp < ". $dbs->quote($row->{month_from}). " and c2.wiki_id = " . $dbs->quote($row->{id}) . " ) ";

			my $subquery = $dbs->sql("distinct(c1.page_id)", 'events c1', $where, $options);
			my $oRow = $dbs->query("select count(1) as cnt from ($subquery) as q");
			$res = $self->__make_value($oRow);
		} else {
			if ( $self->params->{type} eq WIKIA_TYPE ) {
				push @$where, "wiki_id = " . $dbs->quote($row->{id}); 
			} elsif ( $self->params->{type} eq LANG_TYPE ) { 
				push @$where, "wiki_lang_id = " . $dbs->quote($row->{id}); 
			} elsif ( $self->params->{type} eq CAT_TYPE ) { 
				push @$where, "wiki_cat_id = " . $dbs->quote($row->{id}); 
			} elsif ( $self->params->{type} eq CAT_LANG_TYPE ) {
				my @key = split(/\:/, $row->{id});
				push @$where, "wiki_cat_id = " . $dbs->quote($key[0]);
				push @$where, "wiki_lang_id = " . $dbs->quote($key[1]);
			}
			push @$where, "rev_timestamp between " . $dbs->quote($row->{month_from}) . " and " . $dbs->quote($row->{month_to});
			push @$where, "event_type = " . $self->CREATEPAGE_CATEGORY;
			push @$where, "is_redirect = 'N'";
			push @$where, "is_content = 'Y'";

			my $oRow = $dbs->select("count(1) as cnt", 'events', $where, $options);
			$res = $self->__make_value($oRow);
		}
	}
	return $res;
}

sub __articles_edits($$$) {
	my ($self, $dbs, $row) = @_;
	# Registered editors
	# G: Number of edits content namespaces articles

	my $where = [];
	my $options = [];
	switch ( $self->params->{type} ) {
		case WIKIA_TYPE { push @$where, "wiki_id = " . $dbs->quote($row->{id}); }
		case LANG_TYPE 	{ push @$where, "wiki_lang_id = " . $dbs->quote($row->{id}); }
		case CAT_TYPE 	{ push @$where, "wiki_cat_id = " . $dbs->quote($row->{id}); }
		case CAT_LANG_TYPE {
			my @key = split(/\:/, $row->{id});
			push @$where, "wiki_cat_id = " . $dbs->quote($key[0]);
			push @$where, "wiki_lang_id = " . $dbs->quote($key[1]);
		}		
	}

	push @$where, "rev_timestamp between " . $dbs->quote($row->{month_from}) . " and " . $dbs->quote($row->{month_to});
	push @$where, " ( event_type = " . $self->EDIT_CATEGORY . " or event_type = " . $self->CREATEPAGE_CATEGORY . ") ";
	push @$where, "is_redirect = 'N'";
	push @$where, "is_content = 'Y'";

	my $oRow = $dbs->select("count(1) as cnt", 'events', $where, $options);

	return $self->__make_value($oRow);
}

sub __media_imagelinks($$$;$) {
	my ($self, $dbs, $row, $stats) = @_;
	# Images & Video
	# H: Total number of links to images in content namespaces articles
	
	my $res = 0;
	if ( defined($stats) ) {
		if ( $self->params->{type} eq LANG_TYPE ) {
			$res = $stats->{imagelinks}->{$row->{id}};
		} elsif ($self->params->{type} eq CAT_TYPE) { 
			$res = $stats->{imagelinks}->{$row->{id}};
		} elsif ($self->params->{type} eq SUMMARY_TYPE) {
			$res = $stats->{imagelinks};
		} elsif ( $self->params->{type} eq CAT_LANG_TYPE ) {
			my @key = split(/\:/, $row->{id});
			$res = $stats->{imagelinks}->{$key[0]}->{$key[1]};
		}
	} else {	
		my $where = [];
		my $options = [
			' GROUP BY p2.wiki_id, p2.page_id ',
			' ORDER BY null '
		];
		my $cond = "";
		my $use_hint = "";
		if ( $self->params->{type} eq WIKIA_TYPE) { 
			$cond = "wiki_id = " . $dbs->quote($row->{id}); 
			push @$where, "p2.".$cond; $cond = "p1.".$cond; 
		} elsif ( $self->params->{type} eq LANG_TYPE) { 
			$cond = "wiki_lang_id = " . $dbs->quote($row->{id}); 
			push @$where, "p2.".$cond; $cond = "p1.".$cond; 
			$use_hint = " use key(articles_lang) ";			
		} elsif ($self->params->{type} eq CAT_TYPE) { 
			$cond = "wiki_cat_id = " . $dbs->quote($row->{id}); 
			push @$where, "p2.".$cond; $cond = "p1.".$cond; 
			$use_hint = " use key(articles_hub) ";
		} elsif ($self->params->{type} eq CAT_LANG_TYPE) {
			my @key = split(/\:/, $row->{id});
			push @$where, "p2.wiki_cat_id = " . $dbs->quote($key[0]);
			push @$where, "p2.wiki_lang_id = " . $dbs->quote($key[1]);
			$cond = "p1.wiki_cat_id = " . $dbs->quote($key[0]) . " and p1.wiki_lang_id = " . $dbs->quote($key[1]);
			$use_hint = " use key(articles_lang) ";
		}
		push @$where, "p2.is_content = 'Y'";
		push @$where, "p2.rev_timestamp <= " . $dbs->quote($row->{month_to});	
		my $subquery = $dbs->sql("p2.wiki_id, p2.page_id, p2.event_type, max(p2.rev_timestamp) as max_date ", "events p2 $use_hint", $where, $options);

		my $sql = "select sum(p1.image_links) as cnt from events p1 $use_hint ";
		$sql .= "inner join ( " . $subquery . " ) as c ";
		$sql .= "on c.page_id = p1.page_id and p1.rev_timestamp = c.max_date and p1.wiki_id = c.wiki_id and c.event_type = p1.event_type ";
		$sql .= "where " . ( ( $cond ) ? $cond . " and " : "" ) . " p1.event_type != " . $self->DELETE_CATEGORY . " and p1.is_redirect = 'N' and p1.is_content = 'Y' ";
		
		my $oRow = $dbs->query($sql);
		$res = $self->__make_value($oRow);
	}
	return $res;
}

sub __media_videolinks($$$;$) {
	my ($self, $dbs, $row, $stats) = @_;
	# Images & Video
	# J: Total number of embeded video on content namespaces articles
	
	my $res = 0;
	if ( defined($stats) ) {
		if ( $self->params->{type} eq LANG_TYPE ) { 
			$res = $stats->{videolinks}->{$row->{id}};
		} elsif ($self->params->{type} eq CAT_TYPE)	{ 
			$res = $stats->{videolinks}->{$row->{id}};
		} elsif ($self->params->{type} eq SUMMARY_TYPE ) {
			$res = $stats->{videolinks};
		} elsif ( $self->params->{type} eq CAT_LANG_TYPE ) {
			my @key = split(/\:/, $row->{id});
			$res = $stats->{videolinks}->{$key[0]}->{$key[1]};
		}
	} else {		
		my $where = [];
		my $options = [
			' GROUP BY p2.wiki_id, p2.page_id ',
			' ORDER BY null '	
		];
		my $cond = "";
		my $use_hint = "";
		if ( $self->params->{type} eq WIKIA_TYPE ) { 
			$cond = "wiki_id = " . $dbs->quote($row->{id}); 
			push @$where, "p2.".$cond; $cond = "p1.".$cond; 
		} elsif ($self->params->{type} eq LANG_TYPE ) { 
			$cond = "wiki_lang_id = " . $dbs->quote($row->{id}); 
			push @$where, "p2.".$cond; $cond = "p1.".$cond; 
			$use_hint = " use key(articles_lang) ";			
		} elsif ($self->params->{type} eq CAT_TYPE ) { 
			$cond = "wiki_cat_id = " . $dbs->quote($row->{id}); 
			push @$where, "p2.".$cond; $cond = "p1.".$cond; 
			$use_hint = " use key(articles_hub) ";			
		} elsif ($self->params->{type} eq CAT_LANG_TYPE) {
			my @key = split(/\:/, $row->{id});
			push @$where, "p2.wiki_cat_id = " . $dbs->quote($key[0]);
			push @$where, "p2.wiki_lang_id = " . $dbs->quote($key[1]);
			$cond = "p1.wiki_cat_id = " . $dbs->quote($key[0]) . " and p1.wiki_lang_id = " . $dbs->quote($key[1]);
			$use_hint = " use key(articles_lang) ";
		}
		push @$where, "p2.is_content = 'Y'";
		push @$where, "p2.rev_timestamp <= " . $dbs->quote($row->{month_to});
		my $subquery = $dbs->sql("p2.wiki_id, p2.page_id, p2.event_type, max(p2.rev_timestamp) as max_date ", "events p2 $use_hint", $where, $options);

		my $sql = "select sum(p1.video_links) as cnt from events p1 $use_hint";
		$sql .= "inner join ( " . $subquery . " ) as c ";
		$sql .= "on c.page_id = p1.page_id and p1.rev_timestamp = c.max_date and p1.wiki_id = c.wiki_id and c.event_type = p1.event_type ";
		$sql .= "where " . ( ( $cond ) ? $cond . " and " : "" ) . " p1.event_type != " . $self->DELETE_CATEGORY . " and p1.is_redirect = 'N' and p1.is_content = 'Y' ";
		
		my $oRow = $dbs->query($sql);
		$res = $self->__make_value($oRow);
	}
	return $res;
}

sub __media_imageupload($$$;$) {
	my ($self, $dbs, $row, $stats) = @_;
	# Images & Video
	# I: Total number of uploaded images

	my $res = 0;
	if ( defined($stats) ) {
		if ( $self->params->{type} eq LANG_TYPE ) { 
			$res = $stats->{images}->{$row->{id}};
		} elsif ( $self->params->{type} eq CAT_TYPE ) { 
			$res = $stats->{images}->{$row->{id}};
		} elsif ( $self->params->{type} eq SUMMARY_TYPE ) {
			$res = $stats->{images};
		} elsif ( $self->params->{type} eq CAT_LANG_TYPE ) {
			my @key = split(/\:/, $row->{id});
			$res = $stats->{images}->{$key[0]}->{$key[1]};
		}
	} else {				
		my $where = [];
		my $options = [
			' GROUP BY p2.wiki_id, p2.page_id ',
			' ORDER BY null '	
		];
		my $cond = "";
		my $use_hint = "";
		if ( $self->params->{type} eq WIKIA_TYPE ) { 
			$cond = "wiki_id = " . $dbs->quote($row->{id}); 
			push @$where, "p2.".$cond; $cond = "p1.".$cond; 
		} elsif ( $self->params->{type} eq LANG_TYPE ) { 
			$cond = "wiki_lang_id = " . $dbs->quote($row->{id}); 
			push @$where, "p2.".$cond; $cond = "p1.".$cond; 
			$use_hint = " use key(articles_lang) ";			
		} elsif ( $self->params->{type} eq  CAT_TYPE ) { 
			$cond = "wiki_cat_id = " . $dbs->quote($row->{id}); 
			push @$where, "p2.".$cond; $cond = "p1.".$cond; 
			$use_hint = " use key(articles_hub) ";						
		} elsif ($self->params->{type} eq CAT_LANG_TYPE) {
			my @key = split(/\:/, $row->{id});
			push @$where, "p2.wiki_cat_id = " . $dbs->quote($key[0]);
			push @$where, "p2.wiki_lang_id = " . $dbs->quote($key[1]);
			$cond = "p1.wiki_cat_id = " . $dbs->quote($key[0]) . " and p1.wiki_lang_id = " . $dbs->quote($key[1]);
			$use_hint = " use key(articles_lang) ";
		}
		push @$where, "p2.page_ns = " . Wikia::Utils::NS_IMAGE;
		push @$where, "p2.media_type = " . $self->MEDIATYPE_BITMAP;
		push @$where, "p2.is_redirect = 'N'";
		push @$where, "p2.rev_timestamp <= " . $dbs->quote($row->{month_to});
		my $subquery = $dbs->sql("p2.wiki_id, p2.page_id, p2.event_type, max(p2.rev_timestamp) as max_date ", "events p2 $use_hint", $where, $options);

		my $sql = "select count(1) as cnt from events p1 $use_hint";
		$sql .= " inner join ( " . $subquery . " ) as c ";
		$sql .= " on c.page_id = p1.page_id and p1.rev_timestamp = c.max_date and p1.wiki_id = c.wiki_id and c.event_type = p1.event_type ";
		$sql .= " where " . ( ( $cond ) ? $cond . " and " : "" ) . " p1.event_type != " . $self->DELETE_CATEGORY;
		$sql .= " and p1.page_ns = " . Wikia::Utils::NS_IMAGE . " and p1.media_type = " . $self->MEDIATYPE_BITMAP . " and p1.is_redirect = 'N' ";
		
		my $oRow = $dbs->query($sql);
		$res = $self->__make_value($oRow);
	}
	return $res;
}

sub __media_videoupload($$$;$) {
	my ($self, $dbs, $row, $stats) = @_;
	# Images & Video
	# K: Total number of uploaded videos

	my $res = 0;
	if ( defined($stats) ) {
		if ( $self->params->{type} eq LANG_TYPE ) { 
			$res = $stats->{video}->{$row->{id}};
		} elsif ( $self->params->{type} eq CAT_TYPE ) { 
			$res = $stats->{video}->{$row->{id}};
		} elsif ( $self->params->{type} eq SUMMARY_TYPE ) {
			$res = $stats->{video};
		} elsif ( $self->params->{type} eq CAT_LANG_TYPE ) {
			my @key = split(/\:/, $row->{id});
			$res = $stats->{video}->{$key[0]}->{$key[1]};
		}
	} else {				
		my $where = [];
		my $options = [
			' GROUP BY p2.wiki_id, p2.page_id ',
			' ORDER BY null '	
		];
		my $cond = "";
		my $use_hint = "";
		if ( $self->params->{type} eq WIKIA_TYPE ) { 
			$cond = "wiki_id = " . $dbs->quote($row->{id}); 
			push @$where, "p2.".$cond; $cond = "p1.".$cond; 
		} elsif ($self->params->{type} eq LANG_TYPE ) { 
			$cond = "wiki_lang_id = " . $dbs->quote($row->{id}); 
			push @$where, "p2.".$cond; $cond = "p1.".$cond; 
			$use_hint = " use key(articles_lang) ";					
		} elsif ( $self->params->{type} eq CAT_TYPE ) {
			$cond = "wiki_cat_id = " . $dbs->quote($row->{id}); 
			push @$where, "p2.".$cond; $cond = "p1.".$cond; 
			$use_hint = " use key(articles_hub) ";					
		} elsif ($self->params->{type} eq CAT_LANG_TYPE) {
			my @key = split(/\:/, $row->{id});
			push @$where, "p2.wiki_cat_id = " . $dbs->quote($key[0]);
			push @$where, "p2.wiki_lang_id = " . $dbs->quote($key[1]);
			$cond = "p1.wiki_cat_id = " . $dbs->quote($key[0]) . " and p1.wiki_lang_id = " . $dbs->quote($key[1]);
			$use_hint = " use key(articles_lang) ";
		}
		push @$where, "p2.page_ns = " . Wikia::Utils::NS_VIDEO;
		push @$where, "p2.media_type = " . $self->MEDIATYPE_VIDEO;
		push @$where, "p2.is_redirect = 'N'";
		push @$where, "p2.rev_timestamp <= " . $dbs->quote($row->{month_to});
		my $subquery = $dbs->sql("p2.wiki_id, p2.page_id, max(p2.rev_timestamp) as max_date ", "events p2 $use_hint", $where, $options);

		my $sql = "select count(1) as cnt from events p1 $use_hint";
		$sql .= " inner join ( " . $subquery . " ) as c on c.page_id = p1.page_id and p1.rev_timestamp = c.max_date and p1.wiki_id = c.wiki_id ";
		$sql .= " where " . ( ( $cond ) ? $cond . " and " : "" ) . " p1.event_type != " . $self->DELETE_CATEGORY;
		$sql .= " and p1.page_ns = " . Wikia::Utils::NS_VIDEO . " and p1.media_type = " . $self->MEDIATYPE_VIDEO . " and p1.is_redirect = 'N' ";
		
		my $oRow = $dbs->query($sql);
		$res = $self->__make_value($oRow);
	}
	return $res;
}

sub __make_value($$;$) {
	my ($self, $row, $key) = @_;
	$key = 'cnt' unless $key;
	return ( ( ref($row) eq "HASH" ) && ( keys %$row ) ) ? Wikia::Utils->intval($row->{$key}) : 0;
}

sub get_events_log($$;$) {
	my ($self, $key) = @_;
	# check last re-count date

	# db handle 
	my $lb = Wikia::LB->instance;
	$lb->yml( $Wikia::EventFixStats::YML ) if ($Wikia::EventFixStats::YML);
	my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::STATS )} );
		
	my $where = [
		"el_type = " . $dbs->quote($key)
	];
	my $options = [
		' ORDER BY sl_start DESC ',
		' LIMIT 1 '
	];
	my $oRow = $dbs->select("el_start", 'events_log', $where, $options);
	#$dbs->disconnect if ($dbs);

	return ( $oRow && $oRow->{sl_start} ) ? $oRow->{sl_start} : '2001-01-01 00:00:00';
}

sub update_events_log($$$) {
	my ($self, $key, $values) = @_;

	# db handle 
	my $lb = Wikia::LB->instance;
	$lb->yml( $Wikia::EventFixStats::YML ) if ($Wikia::EventFixStats::YML);
	my $dbw = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_MASTER, 'stats', Wikia::LB::STATS )} );
	
	if ( $dbw ) {
		my $where = [
			"el_type = " . $dbw->quote($key)
		];

		my %data = (
			"el_wiki" 		=> Wikia::Utils->intval($values->{wiki}),
			"el_language" 	=> Wikia::Utils->intval($values->{language}),
			"el_category" 	=> Wikia::Utils->intval($values->{category}),
			"el_summary" 	=> Wikia::Utils->intval($values->{summary}),
			"el_catlang"	=> Wikia::Utils->intval($values->{cat_lang}),
			"el_start" 		=> $values->{start},
			"el_end" 		=> DateTime->now()->strftime("%F %T")
		);
		my $ins = $dbw->update( 'events_log', $where, \%data );
		#$dbw->disconnect if ($dbw);
	}
}

sub __events_records($$$$) {
	my ($self, $column, $from_date, $to_date) = @_;

	# db handle 
	my $lb = Wikia::LB->instance;
	$lb->yml( $Wikia::EventFixStats::YML ) if ($Wikia::EventFixStats::YML);
	my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::STATS )} );

	my @events = ();		
	my $where = [ "rev_timestamp between " . $dbs->quote($from_date) . " and " . $dbs->quote($to_date) ]; 
	my $options = [];
	my $sth = $dbs->select_many("distinct $column", "events", \@$where, \@$options);
	if ($sth) {
		while(my ($c) = $sth->fetchrow_array()) {
			push @events, $c;
		}
		$sth->finish();
	}
	
	#$dbs->disconnect if ($dbs);
	return \@events;
}

sub __get_databases($$$) {
	my ($self, $dbname) = @_;
	
	my $lb = Wikia::LB->instance;
	$lb->yml( $Wikia::EventFixStats::YML ) if ($Wikia::EventFixStats::YML);
	my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED )} );

	my $where_db = [
		"city_public = 1", 
		"city_url not like 'http://techteam-qa%'",
		"city_useshared = 1"
	];

	if ( $dbname && $dbname =~ /\+/ ) {
		# dbname=+177
		$dbname =~ s/\+//i;
		push @{$where_db}, "city_id > " . $dbname;
	} elsif ( $dbname && $dbname ne "*" ) {
		# dbname=wikicities
		my @use_dbs = split /,/,$dbname;
		push @{$where_db}, "city_dbname in (".join(",", map { $dbr->quote($_) } @use_dbs).")";
	} elsif ( !$dbname ) {
		# all wikis - check last event
		my $records = $self->__events_records('wiki_id', $self->params->{start_date}, $self->params->{end_date});
		if ( scalar @$records ) { 
			push @{$where_db}, "city_id in (".join(",", map { $dbr->quote($_) } @$records).")";
		}
	}
	
	my $databases = $dbr->get_wikis($where_db, 'city_dbname');
	
	$dbr->disconnect if ($dbr);
	
	return $databases;
}

sub __get_languages($$;$) {
	my ($self, $lang, $check) = @_;
	
	my $lb = Wikia::LB->instance;
	$lb->yml( $Wikia::EventFixStats::YML ) if ($Wikia::EventFixStats::YML);
	my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED )} );

	my @filter = ();
	if ( $lang ) {
		@filter = split /,/,$lang;
	} else {
		if ( $check ) {
			# all languages - check last event
			my $records = $self->__events_records('wiki_lang_id', $self->params->{start_date}, $self->params->{end_date});
			if ( scalar @$records ) { 
				@filter = @$records;
			}
		}
	}
	my $languages = $dbr->get_languages(\@filter);

	$dbr->disconnect if ($dbr);
	
	return $languages;
}

sub __get_categories($$;$) {
	my ($self, $cat, $check) = @_;
	
	my $lb = Wikia::LB->instance;
	$lb->yml( $Wikia::EventFixStats::YML ) if ($Wikia::EventFixStats::YML);
	my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED )} );

	my @filter = ();
	if ( $cat ) {
		@filter = split /,/,$cat;
	} else {
		if ( $check ) {
			# all languages - check last event
			my $records = $self->__events_records('wiki_cat_id', $self->params->{start_date}, $self->params->{end_date});
			if ( scalar @$records ) { 
				@filter = @$records;
			}
		}
	}
	my $categories = $dbr->get_categories(\@filter);
	$dbr->disconnect if ($dbr);
	return $categories;
}

sub __get_categories_language ($$$;$) {
	my ($self, $cat, $lang, $check) = @_;
	
	my $lb = Wikia::LB->instance;
	$lb->yml( $Wikia::EventFixStats::YML ) if ($Wikia::EventFixStats::YML);
	my $dbr = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED )} );

	my @cats = ();
	my @lang = ();
	my $result = {};
	if ( $cat ) {
		@cats = split /,/,$cat;
	} else {
		if ( $check ) {
			# all languages - check last event
			my $records = $self->__events_records('wiki_cat_id', $self->params->{start_date}, $self->params->{end_date});
			if ( scalar @$records ) { 
				@cats = @$records;
			}
		}
	}
	
	if ( $lang ) {
		@lang = split /,/,$lang;
	} else {
		if ( $check ) {
			# all languages - check last event
			my $records = $self->__events_records('wiki_lang_id', $self->params->{start_date}, $self->params->{end_date});
			if ( scalar @$records ) { 
				@lang = @$records;
			}
		}
	}	
	
	my $categories = $dbr->get_categories(\@cats);
	my $languages = $dbr->get_languages(\@lang);
	
	if ( scalar keys %$categories && scalar keys %$languages ) {
		foreach my $cid ( sort keys %$categories ) {
			foreach my $lid ( sort keys %$languages ) { 
				my $key = $cid . ":" . $lid ;
				$result->{$key} = $categories->{$cid} . ", " . $languages->{$lid};
			}
		}
	}
	
	$dbr->disconnect if ($dbr);
	return $result;	
}

1;
__END__
