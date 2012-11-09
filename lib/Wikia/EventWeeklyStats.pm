package Wikia::EventWeeklyStats;
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
use Wikia::Config;

our $YML = undef;
$YML = "$Bin/../../../wikia-conf/DB.moli.yml" if -e "$Bin/../../../wikia-conf/DB.moli.yml" ;

use constant MEDIATYPE_BITMAP 		=> 1;
use constant MEDIATYPE_DRAWING		=> 2;
use constant MEDIATYPE_AUDIO			=> 3;
use constant MEDIATYPE_VIDEO			=> 4;
use constant MEDIATYPE_MULTIMEDIA	=> 5;
use constant MEDIATYPE_OFFICE			=> 6;
use constant MEDIATYPE_TEXT				=> 7;
use constant MEDIATYPE_EXECUTABLE	=> 8;
use constant MEDIATYPE_ARCHIVE		=> 9;

use constant EDIT_CATEGORY 				=> 1; 
use constant CREATEPAGE_CATEGORY 	=> 2; 
use constant DELETE_CATEGORY			=> 3;
use constant UNDELETE_CATEGORY		=> 4; 
use constant UPLOAD_CATEGORY			=> 5; 

use constant WIKIA_TYPE			=> 'wikia';
use constant LANG_TYPE			=> 'language';
use constant CAT_TYPE				=> 'category';
use constant CAT_LANG_TYPE	=> 'category_language';
use constant SUMMARY_TYPE		=> 'summary';

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

	my $result;
	print sprintf (
				"Found %0d records between '%s' and '%s'\n", 
				scalar keys %$databases,
				$self->params->{start_date},
				$self->params->{end_date}
	);
	if ( scalar keys %$databases ) {
		$result = $self->__make_stats($databases, $self->params->{start_date}, $self->params->{end_date});
	}
	
	return $result;
}

sub __make_stats($;$$$) {
	my ($self, $databases, $start, $end) = @_;

	my $records = scalar keys %{$databases};
	my $summary_stats = {};
	foreach my $num ( ($self->params->{type} eq CAT_LANG_TYPE ) ? sort keys %{$databases} : sort ( map { sprintf("%012u",$_) } ( keys %{$databases} ) ) ) {
		#--- set city;
		my $id = ($self->params->{type} eq CAT_LANG_TYPE ) ? $num : int $num;
		#--- set start time
		my $start_sec = time();
		#--- date
		print sprintf( "Proceed %s (%d) (%s => %s) \n", $databases->{$id}, $id, $start, $end );

		my $row = { 'id' => $id, 'date_from' => $start, 'date_to' => $end };
		my $res = $self->__row_stats($row); 
		
		$summary_stats->{$databases->{$id}} = $res;
		undef($row);

		my $end_sec = time();
		my @ts = gmtime($end_sec - $start_sec);
		print $databases->{$id} . " processed " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);
	}
	
	return $summary_stats;
}

sub __row_stats ($;$$) {
	my ($self, $row, $stats) = @_;
	#city, month, cnt

	return 0 unless ( $row->{id} );
	return 0 unless ( $row->{date_from} );

	my $lb = Wikia::LB->instance;
	$lb->yml( $YML ) if ($YML);

	# make summary stats
	my $res = {
		'type'		=> $self->params->{type},
		'editors'	=> { 'all' => 0, 'content' => 0, '5times' => 0, '100times' => 0 },
		'articles'	=> { 'all' => 0, 'newday' => 0, 'edits' => 0 },
		'media'		=> { 'imagelinks' => 0, 'videoembeded' => 0, 'imageupload' => 0, 'videoupload' => 0 }
	};

	# db handle 
	my $dbs = new Wikia::DB( {"dbh" => $lb->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::STATS )} );

	# all editors stats ==============================
	# 'editors' => 'all'
	$res->{editors}->{all} = $self->__users_all_ns($dbs, $row);

	# 'editors' => 'content'
	$res->{editors}->{content} = $self->__users_content_ns($dbs, $row);

	# 'editors' => '5times'
	$res->{editors}->{'5times'} = $self->__users_content_ns_5times($dbs, $row);

	# 'editors' => '100times'
	$res->{editors}->{'100times'} = $self->__users_content_ns_100times($dbs, $row);

	#=article stats ==============================
	# 'articles' => 'all'
	$res->{articles}->{all} =  $self->__articles_all($dbs, $row, $stats);

	# 'articles' => 'newday'
	$res->{articles}->{newday} = $self->__articles_new($dbs, $row, $stats);

	# 'articles' => 'edits'
	$res->{articles}->{edits} = $self->__articles_edits($dbs, $row);

	#=media stats ==============================
	# 'media' => 'imagelinks'
	# disabled $res->{media}->{imagelinks} = $self->__media_imagelinks($dbs, $row, $stats);

	# 'media' => 'videoembeded'
	# disabled $res->{media}->{videoembeded} = $self->__media_videolinks($dbs, $row, $stats);

	# 'media' => 'image upload'
	$res->{media}->{imageupload} = $self->__media_imageupload($dbs, $row, $stats);


	# 'media' => 'video upload'
	# disabled $res->{media}->{videoupload} = $self->__media_videoupload($dbs, $row, $stats);

	$dbs->disconnect if ($dbs);

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
	
	push @$where, "rev_timestamp between " . $dbs->quote($row->{date_from}) . " and " . $dbs->quote($row->{date_to});
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
	
	push @$where, "rev_timestamp between " . $dbs->quote($row->{date_from}) . " and " . $dbs->quote($row->{date_to});
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

	push @$where, "rev_timestamp between " . $dbs->quote($row->{date_from}) . " and " . $dbs->quote($row->{date_to});
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

	push @$where, "rev_timestamp between " . $dbs->quote($row->{date_from}) . " and " . $dbs->quote($row->{date_to});
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
			' HAVING max_log = 0 '
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

		push @$where, "p2.rev_timestamp <= " . $dbs->quote($row->{date_to});
		my $subquery = $dbs->sql("p2.page_id, p2.wiki_id, max(p2.rev_id) as max_rev, max(log_id) as max_log", "events p2 $use_hint", $where, $options);

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
		if ( $self->params->{type} eq WIKIA_TYPE ) {
			push @$where, "c1.wiki_id = " . $dbs->quote($row->{id}); 
			push @$where, "c1.rev_timestamp between " . $dbs->quote($row->{date_from}) . " and " . $dbs->quote($row->{date_to});
			push @$where, "c1.is_redirect = 'N'";
			push @$where, "c1.is_content = 'Y'";
			push @$where, "c1.page_id not in (select distinct c2.page_id from events c2 where c2.rev_timestamp < ". $dbs->quote($row->{date_from}). " and c2.wiki_id = " . $dbs->quote($row->{id}) . " ) ";

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
			push @$where, "rev_timestamp between " . $dbs->quote($row->{date_from}) . " and " . $dbs->quote($row->{date_to});
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

	push @$where, "rev_timestamp between " . $dbs->quote($row->{date_from}) . " and " . $dbs->quote($row->{date_to});
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
		push @$where, "p2.rev_timestamp <= " . $dbs->quote($row->{date_to});	
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
		push @$where, "p2.rev_timestamp <= " . $dbs->quote($row->{date_to});
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
		push @$where, "p2.rev_timestamp between " . $dbs->quote($row->{date_from}) . " and " . $dbs->quote($row->{date_to});
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
		push @$where, "p2.rev_timestamp <= " . $dbs->quote($row->{date_to});
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

sub __events_records($$$$) {
	my ($self, $column, $from_date, $to_date) = @_;

	# db handle 
	my $lb = Wikia::LB->instance;
	$lb->yml( $Wikia::EventWeeklyStats::YML ) if ($Wikia::EventWeeklyStats::YML);
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
	
	$dbs->disconnect if ($dbs);
	return \@events;
}

sub __get_databases($$$) {
	my ($self, $dbname) = @_;
	
	my $lb = Wikia::LB->instance;
	$lb->yml( $Wikia::EventWeeklyStats::YML ) if ($Wikia::EventWeeklyStats::YML);
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
	$lb->yml( $Wikia::EventWeeklyStats::YML ) if ($Wikia::EventWeeklyStats::YML);
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
	$lb->yml( $Wikia::EventWeeklyStats::YML ) if ($Wikia::EventWeeklyStats::YML);
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
	$lb->yml( $Wikia::EventWeeklyStats::YML ) if ($Wikia::EventWeeklyStats::YML);
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

sub prepare_email_old {
	my ( $self, $oConf, $res ) = @_;
	
	if ( ( ref($res) eq "HASH" ) && ( scalar keys ( %$res ) ) ) {
		my $text = "";
		foreach my $inx ( keys %$res ) {
			$text = "";
			if ( $res->{$inx}->{type} eq WIKIA_TYPE ) { 
				$text = "Wikia";
			}
			elsif ( $res->{$inx}->{type} eq LANG_TYPE ) { 
				$text = "Language";
			} 
			elsif ( $res->{$inx}->{type} eq CAT_TYPE ) {
				$text = "Category";
			}
			elsif ( $res->{$inx}->{type} eq CAT_LANG_TYPE ) {
				$text = "Category/Language";
			}
			elsif ( $res->{$inx}->{type} eq SUMMARY_TYPE ) { 
				$text = "Summary";
			}			
			$text .= ";All articles;Edits (in period time);All editors;Editors (content NS);Editors (>5 times);Editors (>100 times);Image uploaded;";
			$oConf->output_csv($text);
			$text = ( $res->{$inx}->{type} eq SUMMARY_TYPE ) ? "all;" : "$inx;";
			$text .= $res->{$inx}->{articles}->{all} . ";";			
			$text .= $res->{$inx}->{articles}->{edits} . ";";
			$text .= $res->{$inx}->{editors}->{all} . ";";
			$text .= $res->{$inx}->{editors}->{content} . ";";
			$text .= $res->{$inx}->{editors}->{'5times'} . ";";
			$text .= $res->{$inx}->{editors}->{'100times'} . ";";
			$text .= $res->{$inx}->{media}->{imageupload}.";";
			$text .= "\n";
			$oConf->output_csv($text);
		}
	}
}

sub prepare_email {
	my ( $self, $oConf, $res ) = @_;
	
	if ( ( ref($res) eq "HASH" ) && ( scalar keys ( %$res ) ) ) {
		my $text = "";
		my %rows = (
			'Columns' => [],
			'All articles' => [],
			'Edits' => [],
			'All editors' => [],
			'Content editors' => [],
			'>5 editors' => [],
			'>100 editors' => [],
			'Photo uploaded' => [],
			'Type' => ''
		);
		my @display_columns = (
			'Columns','All articles','Edits','All editors','Content editors','>5 editors','>100 editors','Photo uploaded','Type'	
		);
		
		my $loop = 0;
		foreach my $inx ( keys %$res ) {
			if ( $loop == 0 ) {
				my $type = "";
				if ( $res->{$inx}->{type} eq WIKIA_TYPE ) { 
					$type = "Wikia";
				}
				elsif ( $res->{$inx}->{type} eq LANG_TYPE ) { 
					$type = "Language";
				} 
				elsif ( $res->{$inx}->{type} eq CAT_TYPE ) {
					$type = "Category";
				}
				elsif ( $res->{$inx}->{type} eq CAT_LANG_TYPE ) {
					$type = "Category/Language";
				}
				elsif ( $res->{$inx}->{type} eq SUMMARY_TYPE ) { 
					$type = "Summary";
				}
				
				$rows{'Type'} = $type;
			}
			
			$rows{'Columns'}->[$loop] = ( $res->{$inx}->{type} eq SUMMARY_TYPE ) ? "all" : "$inx";
			$rows{'All articles'}->[$loop] = $res->{$inx}->{articles}->{all};		
			$rows{'Edits'}->[$loop] = $res->{$inx}->{articles}->{edits};
			$rows{'All editors'}->[$loop] = $res->{$inx}->{editors}->{all};
			$rows{'Content editors'}->[$loop] = $res->{$inx}->{editors}->{content};
			$rows{'>5 editors'}->[$loop] = $res->{$inx}->{editors}->{'5times'};
			$rows{'>100 editors'}->[$loop] = $res->{$inx}->{editors}->{'100times'};
			$rows{'Photo uploaded'}->[$loop] = $res->{$inx}->{media}->{imageupload};
			
			$loop++;
		}
		
		foreach ( @display_columns ) {
			my $x = $_;
			if ( $x ne 'Type' ) {
				$text .= ( $x eq 'Columns' ) ? $rows{'Type'} : $x;
				$text .= ";" . join( ";", @{$rows{$x}} ) . ";\n";
			}
		}	
		$oConf->output_csv($text);
	}
}

1;
__END__
