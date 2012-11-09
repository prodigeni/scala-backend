package Wikia::DB;

use common::sense;

use strict;
use Carp;
use DBI;
use IO::File;
use Data::Types qw(:all);

use Wikia::LB;

use constant contentNS => "359";
use constant NS_IMAGE => "6";
use constant NS_USER => "2";

use base qw(Class::Accessor);
Wikia::DB->mk_accessors(qw(dbname dbh host));

sub handler {
    my ($self, $dbname) = @_;
	$dbname = $self->dbname unless ($dbname);

	if (defined($self->dbh)) {
		return $self->dbh ;
	}

	my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, $dbname );

    $self->dbh($dbh);
    return $self->dbh;
}

sub select_many {
	my ($self, $select, $from, $where, $other) = @_;

	my $dbh = $self->handler();
	return 0 unless ($dbh);

	my $q = "select $select ";
	$q .= "from $from " if $from;
	$q .= "where " . join(" and ", @$where) if ( scalar @$where);
	$q .= " " . join(" ", @$other) if (scalar @$other );

	my $c = $dbh->prepare($q);

	my $cnt = 0;
	while (!$c && $cnt < 5 ) {
		$c = $dbh->prepare($q);
		$cnt++;
	};
	return if (!$c);

	if(!$c->execute()) {
		print ("ERROR: " . $q . " - ". $dbh->errstr."\n");
		$c->finish();
		return;
	};

	return $c;
}

sub select {
	my ($self, $select, $from, $where, $other) = @_;

	my %res = ();
	my $dbh = $self->handler();
	return \%res unless ($dbh);

	my $q = "select $select ";
	$q .= "from $from " if $from;
	$q .= "where " . join(" and ", @$where) if ( ($where) && (scalar @$where) );
	$q .= join(" ", @$other) if ( ($other) && (scalar @$other) );

	my $c = $dbh->prepare($q);
	if (!$c) {
		print ("ERRROR: " . $q . " - " .$dbh->errstr. "\n");
		return;
	}

	if (!$c->execute()) {
		print ("ERRROR: " . $q . " - " .$dbh->errstr. "\n");
		return;
	}

	my $res = $c->fetchrow_hashref(); $c->finish();
	%res = ();
	%res = %{$res} if ($res);
	return \%res;
}

sub query {
	my ($self, $sql) = @_;

	my %res = ();
	my $dbh = $self->handler();
	return \%res unless ($dbh);

	#print "\n" . $sql . "\n";

	my $c = $dbh->prepare($sql);
	if (!$c) {
		print ("ERRROR: " . $sql . " - " .$dbh->errstr. "\n");
		return;
	}

	if (!$c->execute()) {
		print ("ERRROR: " . $sql . " - " .$dbh->errstr. "\n");
		return;
	}

	my $res = $c->fetchrow_hashref(); $c->finish();
	%res = ();
	%res = %{$res} if ($res);
	return \%res;
}

sub sql {
	my ($self, $select, $from, $where, $other) = @_;

	my %res = ();
	my $dbh = $self->handler();
	return \%res unless ($dbh);

	my $q = "select $select ";
	$q .= "from $from " if $from;
	$q .= "where " . join(" and ", @$where) if ( ($where) && (scalar @$where) );
	$q .= join(" ", @$other) if ( ($other) && (scalar @$other) );

	return $q;
}

sub delete
{
    my ($self, $table, $where) = @_;

	my $dbh = $self->handler();
	return 0 unless ($dbh);

	my @w = ();
	if ( ref($where) eq "HASH" ) {
		foreach (keys %$where) {
			my $value = "";
			if (/^-/) {
				$value = $_ . " = " . $where->{$_} ;
			} else {
				$value = $_ . " = " . $dbh->quote($where->{$_});
			}
			push @w, $value;
		}
	} else {
		@w = @{$where};
	}

    my $w = join(" and ", @w) if ( scalar @w );
    my $q = "delete from $table where $w";

    if (!$dbh->do($q)) {
		print ("ERRROR: " . $q . " - " .$dbh->errstr. "\n");
		return 0;
    };
    return $q;
}

sub execute
{
	my($self, $q)=@_;
	my $dbh = $self->handler();
	return 0 unless ($dbh);

	if (!$dbh->do($q)) {
		print ("ERRROR: " . $q . " - " .$dbh->errstr. "\n");
		return 0;
	};
	return 1;
}

sub update
{
	my($self, $table, $where, $data, $soft) = @_;

	my $dbh = $self->handler();
	return 0 unless ($dbh);

	my $q = "update $table set ";
	foreach (keys %$data) {
		if (/^-/) {
			s/^-//;
			$q .= "$_ = ".$data->{-$_}.",";
		} else {
			$q .= "$_ = ".$dbh->quote($data->{$_}).",";
		}
	}
	chop($q);

    my $w = join(" and ", @$where) if (scalar(@$where));
	$q .= " where $w " if ($w);

	if (!$soft) {
		if (!$dbh->do($q)) {
			print ("ERRROR: " . $q . " - " .$dbh->errstr. "\n");
		};
	}
	return $q;
}

sub insert($$$$;$$) {
	my ($self, $table, $quote, $data, $options, $ignore, $dry) = @_;
	my ($names,$values);

	my $dbh = $self->handler();
	return 0 unless ($dbh);

	foreach (keys %$data) {
		if (/^-/) {
			$values.=" ".$data->{$_}.","; s/^-//;
		} else {
			$values.=" ".$dbh->quote($data->{$_}).",";
		}
		$names.="$_,";
	}
	chop($names); chop($values);
	my $q = "";
	$ignore = ( $ignore ) ? " IGNORE " : "";
	if ($quote) {
		$q = "insert $ignore into \"$table\" ($names) values($values) ";
	} else {
		$q = "insert $ignore into $table ($names) values($values) ";
	}

	if ( $options ) {
		$q .= join( " ", @$options );
	}

	if ( $dry ) {
		return $q;
	}

	if ( !$dbh->do($q) ) {
		print ("ERRROR: " . $q . " - " .$dbh->errstr. "\n");
		return 0;
	};

	return $dbh->{mysql_insertid};
}

sub quote
{
	my ($self, $data) = @_;
	my $dbh = $self->handler();
	return $data unless ($dbh);

	return $dbh->quote($data);
}

sub ping {
	my ($self) = @_;
	my $dbh = $self->handler();
	return $dbh->ping();
}

sub disconnect {
	my ($self) = @_;
	my $dbh = $self->handler();
	return $dbh->disconnect();
}

sub max
{
	my ($self,$select,$tabela,$where) = @_;
	my $dbh = $self->handler();
	return 0 unless ($dbh);
	my @where = @{$where};
	my @other = ();

	my $max_id = 0;
	my ($hRow) = $self->select("max($select) as value", "$tabela", \@where, \@other);
	if ( ref($hRow) eq "HASH" ) {
		if (($hRow->{value} eq '') || (!$hRow->{value})) {
			$max_id = 0;
		} else {
			$max_id = $hRow->{value};
		}
	}
	return $max_id;
}

sub tables
{
	my($self, $table) = @_;
	my $dbh = $self->handler();
	return 0 unless ($dbh);

	my @tables = $dbh->tables(undef, "public", "$table", "table" , {noprefix => 1});

	return @tables;
}

sub get_daemon_task
{
	my ($self, $id) = @_;
	my $row = ();
	my $dbh = $self->handler();
	return $row unless ($dbh);

	my @where = ("dj_id = ".$dbh->quote($id));
	my @options = ();
	my %databases = ();
	$row = $self->select("dj_start, dj_end, dj_frequency, dj_param_values, dj_result_file, dj_result_emails", "`dataware`.`daemon_tasks_jobs`", \@where, \@options);

	return $row;
}

sub update_daemon_task
{
	my ($self, $data, $id) = @_;
	my $row = ();
	my $dbh = $self->handler();
	return $row unless ($dbh);

	my @where = ("dj_id = ".$dbh->quote($id));
	my $q = $self->update("`dataware`.`daemon_tasks_jobs`", \@where, $data);

	return $q;
}

sub getLag {
	my $self = shift;

	my $sth = $self->handler()->prepare( 'SHOW SLAVE STATUS' );
	if( $sth->execute() ) {
		my $row = $sth->fetchrow_hashref;
		$sth->finish();

		if ( to_string( $row->{ 'Seconds_Behind_Master' } ) eq '' ) {
			return 0;
		} else {
			return to_int( $row->{ 'Seconds_Behind_Master' } );
		}
	}

	return 0;
}

sub check_lag {
	my ($self, $value) = @_;

	my $is_lag = 1;
	my $lag_loop = 0;
	my $sleep = 10;
	$value = 666 unless ($value);
	while ($is_lag == 1) {
		my $lag = $self->getLag();
		if ($lag > $value) {
			print "WARN: lag: ".$lag.": waiting: " . $sleep . " seconds\n";
			$lag_loop++;
			sleep(int($sleep + .5));
		} else {
			$is_lag = 0;
		}
		if ($is_lag == 1 && $lag_loop > 666666) {
			print "ERROR: Too many lags: script terminated";
			exit;
		}
	}
}

sub get_wikis($$;$)
{
	my ($self, $where, $field) = @_;

	my @options = ("order by city_id");
	my %databases = ();
	$field = "city_dbname" unless($field);
	my $sth = $self->select_many("city_id, ".$field, "`wikicities`.`city_list`", $where, \@options);
	if ($sth) {
		while(my ($city_id,$dbname) = $sth->fetchrow_array()) {
			$databases{$city_id} = $dbname;
		}
		$sth->finish();
	}
	return \%databases;
}

sub get_users($$;$$)
{
	my ($self, $where, $field, $limit) = @_;

	my @options = ("order by user_id");
	if ( $limit ) {
		push @options, " limit $limit ";
	}
	my %databases = ();
	$field = "user_name" unless($field);
	my $sth = $self->select_many("user_id, ".$field, "`wikicities`.`user`", $where, \@options);
	if ($sth) {
		while(my ($user_id,$fname) = $sth->fetchrow_array()) {
			$databases{$user_id} = $fname;
		}
		$sth->finish();
	}
	return \%databases;
}

sub get_wikis_list($$;$)
{
	my ($self, $where, $fields) = @_;

	my @options = ("order by city_id");
	my %databases = ();
	my $f = join(",", @$fields);
	my $sth = $self->select_many("city_id, $f", "city_list", $where, \@options);
	if ($sth) {
		while (my $row = $sth->fetchrow_hashref()) {
			$databases{$row->{city_id}} = $row;
		}
		$sth->finish();
	}
	return \%databases;
}

sub id_to_wikia($$) {
	my ($self, $city_id) = @_;

	my @where = ("city_id = '".$city_id."'"); my @options = ();
	my $oRow = $self->select("*", "city_list", \@where, \@options);
	if ( ref($oRow) eq "HASH" ) {
		$oRow->{server} = $self->get_server($city_id);
		return $oRow;
	}
	return undef;
}

sub get_wgservers
{
	my ($self, $where) = @_;

	push @$where, "cv_value != ''" unless ($where);
	push @$where, "cv_variable_id = 5";
	my @options = ("order by cv_city_id");
	my %servers = ();
	my $sth = $self->select_many("cv_city_id, cv_value", "`wikicities`.`city_variables`", $where, \@options);
	if ($sth) {
		while(my ($city_id, $cv_value) = $sth->fetchrow_array()) {
			$servers{$city_id} = $city_id;
			if ($cv_value =~ /\"http:\/\/(.*?)\"/i) {
				$servers{$city_id} = $1;
			}
		}
		$sth->finish();
	}
	return \%servers;
}

=get subdomain
=cut
sub get_variable_value {
	my ($self, $city_id, $variable_id) = @_;
	my $res = "";

	my $dbh = $self->handler();
	return $res unless ($dbh);

	my @where = (
		"cv_city_id = ".$dbh->quote($city_id),
		"cv_variable_id = ".$dbh->quote($variable_id),
	);
	my @options = ();
	my ($hRow) = $self->select("cv_value", "`wikicities`.`city_variables`", \@where, \@options);

	if ( ref($hRow) eq "HASH" ) {
		if ( $hRow->{cv_value} ) {
			use PHP::Serialization qw(serialize unserialize);
			$res = unserialize($hRow->{cv_value});
		}
	}

	return $res;
}

sub __content_namespaces
{
	my ($self, $city_id) = @_;

	my @options = ();
	my @cNS = ();
	my @where = ("cv_city_id = '".$city_id."'", "cv_variable_id='".contentNS."'");
	my $sth = $self->select_many("cv_value", "`wikicities`.`city_variables`", \@where, \@options);
	if ($sth) {
	    my ($cNSValues) = $sth->fetchrow_array();
	    if ($cNSValues) {
			use PHP::Serialization qw(serialize unserialize);
			my $cNSValues  = unserialize($cNSValues);
			if ( ref($cNSValues) eq 'HASH' ) {
				@cNS = values %$cNSValues;
			} elsif ( ref($cNSValues) eq 'ARRAY' ) {
				@cNS = @$cNSValues;
			}
		}
		$sth->finish();
	}
	if ( !scalar @cNS) {
		push @cNS, 0;
	}
	return \@cNS;
}

sub getWikiCats($;$)
{
	my ($self, $hubs) = @_;

	my %city_cats = ();
	my @where = ();
	if ($hubs) {
		my @hub = split /,/,$hubs;
		push @where, "cat_name in (".join(",", map { $self->handler()->quote($_) } @hub).")";
	}
	my @options = ("order by cat_id");
	my $sth = $self->select_many("cat_id, cat_name", "`wikicities`.`city_cats`", \@where, \@options);
	if ($sth) {
		while(my ($cat_id, $cat_name) = $sth->fetchrow_array()) {
			$city_cats{$cat_id} = $cat_name;
		}
		$sth->finish();
	}

	@where = ();
	if ( scalar keys %city_cats ) {
		my @cats = keys %city_cats;
		push @where, "cat_id in (".join(",", map { $self->handler()->quote($_) } @cats).")";
	}
	@options = ("order by city_id");
	my %cats = ();
	$sth = $self->select_many("city_id, cat_id", "`wikicities`.`city_cat_mapping`", \@where, \@options);
	if ($sth) {
		while(my ($city_id, $cat_id) = $sth->fetchrow_array()) {
			$cats{$city_id} = $city_cats{$cat_id};
		}
		$sth->finish();
	}

	return \%cats;
}

sub getAllImages($) {
	my ($self) = @_;

	$self->ping() if ($self);

	my @where = ( "page_namespace = ".$self->quote(NS_IMAGE) );
	my @options = ();
	my $sth = $self->select_many(
		"page_id, page_title",
		"page",
		\@where,
		\@options
	);

	my %images = ();
	if ($sth) {
		while(my ($page_id, $page_title) = $sth->fetchrow_array()) {
			$page_title =~ s/\_/ /gi;
			$images{"$page_title"} = $page_id;
		}
		$sth->finish();
	}
	return \%images;
}

sub getPagesData($$$) {
	my ($self, $fields, $wheredb) = @_;
	my @res = ();

	my @where = @{$wheredb}; push @where, "page_id = rev_page";
	my @options = ("ORDER BY rev_timestamp, page_id, rev_id");
	my $sth = $self->select_many(
		"/*! STRAIGHT_JOIN */ ".join(',', @{$fields}),
		"page FORCE INDEX (PRIMARY), revision r1 FORCE INDEX (PRIMARY)",
		\@where,
		\@options
	);

	if ($sth) {
		my %results;
		@results{@{$fields}} = ();
		$sth->bind_columns(map { \$results{$_} } @{$fields});

		@res = (\%results, sub {$sth->fetch() }, $sth);
	}

	return @res;
}


sub table_exists {
    my ($self, $table) = @_;
	my $dbh = $self->handler();
	return 0 unless ($dbh);

	eval {
		local $dbh->{PrintError} = 0;
		local $dbh->{RaiseError} = 1;
		$dbh->do(qq{SELECT * FROM $table WHERE 1 = 0 });
	};

	return 1 unless $@;
    return 0;
}


sub get_user_by_id($$) {
	my ($self, $user_id) = @_;

	my $res = "";

	my $dbh = $self->handler();
	return $res unless ($dbh);

	my @where = ( "user_id = ".$dbh->quote($user_id) );
	my @options = ();
	my ($hRow) = $self->select("user_name", "user", \@where, \@options);

	if ( ref($hRow) eq "HASH" ) {
		$res = $hRow->{user_name};
	}

	return $res;
}

sub get_server($$) {
	my ( $self, $city_id) = @_;
	my @w = ("cv_city_id = '" . $city_id . "'");
	my $wgServers = $self->get_wgservers(\@w);
	return $wgServers->{$city_id} if ($wgServers->{$city_id});
	return undef;
}

sub get_image_by_name($$) {
	my ($self, $img_name) = @_;
	my $dbh = $self->handler();
	return undef unless ($dbh);
	my @where = ( "img_name = ".$dbh->quote($img_name) ); my @options = ();
	my $oRow = $self->select("*", "image", \@where, \@options);
	return $oRow if ( ref($oRow) eq "HASH" );
	return undef;
}

sub get_categories($;$) {
	my ($self, $cat_id) = @_;
	my $dbh = $self->handler();
	return undef unless ($dbh);
	my %categories = ();
	my @where = (); my @options = ();

	if ( $cat_id ) {
		if ( UNIVERSAL::isa($cat_id, 'ARRAY') && scalar @$cat_id ) {
			push @where, "cat_id in (".join(",", map { $dbh->quote($_) } @$cat_id).")";
		} elsif ( ! UNIVERSAL::isa($cat_id, 'ARRAY') ) {
			push @where, "cat_id = ".$dbh->quote($cat_id);
		}
	}

	my $sth = $self->select_many("cat_id, cat_name", "city_cats", \@where, \@options);
	if ($sth) {
		while(my ($id, $name) = $sth->fetchrow_array()) {
			$categories{$id} = $name;
		}
		$sth->finish();
	}
	return \%categories;
}

sub get_wiki_cat($;$) {
	my ($self, $city_id) = @_;
	my $dbh = $self->handler();
	return undef unless ($dbh);
	my @where = ( "city_id = ".$dbh->quote($city_id) ); my @options = ();
	my $oRow = $self->select("cat_id", "city_cat_mapping", \@where, \@options);
	return $oRow->{cat_id} if ( ref($oRow) eq "HASH" );
	return undef;
}

sub get_wiki_lang($;$) {
	my ($self, $city_id) = @_;
	my $dbh = $self->handler();
	return undef unless ($dbh);
	my @where = ( "city_id = ".$dbh->quote($city_id) ); my @options = ();
	my $oRow = $self->select("city_lang", "city_list", \@where, \@options);

	my $res = undef;
	if ( ref($oRow) eq "HASH" ) {
		my $lang_code = $oRow->{city_lang} ;
		my $lang = $self->get_lang_by_code($lang_code);
		if ( ref($lang) eq "HASH" ) {
			$res = $lang->{lang_id};
		}
	};
	return $res;
}

sub get_wiki_tags($$;$) {
	my ($self, $city_id, $hash) = @_;
	my $dbh = $self->handler();
	return undef unless ($dbh);
	my @where = ( "city_id = ".$dbh->quote($city_id), " tag_id = id " ); my @options = ();
	my $sth = $self->select_many(
		"tag_id, name",
		"city_tag, city_tag_map",
		\@where,
		\@options
	);

	my %tags = ();
	if ($sth) {
		while(my ($id, $name) = $sth->fetchrow_array()) {
			$tags{"$id"} = ( $hash ) ? {} : $name;
		}
		$sth->finish();
	}
	return \%tags;

}

sub getDBbyId($$) {
	my ($self, $city_id) = @_;
	my $res = "";

	my $dbh = $self->handler();
	return $res unless ($dbh);

	my @where = ( "city_id = ".$dbh->quote($city_id) );
	my @options = ();
	my ($hRow) = $self->select("city_dbname", "city_list", \@where, \@options);

	if ( ref($hRow) eq "HASH" ) {
		$res = $hRow->{city_dbname};
	}

	return $res;
}

sub get_lang_by_code($$) {
	my ($self, $lang_code) = @_;
	my $dbh = $self->handler();
	return undef unless ($dbh);
	my @where = ( "lang_code = ".$dbh->quote($lang_code) ); my @options = ();
	my $oRow = $self->select("*", "city_lang", \@where, \@options);
	return $oRow if ( ref($oRow) eq "HASH" );
	return undef;
}

sub get_languages($;$) {
	my ($self, $lang_code) = @_;
	my $dbh = $self->handler();
	return undef unless ($dbh);
	my %languages = ();
	my @where = (); my @options = ();
	if ( $lang_code ) {
		if ( UNIVERSAL::isa($lang_code, 'ARRAY') && scalar @$lang_code ) {
			push @where, "lang_code in (".join(",", map { $dbh->quote($_) } @$lang_code).")";
		} elsif ( ! UNIVERSAL::isa($lang_code, 'ARRAY') ) {
			push @where, "lang_code=".$dbh->quote($lang_code);
		}
	}
	my $sth = $self->select_many("lang_id, lang_code", "city_lang", \@where, \@options);
	if ($sth) {
		while(my ($id, $code) = $sth->fetchrow_array()) {
			$languages{$id} = $code;
		}
		$sth->finish();
	}
	return \%languages;
}

sub get_tags($) {
	my ($self) = @_;
	my $dbh = $self->handler();
	return undef unless ($dbh);
	my %tags = ();
	my @where = (); my @options = ();

	my $sth = $self->select_many("id, name", "city_tag", \@where, \@options);
	if ($sth) {
		while(my ($id, $name) = $sth->fetchrow_array()) {
			$tags{$id} = $name;
		}
		$sth->finish();
	}
	return \%tags;
}

1;
__END__
