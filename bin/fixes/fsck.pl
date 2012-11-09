#!/usr/bin/perl

#
# fix fuckup from 2010-05-26 00-00-00  16:35:24
#

=pod
select count(*) from wikicities_201005271634.city_list where city_url not in ( select city_url from wikicities.city_list );

CREATE TABLE city_migrated (
    `city_id` int(5) unsigned NOT NULL,
    `new_city_id` int(5) unsigned NOT NULL,
    `city_dbname` varchar(255) character set latin1 collate latin1_bin NOT NULL default '',
    `city_url` varchar(255) NOT NULL
);

mysql> describe city_list;
+-------------------------+------------------+------+-----+---------------------------------+----------------+
| Field                   | Type             | Null | Key | Default                         | Extra          |
+-------------------------+------------------+------+-----+---------------------------------+----------------+
| city_id                 | int(9)           | NO   | PRI | NULL                            | auto_increment |
| city_path               | varchar(255)     | NO   |     | /home/wikicities/cities/notreal |                |
| city_dbname             | varchar(64)      | NO   | MUL | notreal                         |                |
| city_sitename           | varchar(255)     | NO   |     | wikicities                      |                |
| city_url                | varchar(255)     | NO   | MUL | http://notreal.wikicities.com/  |                |
| city_created            | datetime         | YES  |     | NULL                            |                |
| city_founding_user      | int(5)           | YES  |     | NULL                            |                |
| city_adult              | tinyint(1)       | YES  |     | 0                               |                |
| city_public             | int(1)           | NO   |     | 1                               |                |
| city_additional         | text             | YES  |     | NULL                            |                |
| city_description        | text             | YES  |     | NULL                            |                |
| city_title              | varchar(255)     | YES  | MUL | NULL                            |                |
| city_founding_email     | varchar(255)     | YES  |     | NULL                            |                |
| city_lang               | varchar(8)       | NO   |     | en                              |                |
| city_special_config     | text             | YES  |     | NULL                            |                |
| city_umbrella           | varchar(255)     | YES  |     | NULL                            |                |
| city_ip                 | varchar(256)     | NO   |     | /usr/wikia/source/wiki          |                |
| city_google_analytics   | varchar(100)     | YES  |     |                                 |                |
| city_google_search      | varchar(100)     | YES  |     |                                 |                |
| city_google_maps        | varchar(100)     | YES  |     |                                 |                |
| city_indexed_rev        | int(8) unsigned  | NO   |     | 1                               |                |
| city_lastdump_timestamp | varchar(14)      | YES  |     | 19700101000000                  |                |
| city_factory_timestamp  | varchar(14)      | YES  |     | 19700101000000                  |                |
| city_useshared          | tinyint(1)       | YES  |     | 1                               |                |
| ad_cat                  | char(4)          | NO   |     |                                 |                |
| city_flags              | int(10) unsigned | NO   | MUL | 0                               |                |
| city_cluster            | varchar(255)     | YES  |     | NULL                            |                |
| city_last_timestamp     | timestamp        | NO   |     | CURRENT_TIMESTAMP               |                |
+-------------------------+------------------+------+-----+---------------------------------+----------------+
28 rows in set (0.00 sec)

mysql> show tables like 'city%';
+-------------------------------------------+
| Tables_in_wikicities_201005271634 (city%) |
+-------------------------------------------+
| city_ads                                  |
| city_cat_mapping                          |
| city_domains                              |
| city_list_log                             |
| city_tag_map                              |
| city_variables                            |
+-------------------------------------------+

=cut

use Modern::Perl;
use Data::Dump;

use FindBin qw/$Bin/;
use lib "$Bin/../lib";


use Wikia::LB;
use Wikia::ExternalLB;

sub checkBlobs {
    #
    # find blobs with inconsistency in users
    #
	my @skip = (
		425, 1221, 120439, 2395, 11845, 177, 121055, 120445, 121203, 80337,
		60566, 4097, 410, 15364, 120294, 120291, 11432, 120287, 121146, 706,
		100592, 120254, 120225, 121039, 120437, 120368, 1346, 9119, 120224,
		120440, 120429, 1148, 120218, 73, 3487, 120423, 11557, 766, 120398,
		1268, 304, 120220, 1657, 78733, 120242, 120407, 3125, 120246, 30404
	);
	my %seen = map { $_ => 1 } @skip;

    my $stimestamp = "20100526000000";
    my $etimestamp = "20100526163524";

    my $dbh = Wikia::ExternalLB->instance->getConnection( Wikia::LB::DB_MASTER, undef, "blobs20101" );
    my $dbc = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::EXTERNALSHARED  );

    my $sth = $dbh->prepare("
		SELECT blob_id, rev_user, rev_user_text, rev_timestamp, rev_wikia_id, rev_id FROM blobs20101.blobs WHERE rev_timestamp BETWEEN ? AND ?
		UNION ALL
		SELECT blob_id, rev_user, rev_user_text, rev_timestamp, rev_wikia_id, rev_id FROM dataware.blobs WHERE rev_timestamp BETWEEN ? AND ?
	");
    $sth->execute( $stimestamp, $etimestamp, $stimestamp, $etimestamp );

    my $cnt = 0;
    my %usr = ();
    my %nonexists = ();
    while( my $row = $sth->fetchrow_hashref ) {

		#
		# skip anons & uncyclo
		#
		next if $row->{ "rev_user" } == 0;
		next if exists $seen{ $row->{ "rev_wikia_id" } };

		#
		# for every row check if user table has the same user_id and user_name
		#
		my $csth = $dbc->prepare( "SELECT user_id, user_name FROM user WHERE user_id = ?" );
		$csth->execute( $row->{ "rev_user" } );
		my $user = $csth->fetchrow_hashref;
		if( exists $user->{ "user_id" } ) {
			if( $user->{ "user_name"} ne $row->{ "rev_user_text" } ) {
				# check user_id in user table
				my $usth = $dbc->prepare( "SELECT user_id FROM user WHERE user_name = ?" );
				$usth->execute( $row->{ "rev_user_text" } );
				my $nuser = $usth->fetchrow_hashref;
				if( exists( $nuser->{ "user_id"} ) ) {
					say "rev_user=$row->{ rev_user }, new_user=$nuser->{ user_id}, user_name=$user->{ user_name } <> rev_user_text=$row->{ rev_user_text } rev_id=$row->{rev_id}, blob_id=$row->{blob_id}, city_id=$row->{rev_wikia_id}";
				}
				else {
					say "rev_user=$row->{ rev_user }, new_user=UNKNOWN, user_name=$user->{ user_name } <> rev_user_text=$row->{ rev_user_text } rev_id=$row->{rev_id}, blob_id=$row->{blob_id}, city_id=$row->{rev_wikia_id}";
					$nonexists{ $row->{ "rev_user" } } = $row;
				}
				$cnt++;
				$usr{ $user->{ "user_id" } }++;
			}
		}
		else {
			say "User $row->{ rev_user } from blobs doesn't exists in user table, rev_user_text=$row->{ rev_user_text } rev_id=$row->{rev_id} city_id=$row->{rev_wikia_id}";
			$nonexists{ $row->{ "rev_user" } } = $row;
		}
    }
    say "$cnt rows in blobs with inconsistency";
}

=pod query
select count(*) from wikicities_201005271634.user where user_name not in ( select user_name from wikicities.user where user_id > 2000000 ) and user_id > 2000000;

+--------------------------+-----------------+------+-----+---------+----------------+
| Field                    | Type            | Null | Key | Default | Extra          |
+--------------------------+-----------------+------+-----+---------+----------------+
| user_id                  | int(5) unsigned | NO   | PRI | NULL    | auto_increment |
| user_name                | varchar(255)    | NO   | UNI |         |                |
| user_real_name           | varchar(255)    | NO   |     |         |                |
| user_password            | tinyblob        | NO   |     | NULL    |                |
| user_newpassword         | tinyblob        | NO   |     | NULL    |                |
| user_email               | tinytext        | NO   | MUL | NULL    |                |
| user_options             | blob            | NO   |     | NULL    |                |
| user_touched             | char(14)        | NO   |     |         |                |
| user_token               | char(32)        | NO   |     |         |                |
| user_email_authenticated | char(14)        | YES  |     | NULL    |                |
| user_email_token         | char(32)        | YES  | MUL | NULL    |                |
| user_email_token_expires | char(14)        | YES  |     | NULL    |                |
| user_registration        | varchar(16)     | YES  |     | NULL    |                |
| user_newpass_time        | char(14)        | YES  |     | NULL    |                |
| user_editcount           | int(11)         | YES  |     | NULL    |                |
| user_birthdate           | date            | YES  |     | NULL    |                |
+--------------------------+-----------------+------+-----+---------+----------------+

CREATE TABLE user_migrated (
       `user_id` int(5) unsigned NOT NULL,
       `new_user_id` int(5) unsigned NOT NULL,
       `user_name` varchar(255) character set latin1 collate latin1_bin NOT NULL default ''
);

=cut
sub moveUsers {
    my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, "wikicities_201005271634" );
    my $dbc = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::EXTERNALSHARED  );

    #
    # get diff for users
    #
    my $sth = $dbh->prepare(qq{
	SELECT *
	FROM wikicities_201005271634.user
	WHERE user_name NOT IN (
	    SELECT user_name
	    FROM wikicities.user
	    WHERE user_id > 2000000
	)
	AND user_id > 2000000;
    });
    $sth->execute();
    while( my $row = $sth->fetchrow_hashref ) {
		# prepare query for insert
		my $sql = qq{
			INSERT INTO user (
				user_name,
				user_real_name,
				user_password,
				user_newpassword,
				user_email,
				user_options,
				user_touched,
				user_token,
				user_email_authenticated,
				user_email_token,
				user_email_token_expires,
				user_registration,
				user_newpass_time,
				user_editcount,
				user_birthdate
			)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
		};

		$dbc->do(
			$sql,
			undef,
			$row->{user_name},
			$row->{user_real_name},
			$row->{user_password},
			$row->{user_newpassword},
			$row->{user_email},
			$row->{user_options},
			$row->{user_touched},
			$row->{ "user_token" },
			$row->{user_email_authenticated},
			$row->{user_email_token},
			$row->{user_email_token_expires},
			$row->{user_registration},
			$row->{user_newpass_time},
			$row->{user_editcount},
			$row->{user_birthdate}
		);
		# get new user id and store it in map table
		my $new_user_id = $dbc->{ "mysql_insertid" };
		$dbh->do( "INSERT INTO user_migrated(user_id, new_user_id, user_name ) VALUES (?, ?, ?)", undef, $row->{ "user_id" }, $new_user_id, $row->{ "user_name" } );
		say "User old_id = $row->{ user_id }, new_id = $new_user_id, name = $row->{ user_name }";
    }
}

sub moveWikis {
    my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, "wikicities_201005271634" );
    my $dbc = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::EXTERNALSHARED  );

    my $sth = $dbh->prepare("
		SELECT *
		FROM city_list
		WHERE city_url NOT IN (
		    SELECT city_url
		    FROM wikicities.city_list
			WHERE city_id > 120200
		)
		AND city_id > 120200
    ");
    $sth->execute();
    while( my $row = $sth->fetchrow_hashref ) {
		my $old_city_id = $row->{ "city_id" };
		say "$row->{city_id} $row->{city_url}";

		#
		# get domains and check if they don't exists in wikicities
		#
		say "Checking domains...";
		my $dsth = $dbh->prepare( "SELECT * from city_domains WHERE city_id = ?" );
		$dsth->execute( $row->{ city_id } );
		my @domains = ();
		while( my $domain = $dsth->fetchrow_hashref ) {
		    push @domains, $dbc->quote( $domain->{ city_domain } );
		}
		$dsth->finish;
		my $sql = sprintf "SELECT count(*) AS count FROM city_domains WHERE city_domain IN (%s)", join( ",", @domains );
		$dsth = $dbc->prepare( $sql );
		$dsth->execute;
		my $checkpoint = $dsth->fetchrow_hashref;
		if( $checkpoint->{ "count"} > 0 ) {
		    say "Domains already taken";
		    say join ",", @domains;
		    # we don't have to be gently here
		    exit;
		}
		$dsth->finish;

		# insert into wikicities
		$sql = "
		INSERT INTO city_list(
			city_path,
			city_dbname,
			city_sitename,
			city_url,
			city_created,
			city_founding_user,
			city_adult,
			city_public,
			city_additional,
			city_description,
			city_title,
			city_founding_email,
			city_lang,
			city_special_config,
			city_umbrella,
			city_ip,
			city_google_analytics,
			city_google_search,
			city_google_maps,
			city_indexed_rev,
			city_lastdump_timestamp,
			city_factory_timestamp,
			city_useshared,
			ad_cat,
			city_flags,
			city_cluster,
			city_last_timestamp
		)
		VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
		";

		$dbc->do(
			$sql,
			undef,
			$row->{city_path},
			$row->{city_dbname},
			$row->{city_sitename},
			$row->{city_url},
			$row->{city_created},
			$row->{city_founding_user},
			$row->{city_adult},
			$row->{city_public},
			$row->{city_additional},
			$row->{city_description},
			$row->{city_title},
			$row->{city_founding_email},
			$row->{city_lang},
			$row->{city_special_config},
			$row->{city_umbrella},
			$row->{city_ip},
			$row->{city_google_analytics},
			$row->{city_google_search},
			$row->{city_google_maps},
			$row->{city_indexed_rev},
			$row->{city_lastdump_timestamp},
			$row->{city_factory_timestamp},
			$row->{city_useshared},
			$row->{ad_cat},
			$row->{city_flags},
			$row->{city_cluster},
			$row->{city_last_timestamp}
		);

		my $new_city_id = $dbc->{ "mysql_insertid" };
		say "city_list moved, old_city_id=$old_city_id, new_city_id=$new_city_id";

		$dbh->do(
			"INSERT INTO city_migrated(city_id, new_city_id, city_dbname, city_url) VALUES (?,?,?,?)",
			undef,
			$row->{ "city_id" },
			$new_city_id,
			$row->{ city_dbname },
			$row->{ city_url }
		);

		# now move all tables with city_id as well
		say "Moving city_ads";
		$dsth = $dbh->prepare( "SELECT * FROM city_ads WHERE city_id = ?" );
		$dsth->execute( $old_city_id );
		while( my $ad = $dsth->fetchrow_hashref ) {
			$dbc->do(qq{
				INSERT INTO city_ads(
					r,
					city_id,
					ad_skin,
					ad_lang,
					ad_cat,
					ad_pos,
					ad_zone,
					ad_server,
					comment,
					dbname,
					ad_keywords,
					domain
				)
				VALUES ( ?,?,?,?,?,?,?,?,?,?,?,? )},
				undef,
				$ad->{r},
				$new_city_id,
				$ad->{ad_skin},
				$ad->{ad_lang},
				$ad->{ad_cat},
				$ad->{ad_pos},
				$ad->{ad_zone},
				$ad->{ad_server},
				$ad->{comment},
				$ad->{dbname},
				$ad->{ad_keywords},
				$ad->{domain}
			);
		}
		$dsth->finish;

		# city_cat_mapping
		say "Moving city_cat_mapping";
		$dsth = $dbh->prepare( "SELECT * FROM city_cat_mapping WHERE city_id = ?" );
		$dsth->execute( $old_city_id );
		while( my $map = $dsth->fetchrow_hashref ) {
			$dbc->do(
				"INSERT INTO city_cat_mapping(city_id,cat_id) VALUES(?,?)",
				undef,
				$new_city_id,
				$map->{ "cat_id" }
			);
		}
		$dsth->finish;

	    # city_domains
		say "Moving city_domains";
		$dsth = $dbh->prepare( "SELECT * FROM city_domains WHERE city_id = ?" );
		$dsth->execute( $old_city_id );
		while( my $r = $dsth->fetchrow_hashref ) {
			$dbc->do(
				"INSERT INTO city_domains(city_id,city_domain) VALUES(?,?)",
				undef,
				$new_city_id,
				$r->{ "city_domain" }
			);
		}
		$dsth->finish;

		# city_variables
		say "Moving city_variables";
		$dsth = $dbh->prepare( "SELECT * FROM city_variables WHERE cv_city_id = ?" );
		$dsth->execute( $old_city_id );
		while( my $r = $dsth->fetchrow_hashref ) {
			$dbc->do(
				"INSERT INTO city_variables(cv_city_id,cv_variable_id,cv_value) VALUES(?,?,?)",
				undef,
				$new_city_id,
				$r->{ cv_variable_id },
				$r->{ cv_value }
			);
		}
		$dsth->finish;

		# city_tag_map
		say "Moving city_tag_map";
		$dsth = $dbh->prepare( "SELECT * FROM city_tag_map WHERE city_id = ?" );
		$dsth->execute( $old_city_id );
		while( my $r = $dsth->fetchrow_hashref ) {
			$dbc->do(
				"INSERT INTO city_tag_map(city_id,tag_id) VALUES(?,?)",
				undef,
				$new_city_id,
				$r->{ "tag_id" }
			);
		}
		$dsth->finish;

		# city_list_log
		say "Moving city_list_log";
		$dsth = $dbh->prepare( "SELECT * FROM city_list_log WHERE cl_city_id = ?" );
		$dsth->execute( $old_city_id );
		while( my $r = $dsth->fetchrow_hashref ) {
			$dbc->do(
				"INSERT INTO city_list_log(cl_city_id,cl_timestamp,cl_user_id,cl_type,cl_text) VALUES(?,?,?,?,?)",
				undef,
				$new_city_id,
				$r->{ cl_timestamp },
				$r->{ cl_user_id },
				$r->{ cl_type },
				$r->{ cl_text }
			);
		}
		$dsth->finish;
    }
}


package main;
#moveUsers();
#moveWikis();
checkBlobs();
