#!/usr/bin/perl -w

use common::sense;
use feature "say";
use encoding "UTF-8";

use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";
use Getopt::Long;
use Time::HiRes qw(gettimeofday tv_interval);
use DateTime;
use Data::Dumper;

use Wikia::Utils;
use Wikia::DB;
use Wikia::LB;
use Wikia::Title;

$|++;

#read long options
my $insert = 700;
my $parse = '';
GetOptions( "insert=i" => \$insert, 'parse=s' => \$parse );

=tables
CREATE TABLE `tags_top_users` (
  `tu_user_id` int(10) unsigned NOT NULL,
  `tu_tag_id` int(10) unsigned NOT NULL,
  `tu_date` date NOT NULL,
  `tu_count` int(10) unsigned NOT NULL,
  `tu_city_lang` varbinary(16) DEFAULT NULL,
  `tu_username` varbinary(255) DEFAULT NULL,
  `tu_groups` varbinary(255) DEFAULT NULL,
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`tu_user_id`,`tu_tag_id`,`tu_date`),
  KEY `tu_tag_id` (`tu_tag_id`,`tu_city_lang`,`tu_user_id`,`tu_count`),
  KEY `tag_lang_date` (`tu_date`,`tu_tag_id`,`tu_city_lang`,`tu_count`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
/*!50100 PARTITION BY LINEAR KEY (tu_date)
PARTITIONS 7 */

CREATE TABLE `tags_top_articles` (
  `ta_city_id` int(10) unsigned NOT NULL,
  `ta_tag_id` int(10) unsigned NOT NULL,
  `ta_page_id` int(10) unsigned NOT NULL,
  `ta_date` date NOT NULL,
  `ta_count` int(10) unsigned NOT NULL,
  `ta_city_lang` varbinary(16) DEFAULT NULL,
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`ta_city_id`,`ta_page_id`,`ta_tag_id`,`ta_date`),
  KEY `tag_lang_date` (`ta_date`,`ta_tag_id`,`ta_city_lang`,`ta_count`),
  KEY `tag_lang` (`ta_tag_id`,`ta_city_lang`,`ta_city_id`,`ta_page_id`,`ta_count`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
/*!50100 PARTITION BY LINEAR KEY (ta_date)
PARTITIONS 4 */

CREATE TABLE `tags_top_blogs` (
  `tb_city_id` int(10) unsigned NOT NULL,
  `tb_page_id` int(10) unsigned NOT NULL,
  `tb_tag_id` int(10) unsigned NOT NULL,
  `tb_date` date NOT NULL,
  `tb_count` int(10) unsigned NOT NULL,
  `tb_city_lang` varbinary(16) DEFAULT NULL,
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`tb_city_id`,`tb_page_id`,`tb_tag_id`,`tb_date`),
  KEY `tb_tag_id` (`tb_tag_id`,`tb_city_lang`,`tb_city_id`,`tb_page_id`,`tb_count`),
  KEY `tag_lang_date` (`tb_date`,`tb_tag_id`,`tb_city_lang`,`tb_count`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
/*!50100 PARTITION BY LINEAR KEY (tb_date)
PARTITIONS 4 */

=cut

my $tables = {
	'tags_top_users' 	=> { 
		'col' => 'tu_date', 
		'exp' => 7,
		'fields' => 'tu_user_id as user_id, tu_tag_id as tag_id, tu_username as username, tu_groups as groups, tu_city_lang as city_lang, sum(tu_count) as all_count',
		'where' => 'tu_tag_id = %d and tu_city_lang = \'%s\'',
		'group' => 'tu_tag_id, tu_user_id',
		'order' => 'all_count desc',
		'keys' => 'user_id, tag_id, username, groups, city_lang, all_count',
		'limit' => 200
	},
	'tags_top_articles' => { 
		'col' => 'ta_date', 
		'exp' => 3, 
		'fields' => 'ta_tag_id as tag_id, ta_city_lang as city_lang, ta_city_id as city_id, ta_page_id as page_id, sum(ta_count) as all_count', 
		'where' => 'ta_tag_id = %d and ta_city_lang = \'%s\'',		
		'group' => 'ta_tag_id, ta_city_lang, ta_city_id, ta_page_id',
		'order' => 'all_count desc',
		'keys' => 'tag_id, city_lang, city_id, page_id, all_count, page_name, page_url, wikiname, wikiurl, page_ns, content_ns',
		'limit' => 100
	},
	'tags_top_blogs' 	=> { 
		'col' => 'tb_date', 
		'exp' => 3,
		'fields' => 'tb_tag_id as tag_id, tb_city_lang as city_lang, tb_city_id as city_id, tb_page_id as page_id, sum(tb_count) as all_count',
		'where' => 'tb_tag_id = %d and tb_city_lang = \'%s\'',		
		'group' => 'tb_tag_id, tb_city_lang, tb_city_id, tb_page_id',
		'order' => 'all_count desc',
		'keys' => 'tag_id, city_lang, city_id, page_id, all_count, page_name, page_url, wikiname, wikiurl, page_ns, content_ns',
		'limit' => 100
	}
};

#----

my $dbw = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS );
my $process_start_time = time();

my $dbc = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED );

# languages & tags
my %tags = ();
my $q = sprintf ( "select tag_id, city_lang from city_list c1, city_tag_map c2 where c1.city_id = c2.city_id and c1.city_public = 1 group by 1, 2;" );
my $sth = $dbc->prepare($q);
if ( $sth->execute() ) {
	while( my $row = $sth->fetchrow_hashref ) {
		push @{$tags{ $row->{ "tag_id" } }}, $row->{city_lang};
	}
	$sth->finish();
}

my $dbr = new Wikia::DB( { "dbh" => $dbc } );

my $settings = Wikia::Settings->instance;
my $t = $settings->variables();
									
my %cnt_ns = ();
foreach my $table ( keys %$tables ) {
	
	next if ( $parse && $parse ne $table );
	
	my $summary_table = "summary_" . $table;
	my $sdate = DateTime->now()->subtract( days => $tables->{$table}->{exp} )->strftime('%Y-%m-%d');
	
	say "Remove data older than last " . $tables->{$table}->{col} . " days ( $sdate )";	
	my $start_time = time();
	my $q = sprintf ( "SELECT * FROM `noreptemp`.`%s` WHERE %s = '%s' LIMIT 1", $table, $tables->{$table}->{col}, $sdate );
	my $sth = $dbw->prepare($q);
	if ( $sth->execute() ) {
		if ( my $cnt = $sth->fetchrow_array() ) {
			say "Removing ... ";
			my $q = sprintf ("DELETE from `noreptemp`.`%s` where %s = '%s' ", $table, $tables->{$table}->{col}, $sdate);
			if ( !$dbw->do($q) ) {
				say "Cannot remove records for table: $table and date: $sdate ";
			}
		}
	}
	my $end_time = time();
	my @ts = gmtime($end_time - $start_time);
	say "Removed after: ". sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0] );	

	my $insert_start_time = time();
			
	foreach my $tag_id ( keys %tags ) {
		foreach ( @{$tags{$tag_id}} ) {
			my $lang = $_;
			my $records = [];
			
			$start_time = time();
			say "build summary table for $table table and lang: $lang, tag: $tag_id ";
			my $q = sprintf ( "SELECT %s FROM `noreptemp`.`%s` WHERE %s GROUP BY %s ORDER BY %s LIMIT %d", 
				$tables->{$table}->{fields}, 
				$table, 
				sprintf( $tables->{$table}->{where}, $tag_id, $lang), 
				$tables->{$table}->{group},
				$tables->{$table}->{order},
				$tables->{$table}->{limit}
			);
			$sth = $dbw->prepare($q);
			if ( $sth->execute() ) {
				my $loop=0;
				my $y = 0;

				while(my @data = $sth->fetchrow_array()) {

					if ( !defined $cnt_ns{$data[2]} ) {
						$cnt_ns{$data[2]} = $dbr->__content_namespaces( $data[2] );
					}
					
					if ( $table eq 'tags_top_articles' || $table eq 'tags_top_blogs' ) {
						my $WF = Wikia::WikiFactory->new( city_id => $data[2] );
						my $dbname = $WF->city_dbname;
						
						next unless $dbname;
						
						my $Title = Wikia::Title->new( db => $dbname, from_id => $data[3] );
						
						next unless $Title;
						next unless $Title->title;
						
						#page_name, 
						$data[5] = $Title->title;
						#page_url, 
						$data[6] = $Title->url;
						#wikiname,
						$data[7] = $Title->sitename;
						#wikiurl
						$data[8] = $WF->city_url;
						#page_ns
						$data[9] = $Title->namespace;
						#content_ns
						$data[10] = Wikia::Utils->in_array( $Title->namespace, $cnt_ns{$data[2]} );
					}
					
					$y++ if ( $loop > 0 && $loop % $insert == 0 );
					$records->[$y] = [] unless $records->[$y];

					push @{$records->[$y]}, "(" . join(",", map { $dbw->quote($_) } @data). ")";
					undef(@data);		
					$loop++;
				}

				say "Prepared " . scalar ( @{$records} ) . ' records to insert ';

				$dbw->do("BEGIN");	
				$dbw->do( sprintf ("DELETE FROM specials.%s WHERE tag_id = %d and city_lang = '%s'", $summary_table, $tag_id, $lang) );	
				if ( scalar @$records ) {	
					my $x = 1;
					foreach my $k ( @{$records} ) {
						my $values = join(",", map { $_ } @$k);
						if ( $values ) {
							say "add $x package with data ";
							my $sql = "INSERT IGNORE INTO specials.$summary_table  ( " . $tables->{$table}->{keys} . " ) values " . $values;
							$sql = $dbw->do($sql);
						}
						$x++;
					}	
				}
				$dbw->do("COMMIT");					
			}
			
			undef ($records);
			
			$end_time = time();
			@ts = gmtime($end_time - $start_time);
			say "Finish after: ". sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0] );			
		}
	}
	
	# clean unused tags;
	my $used_tags = join(", ", keys %tags ) ;
	my $sql = sprintf ("DELETE FROM specials.%s WHERE tag_id not in (%s)", $summary_table, $used_tags);
	$dbw->do( $sql );	
	
	my $insert_end_time = time();
	@ts = gmtime($insert_end_time - $insert_start_time);
	say "Table $table parse after: ". sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0] );			
}

$dbw->disconnect();

my $process_end_time = time();
my @ts = gmtime($process_end_time - $process_start_time);
say "\nscript processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);
say "done";

1;
