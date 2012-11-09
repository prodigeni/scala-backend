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
my $fromid = 0;
my $limit = 10000000000000;
GetOptions( "insert=i" => \$insert, 'parse=s' => \$parse, 'fromid=i' => \$fromid, 'limit=i' => \$limit );

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

#----

my $dbw = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS );
my $process_start_time = time();

my $dbc = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, 'stats', Wikia::LB::EXTERNALSHARED );

# languages & tags
my %tables = ( 'summary_tags_top_articles', 'summary_tags_top_blogs' );
my @ts = 0;

my $dbr = new Wikia::DB( { "dbh" => $dbc } );

my $settings = Wikia::Settings->instance;
my $t = $settings->variables();


my $where = "1 = 1";
my $order = "city_id, page_id, tag_id";



foreach my $table ( %tables ) {
	next if ( $parse && $parse ne $table );
	my $summary_table = $table;

	my $start_time = time();
	my $end_time = 0;
	
	my $currow = -1 + $fromid;
	my $numrows = 0;
	
	my $countsth = $dbw->prepare(qq(SELECT count(*) as c FROM specials.$table WHERE $where));
	if ( $countsth->execute() ) {
		my $countrow = $countsth->fetchrow_hashref();
		$numrows = $countrow->{c};
	}
	
	my $sth = $dbw->prepare(qq(SELECT * FROM specials.$table WHERE $where ORDER BY $order LIMIT $fromid, $limit));
	if ( $sth->execute() ) {
		while(my $row = $sth->fetchrow_hashref()) {
			$currow++;
			next if ($currow < $fromid);
			my $WF = Wikia::WikiFactory->new( city_id => $row->{city_id} );
			my $dbname = $WF->city_dbname;

			next unless $dbname;

			my $Title = Wikia::Title->new( db => $dbname, from_id => $row->{page_id} );
			next unless $Title;
			next unless $Title->title;
						
			if ($row->{page_name} eq $Title->title && $row->{page_url} eq $Title->url && $row->{wikiname} eq $Title->sitename && $row->{wikiurl} eq $WF->city_url) {
				print sprintf("-- %d / %d -- skipping (%d,%d,%d) = %s [%s]\n",$currow,$numrows,$row->{city_id},$row->{page_id},$row->{tag_id},$row->{page_name},$row->{wikiname});
				next;
			} else {
				print sprintf("-- %d / %d -- fixing (%d,%d,%d) = %s [%s]\n",$currow,$numrows,$row->{city_id},$row->{page_id},$row->{tag_id},$row->{page_name},$row->{wikiname});
			}
			
			#page_name, 
			$row->{page_name} = $Title->title;
			#page_url, 
			$row->{page_url} = $Title->url;
			#wikiname,
			$row->{wikiname} = $Title->sitename;
			#wikiurl
			$row->{wikiurl} = $WF->city_url;
			#page_ns
			#$data[9] = $Title->namespace;
			#content_ns
			#$data[10] = Wikia::Utils->in_array( $Title->namespace, $cnt_ns{$data[2]} );

			my $updateq = sprintf( "UPDATE specials.%s SET page_name = %s, page_url = %s, wikiname = %s, wikiurl = %s WHERE city_id = %d and page_id = %d and tag_id = %d",
				$table,
				$dbw->quote($row->{page_name}),
				$dbw->quote($row->{page_url}),
				$dbw->quote($row->{wikiname}),
				$dbw->quote($row->{wikiurl}),
				$row->{city_id},
				$row->{page_id},
				$row->{tag_id}
			);
#			print $updateq, "\n";
			$dbw->do($updateq);
		}
		$end_time = time();
		@ts = gmtime($end_time - $start_time);
		say "Finish after: ". sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0] );			
	}
}

$dbw->disconnect();

my $process_end_time = time();
my @ts = gmtime($process_end_time - $process_start_time);
say "\nscript processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);
say "done";

1;
