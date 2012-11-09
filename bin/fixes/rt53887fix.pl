#!/usr/bin/perl -w

#
# fix for #53887: fix wiki broken in #53882
#
use Modern::Perl;

use Data::Dump;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib";
use Wikia::LB;
use Wikia::WikiFactory;
use File::Next;
use File::Basename;
use File::Path qw(make_path);
use File::Copy;

my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::EXTERNALSHARED );

my $stm = 140110;
my $etm = 140486;

my $starters = {
	"*" => {
		"*"  => "aastarter",
		"en" => "starter",
		"ja" => "jastarter",
		"de" => "destarter",
		"fr" => "frstarter",
		"nl" => "nlstarter",
		"es" => "esstarter",
		"pl" => "plstarter"
	},
	"answers" => {
		"*"  => "genericstarteranswers",
		"en" => "newstarteranswers",
		"de" => "deuanswers",
		"es" => "esstarteranswers",
		"fr" => "frstarteranswers",
		"he" => "hestarteranswers",
		"ar" => "arstarteranswers",
		"nl" => "nlstarteranswers",
	}
};

my $sth = $dbh->prepare( "SELECT * FROM city_list WHERE city_id BETWEEN ? AND ? AND city_public = 1 AND city_id > 123753" );
$sth->execute( $stm, $etm );
while( my $row = $sth->fetchrow_hashref ) {

	my $is_answers = 0;
	my $city_lang = $row->{ "city_lang" };
	my $city_id   = $row->{ "city_id" };
	say "city_id=$city_id city_url=". $row->{city_url};
	#
	# check type (is it answers?)
	#
	if( $row->{ "city_dbname" } =~ /answers/ ) {
		$is_answers = 1;
	}

	#
	# check language
	#
	my $starterdb = $starters->{ ( $is_answers ? "answers" : "*" ) }->{ "*" };

	if( exists $starters->{ ( $is_answers ? "answers" : "*" ) }->{ $city_lang } ) {
		$starterdb = $starters->{ ( $is_answers ? "answers" : "*" ) }->{ $city_lang };
	}
	say "Using $starterdb as starter (language $city_lang, answers $is_answers)";
	my $swf = Wikia::WikiFactory->new( city_dbname => $starterdb );
	my $lwf = Wikia::WikiFactory->new( city_dbname => $row->{ "city_dbname" } );
	my $starter_id = $swf->city_id;
		#
		# export XML pages
		#
	say qx(SERVER_ID=$starter_id php /usr/wikia/source/wiki/maintenance/dumpBackup.php --current --output=file:/tmp/$starterdb.xml --conf /usr/wikia/conf/current/wiki.factory/LocalSettings.php --aconf /usr/wikia/conf/current/AdminSettings.php);

		#
		# import XML pages
		#
	say qx(SERVER_ID=$city_id php /usr/wikia/source/wiki/maintenance/importDump.php /tmp/$starterdb.xml --conf /usr/wikia/conf/current/wiki.factory/LocalSettings.php --aconf /usr/wikia/conf/current/AdminSettings.php);
	say qx(SERVER_ID=$city_id php /usr/wikia/source/wiki/maintenance/rebuildrecentchanges.php --conf /usr/wikia/conf/current/wiki.factory/LocalSettings.php --aconf /usr/wikia/conf/current/AdminSettings.php);
	#
	# open starters image folder
	#
	my $starterdir = $swf->variables()->{ 'wgUploadDirectory' };
	my $targetdir  = $lwf->variables()->{ 'wgUploadDirectory' };

	my $iterator = File::Next::files( $starterdir );

	while ( defined ( my $file = $iterator->() ) ) {
		#
		# check if file exists on target
		#
		my $source = $file;
		$file =~ s/$starterdir//;
		my $target = $targetdir . $file;

		unless( -f $target ) {
			my $dir = dirname( $target );
			make_path( $dir, { mode => 0777 } ) unless ( -d $dir );
			say "Copy $source to $target";
			copy( $source, $target );
		}

	}

	#
# set permissions on target
	#
	print "Settings permissions on $targetdir...";
	print qx(chmod ug+rw -R $targetdir);
	print qx(chown www-data.www-data -R $targetdir);
	say "done";
}
