#!/usr/bin/perl
package UpdatePages;

use common::sense;
use Data::Dumper;
use FindBin qw/$Bin/;
use lib "$Bin/../lib";
use PHP::Serialization qw(serialize unserialize);

use Moose;

use Wikia::Utils qw(strtolower fixutf);
use Wikia::LB;
use Wikia::ExternalLB;
use Wikia::DB;
use Wikia::Settings;
use Time::HiRes qw(usleep);

use base qw/Class::Accessor::Fast/;

$|++;
has "row" => (
	is => "rw",
	isa => "HashRef",
	required => 1
);
has "dbname" => ( 
	is => "rw", 
	isa => "Str",
	trigger => sub {
		my ( $self ) = @_;
		say "Set dbname" if $self->debug;
		$self->dbw( Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, $self->dbname ) );
		$self->dbd( Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::DATAWARESHARED ) );
	}
);
has "id"  => ( 
	is => "rw", 
	isa => "Int",
	trigger => sub {
		my ( $self ) = @_;
		$self->_build_content_namespaces();
	}
);
has "dbw" => (
	is => "rw"
);
has "dbd" => (
	is => "rw"
);
has "dbo" => (
	is => "rw",
	isa => "Wikia::DB"
);
has "debug" => (
	is => "rw",
	isa => "Int",
	default => 0
);
has "content_namespaces" => (
	is => "rw",
	isa => "ArrayRef"
);
has "sql_page" => (
	is => "ro",
	isa => "Str",
	default => sub {
		return qq{
			SELECT 
				page_id, 
				page_is_redirect, 
				page_latest, 
				page_title, 
				page_namespace, 
				count(rev_id) as edits, 
				max(rev_timestamp) as last_edited, 
				rev_user 
			FROM page 
			JOIN revision ON rev_page = page_id 
			GROUP BY page_id ORDER BY page_id		
		};
	}
);
has "wiki_pages" => (
	is => "rw",
	isa => "HashRef"
);
has "sql_pages" => (
	is => "ro",
	isa => "Str",
	default => sub {
		return qq{
			SELECT 
				page_id, 
				page_status, 
				page_latest, 
				page_title, 
				page_title_lower, 
				page_namespace, 
				page_is_redirect, 
				page_is_content, 
				page_edits, 
				page_last_edited
			FROM dataware.pages 
			WHERE page_wikia_id = ?
		};
	}
);
has "dataware_pages" => (
	is => "rw",
	isa => "HashRef"
);
has "remove_pages" => (
	is => "rw",
	isa => "ArrayRef"
);
has "add_pages" => (
	is => "rw",
	isa => "ArrayRef"
);
sub _build_content_namespaces {
	my ( $self ) = @_;
	my @content_namespaces = ();
	
	say "Set content namespaces" if $self->debug;
		
	if ( defined $self->row->{cv_value} ) {
		my $cv_value  = unserialize( $self->row->{cv_value} );
		if ( ref( $cv_value ) eq 'HASH' ) {
			@content_namespaces = values %{$cv_value};
		} elsif ( ref($cv_value) eq 'ARRAY' ) {
			@content_namespaces = @{$cv_value};
		}
	}
	
	if ( scalar @content_namespaces == 0 ) {
		@content_namespaces = ( 0 );
	}
	
	$self->content_namespaces( \@content_namespaces );
}

sub get_local_pages {
	my ( $self ) = @_;

	say "Get local pages" if $self->debug;
		
	unless ( $self->dbw ) {
		say "\tCannot connect to database: " . $self->dbname;
		return 0;
	}
	
	my %wiki_pages = ();
	my $sth = $self->dbw->prepare( $self->sql_page );
	if ( $sth->execute() ) {
		while ( my $row = $sth->fetchrow_hashref ) {
			%{ $wiki_pages{ $row->{ page_id } } } = (
				'latest' 		=> $row->{ page_latest },
				'title' 		=> strtolower( $row->{ page_title } ),
				'nspace'		=> $row->{ page_namespace },
				'realtitle'		=> $row->{ page_title },
				'exists' 		=> 1,
				'is_redirect'	=> $row->{ page_is_redirect },
				'is_content'	=> int grep /^\Q$row->{page_namespace}\E$/, @{ $self->content_namespaces },
				'edits' 		=> $row->{ edits } || 0,
				'last_edited'	=> $row->{ last_edited },
				'rev_user' 		=> $row->{ rev_user }
			);
		}
		$sth->finish();
	}
	
	$self->wiki_pages( \%wiki_pages );
	say "\tFound " . scalar( keys( %{ $self->wiki_pages } ) ) . " pages in Wikia page table";
}

sub get_dataware_pages {
	my ( $self ) = @_;
	
	say "Get pages from dataware" if $self->debug;
	
	unless ( $self->dbd ) {
		say "\tCannot connect to dataware database";
		return 0;
	}
	
	my %dataware_pages = ();
	my @remove_pages = ();

	my $sth = $self->dbd->prepare( $self->sql_pages );
	if ( $sth->execute( $self->id ) ) {
		while ( my $row = $sth->fetchrow_hashref ) {
			%{ $dataware_pages{ $row->{ page_id } } } = (
				'title'	 		=> $row->{ page_title },
				'status' 		=> $row->{ page_status }, 
				'latest' 		=> $row->{ page_latest }, 
				'lower_title'	=> $row->{ page_title_lower },
				'nspace' 		=> $row->{ page_namespace },
				'is_redirect'	=> $row->{ page_is_redirect },
				'is_content'	=> $row->{ page_is_content },
				'edits'			=> $row->{ page_edits } || 0,
				'last_edited'	=> $row->{ page_last_edited }
			);
			
			push @remove_pages, $row->{ page_id } unless ( $self->wiki_pages->{ $row->{ page_id } } );
		}
		$sth->finish();
	}

	$self->remove_pages( \@remove_pages );
	$self->dataware_pages( \%dataware_pages );
	say "\tFound " . scalar( keys( %{ $self->dataware_pages } ) ) . " pages in pages table";
	say "\tFound " . scalar @{ $self->remove_pages } . " pages to delete";
}

sub update_pages {
	my ( $self ) = @_;

	say "Update pages" if $self->debug;
	
	my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::DATAWARESHARED );
	my $dbo = new Wikia::DB( { dbh => $dbh } );
	$self->dbo( $dbo );
	
	my $s_dbh = new Wikia::DB( { dbh => $self->dbd } );

	foreach ( @{ $self->add_pages } ) {
		if ( $self->wiki_pages->{ $_ } ) {
			my $insert_data = {
				"page_wikia_id" 	=> $self->id,
				"page_id" 			=> $_,
				"page_namespace"	=> $self->wiki_pages->{ $_ }->{ nspace },
				"page_title_lower"	=> $self->wiki_pages->{ $_ }->{ title },
				"page_title" 		=> fixutf( $self->wiki_pages->{ $_ }->{ realtitle } ),
				"page_status" 		=> 0,
				"page_is_content"	=> $self->wiki_pages->{ $_ }->{ is_content } || 0,
				"page_is_redirect"	=> $self->wiki_pages->{ $_ }->{ is_redirect } || 0,
				"page_edits"		=> $self->wiki_pages->{ $_ }->{ edits } || 0,
				"page_latest"		=> $self->wiki_pages->{ $_ }->{ latest } || 0,
				"page_last_edited"	=> $self->wiki_pages->{ $_ }->{ last_edited }		
			};
			my $on_update = [
				"page_status = values(page_status)",
				"page_latest = values(page_latest)",
				"page_edits = values(page_edits)",
				"page_last_edited = values(page_last_edited)",
				"page_is_content = values(page_is_content)",
				"page_is_redirect = values(page_is_redirect)"
			];
			my $ins_options = [ "ON DUPLICATE KEY UPDATE " . join ',', @{ $on_update } ];					
			$self->dbo->insert( 'pages', "", $insert_data, $ins_options, 1 );
		}
	}
	
	foreach ( @{ $self->remove_pages } ) {
		my %data = (
			"page_wikia_id" => $self->id, 
			"page_id" => $_ 
		);
		my $sql = $self->dbo->delete("pages", \%data);
	}
	
	# update image review table
	$self->image_review();
	
	$s_dbh->check_lag(15);
}

sub image_review {
	my ( $self ) = @_;
	
	say "Update image_review table" if $self->debug;
	
	#--- image_review
	my $where = [ 
		"wiki_id = " . $self->id, 
		"top_200 = 1" 
	];
	my $options = [ ' LIMIT 1 ' ];
	my $row = $self->dbo->select("top_200", "image_review", $where, $options);
	my $top_200 = $row->{top_200} || 0;
	
	#--- update image_review table
	say "\tUpdate image_review dataware";
	my $ir = 0;
	my $sql = qq(
		SELECT page_wikia_id, dp.page_id, page_latest, page_last_edited 
		FROM pages dp
		LEFT JOIN image_review on wiki_id = page_wikia_id 
		WHERE 
			page_wikia_id = ? AND 
			page_namespace = ? 
			AND page_title_lower REGEXP '.(png|bmp|gif|jpg|ico|svg)\$' 
			AND wiki_id is null
	);

	my $sth = $self->dbo->handler()->prepare( $sql );
	if ( $sth->execute( $self->id, 6 ) ) {
		while ( my $row = $sth->fetchrow_hashref ) {
			my $insert_data = {
				wiki_id		=> $row->{ page_wikia_id },
				page_id		=> $row->{ page_id },
				revision_id	=> $row->{ page_latest },
				user_id		=> $self->wiki_pages->{ $row->{ page_id } }->{ rev_user } || 0,
				last_edited	=> $row->{ page_last_edited },
				top_200		=> $top_200
			};
		
			my $on_update = [
				"last_edited = values(last_edited)",
				"revision_id = values(revision_id)"
			];
			my $ins_options = [ " ON DUPLICATE KEY UPDATE " . join ",", @{ $on_update } ];
			if ( !$self->dbo->insert( 'image_review', "", $insert_data, $ins_options, 1 ) ) {
				say "\t\tCannot add record to image_review table" if ( $self->debug );
			} else {
				$ir++;
			}
		}
		$sth->finish();
	}
	say "\tAdded $ir records to the image_review table";
}

sub update {
	my ( $self ) = @_;	

	my $start_sec = time();
	
	say "Run update method ... ";

	$self->dbname( $self->row->{city_dbname} );
	$self->id ( $self->row->{city_id} );
	
	# check page table on Wikia
	$self->get_local_pages();
	
	# check pages table on dataware
	$self->get_dataware_pages();
	
	if ( $self->wiki_pages ) {
		say "\tCompare records from page & pages tables to find differences";
		my $loop = 0;
		my @records = ();
		foreach my $page_id ( sort keys %{ $self->wiki_pages } ) {
			my $need_update = 0;
			if ( defined $self->dataware_pages->{ $page_id } ) {
				if ( $need_update = ( $self->dataware_pages->{ $page_id }->{ title } ne $self->wiki_pages->{ $page_id }->{ realtitle } ) ) {
					say "\t\tFound differences between titles for page: " . $page_id if ( $self->debug );
				} 
				elsif ( $need_update = ( $self->dataware_pages->{ $page_id }->{ nspace } ne $self->wiki_pages->{ $page_id }->{ nspace } ) ) {
					say "\t\tFound differences between namespaces for page: " . $page_id if ( $self->debug );
				} 
				elsif ( $need_update = ( $self->dataware_pages->{ $page_id }->{ is_content } ne $self->wiki_pages->{ $page_id }->{ is_content } ) ) {
					say "\t\tFound differences between content namespaces for page: " . $page_id if ( $self->debug );
				} 
				elsif ( $need_update = ( $self->dataware_pages->{ $page_id }->{ is_redirect } ne $self->wiki_pages->{ $page_id }->{ is_redirect } ) ) {
					say "\t\tFound differences between redirects for page: " . $page_id if ( $self->debug );
				}
			} else {
				say "\t\tPage: " . $page_id . " doesn't exist in pages table on dataware" if ( $self->debug );
				$need_update = 1;
			}
			
			push @records, $page_id if ( $need_update );
		}
	
		# add (or/and remove) records to/from pages table 
		if ( scalar(@records) ) {
			$self->add_pages( \@records );
		}

		# update pages table
		$self->update_pages();

		say "\tAdded " . scalar @records . " records to pages table";
		say "\tRemove " . scalar( @{ $self->remove_pages } ) . " records from pages table";
	}
	
	my $end_sec = time();
	my @ts = gmtime($end_sec - $start_sec);
	say $self->dbname . " processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);
}

no Moose;

package main;

use Wikia::Log;
use Thread::Pool::Simple;
use Data::Dumper;
use Getopt::Long;
use Pod::Usage;

my $ts_compare = 60 * 60 * 24 * 3;

GetOptions(
	'workers=s' 	=> \( my $workers = 2 ),
	'city_id=i'		=> \( my $cityid = 0 ),
	'fromid=i'		=> \( my $fromid = 0 ),
	'toid=i'		=> \( my $toid = 0 ),
	'debug'			=> \( my $debug = 0 ),
	'regen'			=> \( my $regenerate = 0 ),
	'help'			=> \( my $help = 0 )
) or pod2usage( 2 );

pod2usage( 1 ) if $help;

say "Starting script ... \n";
my $script_start_time = time();

my $pool = Thread::Pool::Simple->new(
	min => 1,
	max => $workers,
	load => 2,
	do => [sub { 
		my ( $tid, $row ) = @_;
		say "Run thread $tid" if $debug;
		say "Update pages for row: " . Dumper( $row ) if $debug;
		my $obj = new UpdatePages( row => $row, debug => $debug );
		if ( $obj ) {
			$obj->update(); 
		}
	}],
	monitor => sub {
		print "done \n";
	},
	passid => 1,
);

# load balancer
my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::EXTERNALSHARED );
my @where_db = ('city_id > 0');
if ($cityid) {
	push @where_db, "city_id = ".$cityid;
}
if ($fromid) {
	push @where_db, "city_id >= ".$fromid;
}
if ($toid) {
	push @where_db, "city_id <= ".$toid;
}

say "get last timestamp of run script";
my $log = Wikia::Log->new( name => "update_pages" );
my $ts = $log->select();
my $ts_script = ( $ts->{ts_log} || time() ) - $ts_compare;

say "get list of Wikis";
my $condition = join " AND ", @where_db;
my $sql = "
	SELECT city_id, city_dbname, cv_value, UNIX_TIMESTAMP(city_last_timestamp) as ts_edit 
	FROM city_list 
	LEFT JOIN city_variables ON cv_city_id = city_id and cv_variable_id = 359 
	WHERE city_public = 1 AND $condition 
	ORDER BY city_last_timestamp DESC, city_id DESC
";
my $sth = $dbh->prepare( $sql );
my $loop = 1;
if ( $sth->execute() ) {
	while ( my $row = $sth->fetchrow_hashref ) {
		# skip some Wikis ... 
		next if ( !$regenerate && $row->{ts_edit} < $ts_script );
		# ... amd run for others
		say "Run thread for Wikia: " . $row->{ city_dbname } . " ( " . $row->{city_id} . " )";
		my $tid = $pool->add( $row );
		say "Thread $tid started \n" if ($debug);
		$loop++;
	}
	$sth->finish();
} else {
	print "SQL error: " . $sth->errstr;
}
say "Wait until all threads finish ... \n";
$pool->join();

$log->update();

my $script_end_time = time();
my @ts = gmtime($script_end_time - $script_start_time);

$dbh->disconnect() if ( $dbh );

print "Process done: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);
1;
__END__

=head1 NAME

update_pages.pl - update pages and image_review tables on dataware

=head1 SYNOPSIS

update_pages.pl [options]

 Options:
	--workers=s		number of thread workers
	--city_id=i		run script for some Wiki
	--fromid=i		run script for Wikis with city_id more than fromid
	--toid=i		run script for Wikis with city_id less than toid
	--debug			use debug mode
	--regen			re-generate data for all Wikis
	--help			show this help

=head1 OPTIONS

=over 8

=item B<--help>

Print a brief help message and exits.

=head1 DESCRIPTION

B<This programm> update all data in pages table and add missing records to the image_review table
=cut
