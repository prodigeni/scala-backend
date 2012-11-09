package Wikia::Search::Indexer;

use Moose;
use Data::Dumper;
use Wikia::LB;
use Wikia::DB;
use Wikia::WikiFactory;
use Wikia::Nirvana;
use Wikia::Search::AmazonCS;
use Wikia::Utils;
use IPC::Open2;
use Encode;
use Devel::Size qw(size total_size);
use MooseX::Types::JSON qw( JSON );
use JSON::XS;
use File::Slurp;
use common::sense;
use HttpSQS;


has "city_id" => ( is => "rw", isa => "Int", trigger => sub {
	my ( $self ) = @_;
	my $wf = Wikia::WikiFactory->new( city_id => $self->city_id );

	if( defined $wf->city_dbname ) {
		$self->city_dbname( $wf->city_dbname );
		$self->city_url( $wf->variables->{'wgServer'} );
	}
});
has "city_dbname" => ( is => "rw", isa => "Str", trigger => sub {
	my ( $self ) = @_;
	$self->dbh;
});
has "city_url" => ( is => "rw" );
has "dbh" => ( is => "rw", lazy_build => 1 );
has "dbr" => ( is => "rw" );
has "db_shared" => ( is => "rw", lazy_build => 1 );
has "db_stats" => ( is => "rw", lazy_build => 1 );
has "master" => ( is => "rw", isa => "Bool", default => 0, documentation => "set to true/1 if master connection is used for reading revision data" );
has "limit" => ( is => "rw", isa => "Int", default => 100 );
has "offset" => ( is => "rw", isa => "Int", default => 0 );
has "page_id" => ( is => "rw", isa => "Int", default => 0 );
has "max_index_limit" => ( is => "rw", isa => "Int", default => 1000 );
has "max_index_limit_kb" => ( is => "rw", isa => "Int", default => 10240 );
has "variables" => ( is => "rw", isa => "ArrayRef[Int]" );
has "worker_id" => ( is => "rw", isa => "Str", default => 0 );
has "documents_path" => ( is => "rw", isa => "Str", default => '' );
has "to_file" => ( is => "rw", isa => "Str", default => '' );
has "queue_host" => ( is => "rw", isa => "Str", required => 1, default => '10.8.34.21' );
has "queue_port" => ( is => "rw", isa => "Int", required => 1, default 	=> 1218 );
has "queue" => ( is => "rw", isa => "HttpSQS", default 	=> sub {
	my $self = shift;
	return HttpSQS->new( $self->queue_host, $self->queue_port, 'tcp', 'utf-8');
});
has "parse_queue_event" => (
	is			=> "rw",
	isa			=> JSON,
	trigger 	=> sub {
		my ($self, $event) = @_;
		my $evt = decode_json( $event );
print Dumper $evt;
	}
);


sub _build_dbh {
	my ( $self ) = @_;
	my $db = undef;

	if( $self->master ) {
		$db = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, $self->city_dbname );
	}
	else {
		$db = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, $self->city_dbname );
	}

	$self->dbh( $db ) if $db;
	$self->dbr( new Wikia::DB( { "dbh" => $self->dbh } ) );
};

sub _build_db_shared {
	my ( $self ) = @_;
	my $db = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::EXTERNALSHARED );

	$self->db_shared( new Wikia::DB( { "dbh" => $db } ) );
};

sub _build_db_stats {
	my ( $self ) = @_;
	my $db = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS );

	$self->db_stats( new Wikia::DB( { "dbh" => $db } ) );
};

sub get_content_namespaces {
	my ( $self ) = @_;

	return $self->db_shared->__content_namespaces( $self->city_id );
};

sub get_wikis {
	my ( $self ) = @_;

	my @where = ( 'in_full_index_ts IS NULL' );
	my @options = ( 'ORDER BY in_city_id' );

	if( $self->limit > 0 ) {
		push( @options, 'LIMIT ' . $self->limit );
	}

	my %wikis = ();
	my $sth = $self->db_stats->select_many("in_city_id, in_full_index_ts", "noreptemp.indexer", \@where, \@options);

	if ($sth) {
		while(my ($city_id, $full_index_ts) = $sth->fetchrow_array()) {
			%{$wikis{$city_id}} = ( 'full_index_ts' => $full_index_ts );
		}
		$sth->finish();
	}

	return \%wikis;
}

sub update_wiki_full_index_ts {
	my ( $self ) = @_;

	my @where = ( "in_city_id='" . $self->city_id . "'" );
	my %data = (
		'-in_full_index_ts' => 'NOW()'
	);

	$self->db_stats->update( "noreptemp.indexer", \@where, \%data );
}

sub get_wiki_pages {
	my ( $self ) = @_;

	my @where = ( 'page_namespace IN (' . join(',', @{$self->get_content_namespaces()} ) . ')  ' );
	my @options = ( 'ORDER BY page_id' );
	if( $self->limit > 0 ) {
		push( @options, 'LIMIT ' . $self->offset . ', ' . $self->limit );
	}
	if( $self->page_id > 0 ) {
		push( @where, 'page_id=' . $self->page_id );
	}

	my %pages = ();
	my $sth = $self->dbr->select_many("page_id, page_namespace, page_title", "page", \@where, \@options);

	if ($sth) {
		while(my ($page_id, $page_namespace, $page_title) = $sth->fetchrow_array()) {
			%{$pages{$page_id}} = ( 'title' => $page_title, 'ns' => $page_namespace );
		}
		$sth->finish();
	}
	return \%pages;
}

sub get_wiki_pages_num {
	my ( $self ) = @_;

	if( !defined $self->dbr ) {
		$self->log( "Wiki DB doesn't exists." );
		return 0;
	}

	my @where = ( 'page_namespace IN (' . join(',', @{$self->get_content_namespaces()} ) . ')  ' );
	my @options = ( 'ORDER BY page_id' );

	my $sth = $self->dbr->select("count(*) AS cnt", "page", \@where, \@options);

	return $sth->{'cnt'};
}

sub get_page {
	my ( $self, $page_id ) = @_;
}

sub index_wiki {
	my ( $self, $update_full_index_ts ) = @_;

	if( !defined $self->dbr ) {
		$self->log( "Wiki DB doesn't exists --- INDEXING SKIPPED! " );
		$self->update_wiki_full_index_ts();
		return 1;
	}

	my $pages = $self->get_wiki_pages();
	my $indexed_pages_cnt = 0;

	my @prepared_pages = ();

	$self->log( "START Indexing (LIMIT " .$self->offset . ", " . $self->limit . ")" );

	my $t_start = [ Wikia::Utils->current_time() ];

	foreach my $page_id ( keys %$pages ) {
		my $prepared_page = $self->prepare_wiki_page( $page_id, $pages->{$page_id}->{'ns'} );

		if ( !defined $prepared_page ) {
			#$self->log( "Error rendering page (ID=" . $page_id . ")" );
			next;
		}

		my $prepared_page_kb = ( total_size(\$prepared_page) / 1024 );

#		if( $prepared_page_kb >= Wikia::Search::IndexTank::MAX_DOCSIZE ) {
#			$self->log( "PageID=" . $page_id . " - document limit exceeded (" . $prepared_page_kb . " kB) --- NOT INDEXED!" );
#		}
#		else {
			push( @prepared_pages, $prepared_page );
#		}

		my $prepared_pages_kb = ( total_size(\@prepared_pages) / 1024 );

		if( ( scalar @prepared_pages >= $self->max_index_limit ) || ( $prepared_pages_kb >= $self->max_index_limit_kb ) ) {
			my $retries = 10;
			my $result = undef;
			do {
				$result = $self->index_wiki_pages( \@prepared_pages, $indexed_pages_cnt );

				if($result->{'status'} != '200') {
					$self->log( "Indexing failed, response code: " . $result->{'status'} . " (Time: " . $result->{'time'} . ")" );
					$self->log( "re-sending last batch..." );
					$retries--;
				}

				last unless $retries;
			}
			while( $result->{'status'} != '200' );

			if( $retries > 0 ) {
				$indexed_pages_cnt += ( scalar @prepared_pages );
				@prepared_pages = ();
				$self->log( "Pages indexed: " . $indexed_pages_cnt . " (Time: " . $result->{'time'} . " Docs Added: " . $result->{'content'}->{'adds'}. ")" );
			}
			else {
				$self->log( "Indexing Failed. --- EXITING" );
				return 1;
			}
		}
	}

	if( scalar @prepared_pages > 0 ) {
		my $result = $self->index_wiki_pages( \@prepared_pages, $indexed_pages_cnt );
		if($result->{'status'} != '200') {
			$self->log( "Indexing failed, response code: " . $result->{'status'}. " (Time: " . $result->{'time'} . ")" );
			return 1;
		}
		else {
			$indexed_pages_cnt += ( scalar @prepared_pages );
			$self->log( "Pages indexed: " . $indexed_pages_cnt . " (Time: " . $result->{'time'} . " Docs Added: " . $result->{'content'}->{'adds'}. ")" );
			
		} 
	}

	$self->log( "DONE Indexing (Total time=" . Wikia::Utils->interval_time( $t_start ) . " sec)" );
	
	if( $update_full_index_ts > 0 ) {
		$self->update_wiki_full_index_ts();
		$self->log( "full_index_ts updated." );
	}
	
	return 0;
}

sub log {
	my ( $self, $msg ) = @_;
	print "[Worker:" . $self->worker_id . ", CityID=" . $self->city_id . "] " . $msg . "\n";
}

sub index_wiki_pages {
	my ( $self, $pages, $count ) = @_;
	my $pages_json = encode_json $pages;

	my $t_start = [ Wikia::Utils->current_time() ];
	my $response = ();

	if( $self->to_file eq '' ) {
		my $amazon_cs = new Wikia::Search::AmazonCS();
		my $amazon_response = $amazon_cs->send_document_batch( $pages_json );

		$response->{'time'} = Wikia::Utils->interval_time( $t_start );
		$response->{'status'} = $amazon_response->code;
		if( $response->{'status'} == '200') {
			$response->{'content'} = decode_json $amazon_response->content;
		}
	}
	else {
		my $file_path = $self->documents_path;
		if( ( $self->to_file eq 'docid' ) && ( $self->page_id > 0 ) ) {
			$file_path .= '/c' . $self->city_id . 'p' . $self->page_id . ".json";
		}
		else {
			$file_path .= '/' . $self->to_file;
		}

		# save to file
		write_file( $file_path, {binmode => ':raw' }, $pages_json);

		$response->{'time'} = Wikia::Utils->interval_time( $t_start );
		$response->{'status'} = '200';
	}

	#print Dumper $response;
	return $response;
}

sub prepare_wiki_page {
	my ( $self, $page_id, $page_ns ) = @_;

	#my $t_start = [ Wikia::Utils->current_time() ];
	my $nirvana = new Wikia::Nirvana({ "wiki_url" => $self->city_url }); # "http://muppet.adi.wikia-dev.com/"
	my $response = $nirvana->send_request( "WikiaSearch", "getPage", { 'id' => $page_id } );
	#print "getPage() time=" . Wikia::Utils->interval_time( $t_start ) . "\n";

	if( $response->{'http_status'} != '200' ) {
		$self->log( "HTTP Status returned: " . $response->{'http_status'} . " (PageID=" . $page_id . ")" );
		return undef;
	}

	my $page_content = $self->generate_page_content( $response->{'text'} );
	if( !defined $page_content ) {
		return undef;
	}

	my $fields = {
		'cityid'    => $self->city_id,
		'title'     => $response->{'title'},
		'ns'        => $page_ns,
		'url'       => $response->{'url'},
		'canonical' => $response->{'canonical'},
		'sitename'  => $response->{'sitename'},
		'text'      => $page_content,
		'views'     => $response->{'metadata'}->{'views'},
		'backlinks' => $response->{'metadata'}->{'backlinks'}
	};

	my %prepared_page = (
		'type'      => 'add',
		'id'        => 'c' . $self->city_id . "p" . $page_id,
		'version'   => time(),
		'lang'      => 'en',
		'fields'    => $fields,
	);

	return \%prepared_page;
}

sub generate_page_content {
	my ( $self, $page_html ) = @_;

	return undef unless defined $page_html;

	# make text from the html, i18n happy
	my $pid = open2( *GET, *SEND, 'lynx -stdin -nolist -assume_charset=utf-8 -display_charset=utf-8 -dump' );
	syswrite( SEND, encode( "UTF-8", $page_html ) );
	close( SEND );

	my $text;
	while (sysread(GET, my $read, 4096)) {
		$text .= $read;
	}
	close( GET );
	waitpid($pid, 0);

	my $content = decode( "UTF-8", $text);
	$content =~ s/\s+/ /g;

	return $content;
}

sub updater_job {
	my ( $self, $qid ) = @_;
	
	my $status = $self->queue->status_json( $qid );
	
print Dumper $status;

	my $qinfo = decode_json ( $status );
	if ( ref( $qinfo ) ) {
		# read queue 
		say "$qid: parse " . $qinfo->{unread} . " unread messages ";
		#$self->queue_id( $qid );
		#$self->usejobs( {} );
		do {
			# parse message
print Dumper $self->queue->get( $qid );
			$self->parse_queue_event( $self->queue->get( $qid ) );
#print Dumper decode_json( $self->queue->get( $qid ) );
return 1;
			
			# check status
			$status = $self->queue->status_json( $qid );
			say "status = " . $status if ( $self->debug ) ;
			$qinfo = decode_json ( $status );			
		} while ( $qinfo->{unread} > 0 );

return 1;

		# reset queue
		say "$qid: clear queue";
		$self->queue->reset( $qid );
		# check status
		$status = $self->queue->status_json( $qid );
		say "status = " . $status if ( $self->debug ) ;
		$qinfo = decode_json( $status );			
	}
}

1;
