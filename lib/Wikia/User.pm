package Wikia::User;

use Wikia::WikiFactory;
use Wikia::LB;
use Wikia::ExternalLB;
use Wikia::Utils;
use Data::Dumper;
use Compress::Zlib;
use PHP::Serialization qw/serialize unserialize/;
use DateTime;
use Moose;

=head1 NAME

Wikia::User - MediaWiki User class for Wikia scripts

=head1 VERSION

version 0.01

=head1 SYNOPSIS

  use Wikia::User;

  #
  # get user information from starwars database
  #
  my $user = new Wikia::User( db => "starwars", id => 1 );

  #
  # get user name
  #
  my $user_name = $rev->user_name;


=cut

has "db"				=> ( is => "rw", "isa" => "Str",  required => 1 );
has "id"				=> ( is => "rw", "isa" => "Int",  required => 1 );
has "city_id"			=> ( is => "rw", "isa" => "Int",  lazy_build => 0 );
has "name"				=> ( is => "rw", "isa" => "Str",  lazy_build => 1 );
has "real_name"			=> ( is => "rw", "isa" => "Str",  lazy_build => 1 );
has "email"				=> ( is => "rw", "isa" => "Str",  lazy_build => 1 );
has "options"			=> ( is => "rw", "isa" => "HashRef",  lazy_build => 1 );
has "token"				=> ( is => "rw", "isa" => "Str",  lazy_build => 1 );
has "touched"			=> ( is => "rw", "isa" => "Str",  lazy_build => 1 );
has "email_auth"		=> ( is => "rw", "isa" => "Str",  lazy_build => 1 );
has "registration"		=> ( is => "rw", "isa" => "Str",  lazy_build => 1 );
has "last_edited"		=> ( is => "rw", "isa" => "Str",  lazy_build => 1 );
has "last_edited_rev" 	=> ( is => "rw", "isa" => "Int",  lazy_build => 1 );
has "blocked"			=> ( is => "rw", "isa" => "Int", lazy_build => 1 );
has "closed"			=> ( is => "rw", "isa" => "Int", lazy_build => 1 );
has "edits"				=> ( is => "rw", "isa" => "Int",  lazy_build => 1 );
has "groups"			=> ( is => "rw", "isa" => "ArrayRef",  lazy_build => 1 );

has "dbh" => (
	is            => "rw",
	lazy_build    => 1,
	documentation => "DBI database handler"
);

has "dbc" => (
	is            => "rw",
	lazy_build    => 1,
	documentation => "DBI shared (wikicities) database handler"
);

has "dbs" => (
	is            => "rw",
	lazy_build    => 1,
	documentation => "DBI stats database handler"
);

has "row" => (
	is            => "rw",
	default       => undef,
	documentation => "database row with user definition"
);

has "row_ipblock" => (
	is            => "rw",
	default       => undef,
	documentation => "database row with user ipblock"
);

has "row_stats" => (
	is            => "rw",
	default       => undef,
	documentation => "database row with user stats"
);

has "array_groups" => (
	is            => "rw",
	default       => undef,
	documentation => "array with user groups"
);

has "master" => (
	is            => "rw",
	isa           => "Bool",
	default       => 0,
	documentation => "set to true/1 if master connection is used for reading user data"
);


=head1 METHODS

=head2 _load_user

	load user from database

=cut
sub _load_user {
	my ( $self ) = @_;

	unless( defined $self->row ) {
		my $dbc = $self->dbc;
		my $sth = $dbc->prepare( qq{SELECT * FROM user WHERE user_id = ? LIMIT 1} );
		$sth->execute( $self->id );
		my $row = $sth->fetchrow_hashref;

		#
		# if row doesn't exists use master connection
		#
		if( !exists( $row->{"user_id" } ) && ! $self->master ) {
			$self->master( 1 );
			$self->_build_dbc();
			$dbc = $self->dbc;
			my $sth = $dbc->prepare( qq{SELECT * FROM user WHERE user_id = ? LIMIT 1} );
			$sth->execute( $self->id );
			my $row = $sth->fetchrow_hashref;
		}
		$self->row( $row );
	}
}

sub _load_user_stats {
	my ( $self ) = @_;
	
	unless( defined $self->row_stats ) {
		$self->_load_city_id();
		$self->_build_dbs();
		
		my $dbs = $self->dbs;			
		my $sth = $dbs->prepare( qq{SELECT * FROM `specials`.`events_local_users` WHERE wiki_id = ? and user_id = ? LIMIT 1} );
		$sth->execute( $self->city_id, $self->id );
		my $row_stats = $sth->fetchrow_hashref;
		$self->row_stats($row_stats);
	}
}

sub _load_user_ipblock {
	my ( $self ) = @_;
	
	unless ( defined $self->row_ipblock ) {
		$self->_build_dbh();
		my $dbh = $self->dbh;
		my $sth = $dbh->prepare( qq{SELECT * FROM ipblocks WHERE ipb_user = ? and (ipb_deleted IS NULL OR ipb_deleted = 0) and ipb_auto = 0 LIMIT 1 } );
		$sth->execute( $self->id );
		my $row_block = $sth->fetchrow_hashref;
			
		$self->row_ipblock($row_block);
	}
}

sub _load_user_groups {
	my ( $self ) = @_;
	
	unless( defined $self->array_groups ) {
		$self->_load_city_id();
		$self->_build_dbc();
		$self->_build_dbh();
		
		my $dbc = $self->dbc;
		my $dbh = $self->dbh;
		
		# central groups
		my @groups = ();
		my $wgWikiaGlobalUserGroups = $self->_global_groups();
		my $sth = $dbc->prepare( qq{SELECT * FROM user_groups WHERE ug_user = ? } );
		$sth->execute( $self->id );
		while (my $row = $sth->fetchrow_hashref()) {
				push @groups, $row->{ug_group} if ( grep /^\Q$row->{ug_group}\E$/, @{$wgWikiaGlobalUserGroups} );
		}
		$sth->finish();
		
		# local groups
		$sth = $dbh->prepare( qq{SELECT * FROM user_groups WHERE ug_user = ? } );
		$sth->execute( $self->id );
		while (my $row = $sth->fetchrow_hashref()) {
			push @groups, $row->{ug_group};
		}
		$sth->finish();
				
		@groups = sort @groups;
				
		$self->array_groups(\@groups);
	}
}

sub _load_city_id {
	my ( $self ) = @_;
	
	unless ( $self->city_id ) {
		my $wikiFactory = Wikia::WikiFactory->new( city_dbname => $self->db );
		$self->city_id($wikiFactory->city_id);	
	}
}

=head2 _build_name

	lazy builder for $rev->name -- user name 

=cut
sub _build_name {
	my ( $self ) = @_;

	$self->_load_user;
	my $name = defined $self->row && exists $self->row->{ 'user_name' } ? $self->row->{ 'user_name' } : '';
	$self->name( $name );
}

=head2 _build_real_name

	lazy builder for $rev->real_name -- user real name

=cut
sub _build_real_name {
	my ( $self ) = @_;

	$self->_load_user;
	$self->real_name( defined $self->row && exists $self->row->{ 'user_real_name' } ? $self->row->{ 'user_real_name' } : undef );
}

=head2 _build_email

	lazy builder for $rev->email -- user email

=cut
sub _build_email {
	my ( $self ) = @_;

	$self->_load_user;
	$self->email( defined $self->row && exists $self->row->{ 'user_email' } ? $self->row->{ 'user_email' } : undef );
}

=head2 _build_options

	lazy builder for $rev->options -- user options

=cut
sub _build_options {
	my ( $self ) = @_;

	$self->_load_user;
	my $us_options = $self->row->{ 'user_options' } ? $self->row->{ 'user_options' } : undef;
	my %options = ();
	if ( defined $us_options ) {
		my @rows = split /\n/, $us_options;
		if ( scalar @rows ) {
			foreach my $row ( @rows ) {
				my ( $key, $value ) = split /\=/, $row ;
				if ( $key && $value ) {
					$options{$key} = $value;
				}
			}
		}
	} else {
		# properties
		$self->_build_dbc();
		my $dbc = $self->dbc;		
		my $sth = $dbc->prepare( qq{SELECT * FROM user_properties WHERE up_user = ? } );
		$sth->execute( $self->id );
		while (my $row = $sth->fetchrow_hashref()) {
			$options{$row->{up_property}} = $row->{up_value};
		}
		$sth->finish();		
	}
	$self->options( \%options );
}

=head2 _build_token

	lazy builder for $rev->token -- user token

=cut
sub _build_token {
	my ( $self ) = @_;

	$self->_load_user;
	$self->token( defined $self->row && exists $self->row->{ 'user_token' } ? $self->row->{ 'user_token' } : undef );
}

=head2 _build_user_touched

	lazy builder for $rev->touched -- user touched

=cut
sub _build_touched {
	my ( $self ) = @_;

	$self->_load_user;
	$self->touched( defined $self->row && exists $self->row->{ 'user_touched' } ? $self->row->{ 'user_touched' } : undef );
}

=head2 _build_email_auth

	lazy builder for $rev->email_auth -- user email authentication

=cut
sub _build_email_auth {
	my ( $self ) = @_;

	$self->_load_user;
	$self->email_auth( defined $self->row && exists $self->row->{ 'user_email_authenticated' } ? $self->row->{ 'user_email_authenticated' } : undef );
}

=head2 _build_user_registration

	lazy builder for $rev->registration -- user registration

=cut
sub _build_registration {
	my ( $self ) = @_;

	$self->_load_user;
	$self->registration( defined $self->row && exists $self->row->{ 'user_registration' } ? $self->row->{ 'user_registration' } : undef );
}

=head2 _build_edits

	lazy builder for $rev->edits -- user registration

=cut
sub _build_edits {
	my ( $self ) = @_;
	
	$self->_load_user_stats;
	$self->edits( defined $self->row_stats && exists $self->row_stats->{ 'edits' } ? $self->row_stats->{ 'edits' } : 0 );
}

=head2 _build_last_edited

	lazy builder for $rev->last_edited -- user last edited timestamp

=cut
sub _build_last_edited {
	my ( $self ) = @_;
	
	$self->_load_user_stats;
	$self->last_edited( defined $self->row_stats && exists $self->row_stats->{ 'editdate' } ? $self->row_stats->{ 'editdate' } : undef );
}

=head2 _build_last_edited_rev

	lazy builder for $rev->last_edited_rev -- user last edited revision id

=cut
sub _build_last_edited_rev {
	my ( $self ) = @_;
	
	$self->_load_user_stats;
	$self->last_edited_rev( defined $self->row_stats && exists $self->row_stats->{ 'last_revision' } ? $self->row_stats->{ 'last_revision' } : undef );
}

=head2 _build_blocked

	lazy builder for $rev->blocked -- user is blocked

=cut
sub _build_blocked {
	my ( $self ) = @_;
	
	$self->_load_user_ipblock;
	$self->blocked( defined $self->row_ipblock && exists $self->row_ipblock->{ 'ipb_user' } ? 1 : 0 );
}

=head2 _build_closed

	lazy builder for $rev->closed -- user account is closed

=cut
sub _build_closed {
	my ( $self ) = @_;
	
	$self->_load_user;
	$self->closed( defined $self->row && exists $self->row->{ 'user_real_name' } ? Wikia::Utils->intval( $self->row->{ 'user_real_name' } eq "Account Disabled" ) : 1 );
}

=head2 _build_groups

	lazy builder for $rev->groups -- user groups

=cut
sub _build_groups {
	my ( $self ) = @_;
	
	$self->_load_user_groups;
	my $groups = UNIVERSAL::isa($self->array_groups, 'ARRAY') ? $self->array_groups : undef;
	$self->groups( $groups );
}

sub _build_dbh {
	my ( $self ) = @_;
	my $dbh = undef;
	if( $self->master ) {
		$dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, $self->db );
	}
	else {
		$dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, $self->db );
	}
	$self->dbh( $dbh ) if $dbh;
}

sub _build_dbc {
	my ( $self ) = @_;
	my $dbc = undef;
	if( $self->master ) {
		$dbc = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::EXTERNALSHARED );
	}
	else {
		$dbc = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::EXTERNALSHARED );
	}
	$self->dbc( $dbc ) if $dbc;
}

sub _build_dbs {
	my ( $self ) = @_;
	my $dbs = undef;
	if( $self->master ) {
		$dbs = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS );
	}
	else {
		$dbs = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::STATS );
	}
	$self->dbs( $dbs ) if $dbs;
}

=head2 _set_option
	
	set user option
	
=cut

sub set_option {
	my ( $self, $option, $value ) = @_;
	
	$self->master( 1 );
	$self->_build_dbc();
	
	my $dbc = new Wikia::DB( {"dbh" => $self->dbc } );	
	
	my @ins_options = ( " ON DUPLICATE KEY UPDATE up_value = values(up_value) " );
	my %data = (
		"up_user"      => $self->id,
		"up_property"  => $option,
		"up_value"     => $value
	);
	my $res = $dbc->insert( 'user_properties', "", \%data, \@ins_options, 1 );
	
	return $res;
}

=head2 _set_stats

	increase edits

=cut
sub _set_stats {
	my ( $self, $data ) = @_;
	
	$self->master( 1 );
	$self->_build_dbs();
	
	my $dbs = new Wikia::DB( {"dbh" => $self->dbs } );	
	
	my @options = ();
	my @where = (
		"wiki_id = " . $dbs->quote( $data->{'wiki_id'} ),
		"user_id = " . $dbs->quote( $data->{'user_id'} )
	);
	my $oRow = $dbs->select(
		" wiki_id, user_id, edits ",
		'`specials`.`events_local_users`',
		\@where,
		\@options
	);
	my $cnt = $oRow->{user_id} || 0;
	
	my $res = undef;
	if ( $cnt == 0 ) { 
		# insert 
		$res = $dbs->insert( '`specials`.`events_local_users`', "", $data );
	} else { 
		# update
		$data->{edits} = $oRow->{edits} + $data->{edits};
		$res = $dbs->update( '`specials`.`events_local_users`', \@where, $data );
	}
	
	return $res;
}

=head2 _global_groups

	increase edits

=cut
sub _global_groups {
	my ( $self ) = @_;
	
	my @wgWikiaGlobalUserGroups = ();

	my $server = 'http://community.wikia.com/';
	#api.php?action=query&meta=siteinfo&siprop=general|namespaces|statistics|variables|category|wikidesc';
	my $params = {
		'action' => 'query',
		'meta'   => 'siteinfo',
		'siprop' => 'variables',
		'format' => 'json'
	};	
	my $response = Wikia::Utils->call_mw_api($server, $params, 0, 0); 
	if ( defined $response  && $response->{query} ) {
		my $variables = $response->{query}->{variables};
		if ( scalar @$variables ) {
			foreach my $var ( @$variables ) {
				if ( $var->{id} && ( $var->{id} eq 'wgWikiaGlobalUserGroups' ) ) {
					foreach my $key ( keys %$var ) {
						if ( UNIVERSAL::isa( $var->{$key},'HASH' ) ) {
							push @wgWikiaGlobalUserGroups, $var->{$key}->{'*'};
						}
					}
				}
			}
		}
	} else {
		@wgWikiaGlobalUserGroups = ('staff', 'helper', 'vstf', 'beta', 'checkuser-global');
	}	
	return \@wgWikiaGlobalUserGroups;
}

=head2 user_cluster

	check user exist on cluster in wikicities_CLUSTER table
	
=cut
sub user_exists_cluster {
	my ( $self, $cluster ) = @_;
	
	$self->master( 1 );
	my $db = sprintf("wikicities_%s", $cluster);
	$self->db($db);
	$self->_build_dbh();
	
	my $row = undef;
	if ( $self->dbh ) {
		my $sth = $self->dbh->prepare( qq{SELECT * FROM user WHERE user_id = ? LIMIT 1} );
		$sth->execute( $self->id );
		$row = $sth->fetchrow_hashref;
	}
	
	return ( defined ($row) && $row->{user_id} == $self->id );
}

=head2 copy_to_cluster

	copy record from central to cluster
	
=cut
sub copy_to_cluster {
	my ( $self, $cluster ) = @_;
	
	$self->master( 1 );
	my $db = sprintf("wikicities_%s", $cluster);
	$self->db($db);
	$self->_build_dbh();
	
	$self->_build_dbc();
	
	my $row = undef;
	my $result = 0;
	if ( $self->dbc && $self->dbh ) {
		my $sth = $self->dbc->prepare( qq{SELECT * FROM user WHERE user_id = ? LIMIT 1} );
		$sth->execute( $self->id );
		$row = $sth->fetchrow_hashref;
		
		my %fields = ();
		if ( defined ($row) ) {
			my ($names,$values);

			my $allowed_keys = ['user_id', 'user_name'];
			my @duplicate = ();
			foreach (keys %$row) {
				my $key = $_;
				$values.= " ".$self->dbh->quote($row->{$_}).","; 
				$names .= "$_,";
				if ( !Wikia::Utils->in_array( $key, $allowed_keys ) ) {
					push @duplicate, sprintf("%s = %s", $key, 'values('.$key.')');
				}
			}
			chop($names); chop($values);
			my $q = "INSERT IGNORE INTO user ($names) VALUES ($values) "; 
			$q .= "ON DUPLICATE KEY UPDATE " . join (",", @duplicate);

			$result = $self->dbh->do($q);
			if ( !$result ) {
				print ("ERRROR: " . $q . " - " .$self->dbh->errstr. "\n");
				return $result;
			}		
		}
	}
	
	return $result;
}

1;
