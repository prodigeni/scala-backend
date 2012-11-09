package Wikia::MailQueue;

use strict;
use common::sense;

use Wikia::LB;
use Wikia::Settings;

use Moose;
use Data::Types qw(:int);
use Net::SMTP::SSL;
use Email::Valid;
use Data::Dumper;

has "settings" => ( 
	is => "rw", 
	isa => "Wikia::Settings", 
	default => sub { return Wikia::Settings->instance; } 
);
has "groups"  => ( 
	is => "rw", 
	isa => "HashRef", 
	lazy_build => 1 
);
has "config"  => ( 
	is => "rw", 
	isa => "HashRef", 
);
has "queue"   => ( 
	is => "rw", 
	isa => "Str", 
	required => 1, 
	trigger => sub { my ( $self, $queue ) = @_; $self->_build_config(); $self->_build_dbh(); } 
);
has "dbh"     => ( 
	is => "rw",
	lazy_build => 0 
);
has "record"  => (
	is => "rw",
	lazy_build => 0
);
has "debug"   => (
	is => "rw",
	lazy_build => 0,
	default => 0
);
has "columns"   => (
	is => "rw",
	isa => "ArrayRef"
);

sub _build_groups {
	my ( $self ) = @_;
	say "load groups" if ( $self->debug );
	$self->groups( $self->settings->variables()->{ "wgEmailSendGridDBGroups" } );
}

sub _build_config {
	my ( $self ) = @_;
	say "load config" if ( $self->debug );
	$self->config( $self->settings->variables()->{ "wgEmailSendGridConfig" } );
}

sub _build_dbh {
	my ( $self ) = @_;
	say "load dbh - host: " . $self->groups->{ $self->queue }->{ "host" } if ( $self->debug );
	#$self->dbh( Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, $self->queue, $self->groups->{ $self->queue }->{ "host" }, Wikia::LB::MAILER ) );
	$self->dbh( Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, 'wikia_mailer' ) );
}

sub pop {
	my( $self ) = @_;

	my $sth = $self->dbh->prepare( qq{SELECT * FROM mail WHERE locked IS NULL ORDER BY priority DESC, created DESC LIMIT 1} );
	$sth->execute();
	# columns 
	my @cols = @{$sth->{NAME}};
	# rows
	my $row = $sth->fetchrow_hashref();
	$sth->finish;
	# 
	if ( defined $row ) {
		$row->{TABLE} = \@cols;
		$self->record( $row );
		# lock record
		my $item = $self->lock();
	}

	return $row;
}

sub lock {
	my ( $self ) = @_;
	my $sth = $self->dbh->prepare( qq{UPDATE mail SET locked = NOW(), attempted = IFNULL(attempted, NOW()) WHERE id = ?} );
	$sth->execute( $self->record->{ "id" } );
}
		
sub unlock {
	my ( $self, $error ) = @_;
	my $sth = $self->dbh->prepare( qq{UPDATE mail SET locked = NULL, is_error = 0 WHERE id = ?} );
	$sth->execute( $self->record->{ "id" } );
}

sub error {
	my ( $self, $error ) = @_;
	if ( $self->record->{"is_error"} && $self->record->{"is_error"} < 3 ) {
		my $sth = $self->dbh->prepare( qq{UPDATE mail SET locked = NULL, is_error = 1 WHERE id = ?} );
		$sth->execute( $self->record->{ "id" } );
	} else {
		$self->finish();
	}
}

sub finish {
	my ( $self ) = @_;
	my $cols = join ( ",", grep { $_ ne 'transmitted' } @{ $self->record->{TABLE} } );
	$self->dbh->{PrintError} = 1;
	my $q = sprintf( 'INSERT INTO mail_send ( %s,transmitted ) SELECT %s,now() from mail WHERE id = ? LIMIT 1', $cols, $cols );
	my $sth = $self->dbh->prepare( $q );
	if ( !$sth->execute( $self->record->{ "id" } ) ) {
		say "Cannot copy email to the backup table: " . $self->record->{ "id" };
		$self->error();
	} else {
		$sth = $self->dbh->prepare( qq{UPDATE mail SET transmitted = now() WHERE id = ?} );
		$sth->execute( $self->record->{ "id" } );
	}
}

sub cleanup {
	my ( $self ) = @_;
	my $sth = $self->dbh->prepare( qq{DELETE FROM mail WHERE locked IS NOT NULL AND transmitted IS NOT NULL} );
	$sth->execute();
	
	$sth = $self->dbh->prepare( qq{UPDATE mail SET locked = NULL, is_error = 1 WHERE locked IS NOT NULL AND attempted IS NOT NULL AND transmitted IS NOT NULL} );
	$sth->execute();
}

package Wikia::MailQueueWorker;

use strict;
use common::sense;

use Wikia::LB;
use Wikia::Settings;

use Moose;
use Data::Dumper;
use JSON::XS;

extends 'Wikia::MailQueue';

has "smtp"    => ( 
	is => "rw", 
	isa => "Net::SMTP::SSL",
	lazy_build => 0
);

sub _build_smtp {
	my ( $self ) = @_;
	
	# init SD smtp connection
	my $smtp = Net::SMTP::SSL->new ( 
		$self->config->{ "host"}, 
		Port  => $self->config->{ "port" }, 
		Debug => $self->config->{ "debug" }, 
		Hello => $self->config->{ "domain" }
	) || die "SMTP Error $!:" . __LINE__;

	$smtp->auth( 
		$self->groups->{ $self->queue }->{ "sglogin" } , 
		$self->groups->{ $self->queue }->{ "sgpassword" } 
	) || die "SMTP Error $!:" . __LINE__;

	$self->smtp( $smtp );
}

sub parse_status {
	my ( $self, $status ) = @_;
	
	if ( defined $status->{unlock} ) {
		$self->unlock();
	} 
	elsif ( defined $status->{error} ) { 
		$self->error( $status->{error} );
	} 
	elsif ( defined $status->{done} ) {
		$self->finish();
	}
}

sub run {
	my ( $self ) = @_;
	
	my ( $category, $servername, $token );
	my $status = {};
	
	say "Parse " .  $self->record->{id};
	
	say "Header: " . $self->record->{hdr} . "\n" if ( $self->debug );
	# category
	$category = $1 if ( $self->record->{hdr} =~s/X-Msg-Category: (\S+)// );
	# servername
	$servername = $1 if ( $self->record->{hdr} =~s/X-ServerName: (\S+)// );
	# token
	$token = $1 if ( $self->record->{hdr} =~s/X-CallbackToken: (\S+)// );
	# remove blank lines
	$self->record->{hdr} =~ s/^\s*\n+//mg;
	say "Cleaned header: " . $self->record->{hdr} . " \n" if ( $self->debug );

	# test
	#$self->record->{hdr} =~ s/To: (.*)/To: moli\@kofeina.net/;
	#print "\n\nheader: " . $self->record->{hdr} . "\n\n";

	# validate and correct for common issues with mail addresses (-fudge)
	my ( $mailTo, $mailFrom );
	eval {
		my $mailTo   = Email::Valid->address( "-address" => $self->record->{dst}, "-fudge" => 1 );
		my $mailFrom = Email::Valid->address( "-address" => $self->record->{src}, "-fudge" => 1 );
		
		if ( !$mailTo || !$mailFrom ) {
			say "Skipping invalid address: to " . $self->record->{dst} . " from " . $self->record->{src};
			die "Invalid email";
		} else {
			my $api = {
				'category'    => $category || "Unknown",
				'unique_args' => {
					'wikia-db'            => $self->config->{ "host" },
					'wikia-email-id'      => $self->record->{id},
					'wikia-email-city-id' => $self->record->{city_id},
					'wikia-token'         => $token || "",
				}
			};
			
			$api->{'unique_args'}->{'wikia-server-name'} = $servername if ( $servername );

			say "Init sendgrid connection";
			$self->_build_smtp();
			
			say "Sending " . $self->record->{id} . ", to: " . $self->record->{dst};
			unless ( $self->smtp->mail( $mailFrom ) ) { 
				say "from: $mailFrom invalid"; 
				$status->{unlock} = 1;
			} else {
				unless ( $self->smtp->to($mailTo) ) { 
					say "to: $mailTo invalid"; 
					$status->{unlock} = 1;
				} else {
					$self->smtp->data() 
						|| die "SMTP Error $!:" . __LINE__;
					$self->smtp->datasend( "X-SMTPAPI: " . encode_json( $api ) . " \n" );
					$self->smtp->datasend( "X-Wikia-Id: " . $self->record->{city_id} . ":" . $self->record->{id} . "\n" );
					$self->smtp->datasend( $self->record->{hdr} ) 
						|| die "SMTP Error $!:" . __LINE__;
					$self->smtp->datasend("\r\n\r\n") 
						|| die "SMTP Error $!:" . __LINE__;
					$self->smtp->datasend($self->record->{msg}) 
						|| die "SMTP Error $!:" . __LINE__;
					$self->smtp->dataend() 
						|| die "SMTP Error $!:" . __LINE__;
					$self->smtp->quit 
						|| die "SMTP Error $!:" . __LINE__;
				}
			}
		}
	};
	
	if ( $@ ) {
		my $error = "Error: $@";
		say "Error: $@";
		$status->{error} = $error;
		$self->parse_status( $status );
	} else {
		$status->{done} = 1 unless $status->{unlock};
		$self->parse_status( $status );
	}
}

__PACKAGE__->meta->make_immutable;
1;
