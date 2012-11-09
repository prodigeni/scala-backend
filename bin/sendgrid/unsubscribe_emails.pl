#!/usr/bin/perl

use common::sense;
use feature "say";

use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";

#
# private
#
use Wikia::DB;
use Wikia::Settings;
use Wikia::User;
use Wikia::Utils;
use Wikia::LB;

#
# public
#

use LWP::UserAgent;
use Data::Dumper;
use Pod::Usage;
use Getopt::Long;

package main;

my ( $help, $pref, $delete ) = undef;

$|++;        # switch off buffering
GetOptions(
	"help|?"  => \$help,
	"pref=s"  => \$pref,
	"delete"  => \$delete
) or pod2usage( 2 );

pod2usage( 1 ) if $help;
pod2usage( 1 ) unless ( $pref );

my $settings = Wikia::Settings->instance();

our $username = $settings->variables()->{ "wgSendGridUser" }->{ "username" };
our $password = $settings->variables()->{ "wgSendGridUser" }->{ "password" };

# the real junk
my $api_list_uri = 'https://sendgrid.com/api/unsubscribes.get.json?api_user='.$username.'&api_key='.$password;
my $api_unsubscribe_uri ='https://sendgrid.com/api/unsubscribes.delete.json?api_user='.$username.'&api_key='.$password.'&email';

my @preferences = split /,/, $pref;

my $ua = LWP::UserAgent->new;
my $res = $ua->get($api_list_uri);

if ($res->is_success) {
	my $response = Wikia::Utils->json_decode( $res->content );
	
	foreach ( @$response ) {
		say "got email: " . $_->{email} ;
		my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::CENTRALSHARED );
		my %users;
		my $sth = $dbh->prepare( qq{SELECT user_id, user_name FROM user WHERE user_email = ?} );
		$sth->execute( $_->{email} );
		while( my $row = $sth->fetchrow_hashref ) {
			$users{ $row->{ "user_id" } } = $row->{ "user_name" };
		}
		
		if ( scalar keys %users ) {
			foreach my $user_id ( sort keys %users ) {
				my $user = new Wikia::User( db => Wikia::LB::CENTRALSHARED, id => $user_id );
				
				say "\twork with [". $user->name ."] (#". $user->id .")";
				my $options = $user->options;
				
				foreach ( @preferences ) {
					if ( defined $options->{$_} ) {
						say "\t\tunset " . $_ . " option";
						$user->set_option( $_, '0' );
					}
				}
			}
		} else {
			say "\tno user found with email: " . $_->{email};
		}
		
		# unsubscribe user
		if ( $delete ) {
			my $unsub_url = sprintf( "%s=%s", $api_unsubscribe_uri, $_->{email} );
			my $request = $ua->get($unsub_url);
			if ( $request->is_success ) {
				if ( $request->content =~ /success/ ) {
					say "\temail " . $_->{email} . " was unsubscribed via SendGrid WebApi";
				} else {
					say "\temail" . $_->{email} . " was not able to be unsubscribed (1).";
				}	
			} else {
				say "\temail" . $_->{email} . " was not able to be unsubscribed (2).";
			}	
		}	
	}
} else {
	say $res->content;
	die "Unable to connect to Sendgrid Web-API.\n";
}

say "\nScript finished";

1;
__END__

=head1 NAME

unsubscribe_emails.pl - fetch list of unsubscribe requests from SendGrid and uncheck those users preferences

=head1 SYNOPSIS

unsubscribe_emails.pl [options]

 Options:
  --help            brief help message
  --pref=<pref1,[pref2,...,prefn]>    comma separated list of preferences to uncheck

=head1 OPTIONS

=over 8

=item B<--help>

Print a brief help message and exits.

=head1 DESCRIPTION

B<This programm> call SendGrid Web-APO to fetch list of unsubscribe requests and uncheck those users preferences
=cut
