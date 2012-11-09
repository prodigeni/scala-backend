#!/usr/bin/perl -w

#
# options #test
#
use strict;
use common::sense;

use FindBin qw/$Bin/;
use lib "$Bin/../lib/";
use Data::Dumper;

#
# private
#
use Wikia::Settings;
use Wikia::WikiFactory;
use Wikia::Utils;
use Wikia::LB;
use Wikia::MailQueue;

#
# public
#
use MediaWiki::API;
use Pod::Usage;
use Getopt::Long;
use Thread::Pool::Simple;
use Time::HiRes qw(gettimeofday tv_interval);

package main;

=sql mail table

CREATE TABLE `mail_new` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `src` varchar(255) NOT NULL,
  `dst` varchar(255) NOT NULL,
  `subj` varchar(255) NOT NULL,
  `hdr` text NOT NULL,
  `msg` text NOT NULL,
  `city_id` int(11) NOT NULL,
  `priority` int(11) NOT NULL DEFAULT 0,
  `category` varchar(255) DEFAULT NULL,
  `locked_by` varchar(255) DEFAULT NULL,
  `locked` datetime DEFAULT NULL,
  `attempted` datetime DEFAULT NULL,
  `transmitted` datetime DEFAULT NULL,
  `is_bounce` tinyint(1) NOT NULL DEFAULT 0,
  `is_error` tinyint(1) NOT NULL DEFAULT 0,
  `is_spam` tinyint(1) NOT NULL DEFAULT 0,
  `error_status` varchar(255) DEFAULT NULL,
  `error_msg` varchar(255) DEFAULT NULL,
  `opened` datetime DEFAULT NULL,
  `clicked` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `dst` (`dst`),
  KEY `subj` (`subj`),
  KEY `city_id` (`city_id`),
  KEY `locked_by` (`locked_by`),
  KEY `created` (`created`),
  KEY `attempted` (`attempted`),
  KEY `transmitted` (`transmitted`),
  KEY `category` (`category`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1

CREATE TABLE `mail_send` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `src` varchar(255) NOT NULL,
  `dst` varchar(255) NOT NULL,
  `subj` varchar(255) NOT NULL,
  `hdr` text NOT NULL,
  `msg` text NOT NULL,
  `city_id` int(11) NOT NULL,
  `priority` int(11) NOT NULL DEFAULT '0',
  `category` varchar(255) DEFAULT NULL,
  `locked_by` varchar(255) DEFAULT NULL,
  `locked` datetime DEFAULT NULL,
  `attempted` datetime DEFAULT NULL,
  `transmitted` datetime DEFAULT NULL,
  `is_bounce` tinyint(1) NOT NULL DEFAULT '0',
  `is_error` tinyint(1) NOT NULL DEFAULT '0',
  `is_spam` tinyint(1) NOT NULL DEFAULT '0',
  `error_status` varchar(255) DEFAULT NULL,
  `error_msg` varchar(255) DEFAULT NULL,
  `opened` datetime DEFAULT NULL,
  `clicked` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `dst` (`dst`),
  KEY `subj` (`subj`),
  KEY `city_id` (`city_id`),
  KEY `locked_by` (`locked_by`),
  KEY `created` (`created`),
  KEY `attempted` (`attempted`),
  KEY `transmitted` (`transmitted`),
  KEY `transmitted_2` (`transmitted`,`locked_by`),
  KEY `category` (`category`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1

alter table mail_new add key `locked_prio` (locked, priority, created);
alter table mail_send add key `locked_prio` (locked, priority, created);
=cut

$|++;
GetOptions(
	"help|?"		=> \( my $help = 0 ),
	"qname=s"		=> \( my $qname ),
	"workers=i"		=> \( my $workers = 10 ),
	"debug"			=> \( my $debug = 0 )
) or pod2usage( 2 );

pod2usage( 1 ) if $help;
pod2usage( 1 ) unless $qname;

=item worker
=cut
my @clean = ();
my $queue = Wikia::MailQueue->new( "queue" => $qname, "debug" => $debug );
my $pool = Thread::Pool::Simple->new(
	min => 2,
	max => $workers,
	load => 2,
	do => [sub {
		my ( $qname, $record ) = @_;
		my $q = Wikia::MailQueueWorker->new( "queue" => $qname, "record" => $record, "debug" => $debug );
		$q->run();
	}],
	monitor => sub {
		say "done";
	}
);

my $i = 0;
while ( my $record = $queue->pop() ) {
	#print "record = " . Dumper( $record->{dst} ) . "\n";
	$pool->add( $qname, $record );
	$i++;
}
$pool->join;
$queue->cleanup() if ( $i > 0 );
say "Queue is empty.";
1;
__END__

=head1 NAME

mailer-q.pl - simple mailer queue

=head1 SYNOPSIS

mailer-q.pl [options]

 Options:
  --help            brief help message
  --workers=<nr>    how many workers should be run (default 10)
  --qname=<NAME>	name of queue to run

=head1 OPTIONS

=over 8

=item B<--help>

Print a brief help message and exits.

=head1 DESCRIPTION

B<This programm> will iterate by all email in mail table and send to the Sendgrid.
=cut
