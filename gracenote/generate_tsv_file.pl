#!/usr/bin/perl
package main;

use strict;
use common::sense;
use Data::Dumper;
use Pod::Usage;
use Getopt::Long;

use FindBin qw/$Bin/;
use lib "$Bin/../lib/";
use Wikia::LB;

GetOptions(
	'ns=s'		=> \( my $namespaces = "" ),
	'sdate=s' 	=> \( my $sdate = "" ),
	'edate=s'	=> \( my $edate = "" ),
	'file=s'	=> \( my $tsvfile = '' ),
	'help'		=> \( my $help = 0 )
) or pod2usage( 2 );

pod2usage( 1 ) if $help;
pod2usage( 1 ) unless ( $sdate && $edate && $tsvfile );

say "Starting script ... \n";
my $script_start_time = time();

# load balancer
my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, 'statsdb_tmp' );

my $sth = $dbh->prepare(
	qq{
		SELECT article_id, namespace_id, sum(pageviews) as pviews 
		FROM lyricwiki_quarterly_pageviews 
		WHERE month BETWEEN ? and ? AND namespace_id in (?) 
		GROUP BY 1, 2 
		ORDER BY 3 DESC
	}
);

my $loop = 1;
unlink( $tsvfile );
binmode(STDOUT, ":utf8");
if ( $sth->execute( sprintf("%s 00:00:00", $sdate), sprintf("%s 00:00:00", $edate), $namespaces ) ) {
	open(TSV, ">$tsvfile" );
	while ( my $row = $sth->fetchrow_hashref ) {
		print TSV sprintf("/GN4/%d/%d\t%d\n", $row->{namespace_id}, $row->{article_id}, $row->{pviews}); 
	}
	$sth->finish();
	close(TSV);
}

my $script_end_time = time();
my @ts = gmtime($script_end_time - $script_start_time);

$dbh->disconnect() if ( $dbh );

print "Process done: " . sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);
1;
__END__

=head1 NAME

generate_tsv_file.pl - genereate TSV file for gracenote reports

=head1 SYNOPSIS

generate_tsv_file.pl [options]

 Options:
  --help			brief help message
  --namespaces			comma separated list of namespaces
  --sdate=<YYYY-MM-DD>			begin date of report file
  --edate=<YYYY-MM-DD>			end date of report file

=head1 OPTIONS

=over 8

=item B<--help>

Print a brief help message and exits.

=head1 DESCRIPTION

B<This programm> will generate TSV file for Gracenote reports
=cut
