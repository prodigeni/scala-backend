#!/usr/bin/perl -w

#
# options
#
use common::sense;
use feature "say";

use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";

#
# private
#
use Wikia::DB;
use Wikia::LB;

#
# public
#
use Pod::Usage;
use Getopt::Long;
use Time::HiRes qw(gettimeofday tv_interval);
use Thread::Pool::Simple;
use Data::Dumper;

=table
CREATE TABLE `const_values` (
  `name` varchar(50) NOT NULL,
  `val` int(8) unsigned NOT NULL,
  PRIMARY KEY (`name`)
) ENGINE=INNODB DEFAULT CHARSET=latin1
=cut

$|++;  
 GetOptions(
	"help|?"    => \( my $help    = 0 ),
	"workers=i" => \( my $workers = 5 )
) or pod2usage( 2 );
pod2usage( 1 ) if $help;

my $CONST_VALUES = {
	"content_ns" => {
		'query' => "select count(*) as cnt from pages where page_namespace not in (2, 4, 6, 8, 10, 12, 14, 400, 700, 1000, 1010, 1200, 1202) and page_namespace % 2 = 0",
		'db' => Wikia::LB::DATAWARESHARED,
		'value' => 'cnt'
	}
};

sub worker {
	my( $worker_id, $key, $task ) = @_;

	my $dbh = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, $task->{db} );
	
	my $sth = $dbh->prepare( $task->{ query } );
	$sth->execute();
	
	my $val = 0;
	if ( my $row = $sth->fetchrow_hashref ) {
		$val = $row->{ $task->{ value } }
	}
	
	if ( $val > 0 ) {
		my $dbs = Wikia::LB->instance->getConnection( Wikia::LB::DB_MASTER, undef, Wikia::LB::STATS );
		my $sth = $dbs->prepare( qq{INSERT INTO specials.const_values ( name, val ) VALUES ( ?, ? ) ON DUPLICATE KEY UPDATE val = VALUES( val ) } );
		$sth->execute( $key, $val );
	}
	
	return $val;
}

my $process_start_time = time();

my $pool = Thread::Pool::Simple->new(
	min => 1,
	max => $workers,
	load => 4,
	do => [sub {
		worker( @_ );
	}],
	monitor => sub {
		say "done";
	},
	passid => 1,
);

foreach my $key ( sort keys %{$CONST_VALUES} ) {
	say "Run $key";
	
	$pool->add( $key, $CONST_VALUES->{ $key } );
}

$pool->join;

my $process_end_time = time();
my @ts = gmtime($process_end_time - $process_start_time);
say "\nscript processed ".sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]) ;

1;
__END__

=head1 NAME

const_values.pl - generate some const values into specials.const_values table

=head1 SYNOPSIS

const_values.pl [options]

 Options:
  --help            brief help message
  --workers=<nr>    how many workers should be spawned (default 10)

=head1 OPTIONS

=over 8

=item B<--help>

Print a brief help message and exits.

=head1 DESCRIPTION

B<This programm> iterates through all task in %CONST_VALUES hash and run it
=cut
