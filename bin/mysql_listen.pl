#!/usr/bin/perl -w

use Socket;

use Getopt::Long;

$|=1;
my $limit = 100000;
my $help = undef;
my $port = 3306;
my $top = 50;

GetOptions(
	"help|?"	=> \$help,
	"port=i"	=> \$port,
	"top=i"		=> \$top,
	"limit=i"	=> \$limit
) or pod2usage( 2 );

pod2usage( 1 ) if $help;

my $start_sec = time();	

my %method = ();
my %ip = ();
my %tables = ();
open (STDIN,"/usr/bin/tshark -i 1 -T text -V -f 'dst port 3306' |grep -i 'statement: ' | cut -b20- |");
my $loop;
while (<>) {
	chomp; if (  m/\/\*\s*(.*)\:\:(.*)\s{1}(.*)\s{1}URL\:\s{1}\*\/(.*)`(.*)`/i ) {
		$method{$1."::".$2}++; $ip{ ($3)?$3:"no IP" }++; $tables{$5}++;
	}
	$loop++;
	if ( $loop > $limit ) {
		my $tloop = 0;
		
		# methods
		print "\n\n\nMW methods: \n";
		foreach $key ( sort {$method{$b} <=> $method{$a}} ( keys( %method ) ) ) {
			print $key. "\t" . $method{$key}."\n";
			$tloop++;
			last if ( $tloop == $top );
		}
		
		# ips
		$tloop=0;
		print "\n\n\nIP: \n";
		foreach $key ( sort {$ip{$b} <=> $ip{$a}} ( keys( %ip ) ) ) {
			print $key. "\t" . $ip{$key}."\n";
			$tloop++;
			last if ( $tloop == $top );
		}
		
		#tables
		print "\n\n\nTables: \n";
		foreach $key ( sort {$tables{$b} <=> $tables{$a}} ( keys( %tables ) ) ) {
			print $key. "\t" . $tables{$key}."\n";
			$tloop++;
			last if ( $tloop == $top );
		}
		last;
	}
}

my $end_sec = time();
my @ts = gmtime($end_sec - $start_sec);
print "Done after " .  sprintf ("%d hours %d minutes %d seconds\n",@ts[2,1,0]);	

1;
__END__

=head1 NAME

mysql_listen.pl - read all mysql queries and count MW methods and IPs

=head1 SYNOPSIS

mysql_listen.pl [options]

 Options:
  --help            brief help message
  --limit=<limit>	number of queries to parse
  --port=<port>     listen on port (default 3306)
  --top=<top>		display TOP methods and IPs

=head1 OPTIONS

=over 8

=item B<--help>

Print a brief help message and exits.

=head1 DESCRIPTION

B<This programm> will read all mysql queries and count MW methods and IPs.
=cut

