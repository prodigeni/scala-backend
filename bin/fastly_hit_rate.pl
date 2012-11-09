#!/usr/bin/perl -w

use strict;

=pod

=head1 Author

Sean Colombo
Garth Webb

=head1 Date

20120612

=head1 Description

Calculates the fastly hit-rate averaged across all the datacenters and prints
that hit-rate w/some details.

=cut

use JSON;
use Getopt::Long;

my ($csv, $header);
GetOptions('--csv'    => \$csv,
		   '--header' => \$header,
		  );

if ($header) {
	print "date,";
	print "e_hits,e_miss,e_pass,e_cacheable,e_total,";
	print "s_hits,s_miss,s_pass,s_cacheable,s_total,";
	print "a_hits,a_miss,a_pass,a_cacheable,a_total,";
	print "t_hits,t_miss,t_pass,t_cacheable,t_total\n";
	exit;
}

my $SERVICE_ID = "5NzYW6HIKNZhcSUjVHUzWP";
my $FASTLY_API_KEY = "6322c8a587efb64f2f449beef06bb510";

# Valid opts {"minutely", "hourly", "daily", "all"}.  "all" is too slow w/big
# sites, don't do that :P
my $period = "daily"; 

my %SHIELD_CACHES = (SJC  => 1,
					 SJC2 => 1,
					 IOW  => 1,
					 IOWA => 1
					);


my $json = JSON->new->allow_nonref;

print STDERR "Requesting data ... " unless $csv;
my $fastlyJsonString = `curl --silent https://api.fastly.com/service/$SERVICE_ID/stats/$period -l -H "X-Fastly-Key: $FASTLY_API_KEY"`;
print STDERR "done\n" unless $csv;

#print "RAW FASTLY STRING\n";
#print "$fastlyJsonString\n";

print STDERR "Decoding data ... " unless $csv;
my $data = $json->decode($fastlyJsonString);
print STDERR "done\n" unless $csv;
print STDERR "\n" unless $csv;

my %shield_stats = (hits => 0, misses => 0, requests => 0, pass => 0);
my %edge_stats = (hits => 0, misses => 0, requests => 0, pass => 0);

# Collect stats from each varnish
foreach my $key (sort keys %$data){
	my $stats = $data->{$key};
	my $store;

	# Separate shield varnish stats from edge stats 
	if (exists $SHIELD_CACHES{uc($key)}) {
		$store = \%shield_stats;
	} else {
		$store = \%edge_stats;
	}

	$store->{hits}     += $stats->{hits};
	$store->{miss}     += $stats->{miss};
	$store->{pass}     += $stats->{pass};
	$store->{requests} += $stats->{requests};

	if (!$csv) {
		printf("%6s: - hits: %15s - misses: %15s\n",
			   $key, commify($stats->{'hits'}), commify($stats->{'miss'}));
	}
}

# Derive old style overall average stats
my %ave = (miss     => ($edge_stats{miss} + $shield_stats{miss}),
		   hits     => ($edge_stats{hits} + $shield_stats{hits}),
		   pass     => ($edge_stats{pass} + $shield_stats{pass}),
		   requests => ($edge_stats{requests} + $shield_stats{requests}),
	  	);

# Derive actual system miss and hit rates based on what reaches the apaches
my $cache_traffic = $edge_stats{hits} + $edge_stats{miss};
my %system = (miss     => $shield_stats{miss},
			  hits     => ($cache_traffic - $shield_stats{miss}),
			  pass     => $edge_stats{pass},
			  requests => $edge_stats{requests},
			 );

if ($csv) {
	csv_output(\%edge_stats, \%shield_stats, \%system, \%ave);
} else {
	screen_output(\%edge_stats, \%shield_stats, \%system, \%ave);
}

################################################################################

sub csv_output {
	my ($edge_stats, $shield_stats, $system, $ave) = @_;
	my @t = gmtime;
	$t[5] += 1900;
	$t[4]++;

	printf("%04d-%02d-%02d %02d:%02d:%02d,", @t[5,4,3,2,1,0]);
	print_csv($edge_stats);
	print ',';

	print_csv($shield_stats);
	print ',';

	print_csv($ave);
	print ',';

	print_csv($system);
	print "\n";
}

sub screen_output {
	my ($edge_stats, $shield_stats, $system, $ave) = @_;

	print "\n";
	print "== TOTALS FOR $period ==\n";

	print "-- Edge Caches --\n";
	print_stats($edge_stats);

	print "\n";
	print "-- Shield Caches --\n";
	print_stats($shield_stats);

	print "\n";
	print "-- System Average (old metric) --\n";
	print_stats($ave);

	print "\n";
	print "-- System Total --\n";
	print_stats($system);
	print "\n";
}

sub commify {
	my ($num) = @_;
	$num =~ s/(^[-+]?\d+?(?=(?>(?:\d{3})+)(?!\d))|\G\d{3}(?=\d))/$1,/g;
	return $num;
}

sub print_csv {
	my ($stats) = @_;
	print join(',', $stats->{hits}, $stats->{miss}, $stats->{pass},
					$stats->{hits} + $stats->{miss}, $stats->{requests});
}

sub print_stats {
	my ($stats) = @_;

	printf "\t    HITS: %15s (%5.2f%% of cacheable)\n",
		   commify($stats->{hits}),
		   commify($stats->{hits} * 100 / ($stats->{hits} + $stats->{miss}));

	printf "\t    MISS: %15s (%5.2f%% of cacheable)\n",
		   commify($stats->{miss}),
		   commify($stats->{miss} * 100 / ($stats->{hits} + $stats->{miss}));

	printf "\t    PASS: %15s (%5.2f%% of total)\n",
		   commify($stats->{pass}),
		   commify($stats->{miss} * 100 / ($stats->{requests}));
	printf "\t---------\n";
	printf "\tREQUESTS\n";
	printf "\tCACHABLE: %15s\n", commify($stats->{hits} + $stats->{miss});
	printf "\t   TOTAL: %15s\n", commify($stats->{requests});
}