#!/usr/bin/perl

my $current_beacon;
my @data = ();

my $domains = ['facebook.com', 'twitter.com'];
my $check = "(" . join("|", @$domains) . ")";
my $wikia = 509;

my $is_ok = 0;
while (<STDIN>) {
	chomp();
	my ($city, $lang, $langid, $dbname, $cl, $user, $article, $namespace, $referrer, $beacon, $ts) = split(/\t/);
	next unless ( $beacon );
	
	if ( $current_beacon ne $beacon ) {
		$is_ok = 0;
		$current_beacon = $beacon;
	}
	
	$user = 0 unless ( $user );
	if ( $referrer =~ /$check/ ) {
		$is_ok = 1;
	}

	if ( $is_ok ) {
		print $referrer . "\t" . $current_beacon . "\t" . $city . "\t" . $article . "\t" . $user . "\t" . $ts . "\n";   
	}
}

1;
