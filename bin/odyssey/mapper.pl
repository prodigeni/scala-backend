#!/usr/bin/perl

use CGI;

while (<STDIN>) {
	chomp();
	my $query = CGI->new ( $_ ) ;

	my %result = ( 
		'c' => '',
		'lc' => '',
		'lid' => '',
		'x' => '',
		'y' => '',
		'u' => '',
		'a' => '',
		'n' => '',
		'r' => '',
		'beacon' => '',
		'lv' => ''
	);
	if ( defined($query->param) ) {
		foreach my $key ( $query->param ) {
			$result{$key} = $query->param( $key );
		}
	}

	print $result{'c'} . "\t" . $result{'lc'} . "\t" . $result{'lid'} . "\t" . $result{'x'} . "\t" . $result{'y'} . "\t" . $result{'u'} . "\t" . $result{'a'} . "\t" . $result{'n'} . "\t" . $result{'r'} . "\t" . $result{'beacon'} . "\t" . $result{'lv'} . "\n";	
}

1;
