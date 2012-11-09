#!/usr/bin/perl                                                                 

while (<STDIN>) {
    my $line = $_;
    chomp($line);

    my @kvs = split(/,/, $line);

    foreach my $p (@kvs) {
        my @kv = split(/=/, $p);
        print $kv[0] . "\t" . $kv[1] . "\n";
    }
}
