package Wikia::DW::EC2::S3;

use strict;
use warnings;

use Capture::Tiny ':all';

sub get {
    my ($s3file, $localfile) = @_;
    my ($stdout, $stderr, $result) = capture {
        my $s3cmd = "s3cmd --force --no-progress get $s3file $localfile";
        scalar system($s3cmd);
    };
    die "FAILED: s3cmd get\n" if ($stderr);
}

sub put {
    my ($localfile, $s3file) = @_;
    my ($stdout, $stderr, $result) = capture {
        my $s3cmd = "s3cmd --force --no-progress put $localfile $s3file";
        scalar system($s3cmd);
    };
    die "FAILED: s3cmd put\n" if ($stderr);
}

1;
