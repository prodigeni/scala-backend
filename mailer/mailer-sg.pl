#!/usr/bin/perl



use strict;
use warnings;
use Sys::Hostname qw (hostname);
use Email::Valid;
use DBI;
$|++;
use DBD::mysql;

my $username = shift;
my $password = shift;
my $host = shift;
my $sg_id = shift;
my $sg_pw = shift;
my $id = hostname . "-$$";

my $dbh = DBI->connect("DBI:mysql:database=wikia_mailer;host=$host", $username, $password, {RaiseError => 1, AutoCommit => 0});
use JSON::XS;

my $lock = $dbh->prepare("UPDATE mail SET locked_by = ?, locked = NOW(), attempted=IFNULL(attempted, NOW()) WHERE locked_by IS NULL AND transmitted IS NULL ORDER BY priority desc LIMIT 100");
my $jobs = $dbh->prepare("SELECT * FROM mail WHERE locked_by = ?");
my $update = $dbh->prepare("UPDATE mail SET locked_by = NULL, locked = NULL, transmitted = NOW() WHERE id = ?");
my $failed= $dbh->prepare("UPDATE mail SET locked_by = NULL, locked = NULL, transmitted = 0, is_error = 1 WHERE id = ?");
my $unlock = $dbh->prepare("UPDATE mail SET locked_by = NULL, locked = NULL WHERE locked_by = ?");
my $cleanup = $dbh->prepare("DELETE from mail WHERE transmitted < NOW() - INTERVAL 1 WEEK");

while(1) {

    eval {
        $lock->execute($id);
        $jobs->execute($id);
        $dbh->commit;
    };
    if ($@) {
        print "Waiting for another process: $@\n";
        $dbh->rollback;
        sleep 1;
        next;
    }

    eval {
        while(my $ref = $jobs->fetchrow_hashref()) {
            my ($category, $servername, $token);
            print "\n#before#\n" . $ref->{hdr} . "\n#before#\n";
            if($ref->{hdr} =~s/X-Msg-Category: (\S+)//) {
                $category = $1;
            }
            if ($ref->{hdr} =~s/X-ServerName: (\S+)//) {
                $servername =  $1;
            }
            if ($ref->{hdr} =~s/X-CallbackToken: (\S+)//) {
                $token = $1;
            }
            $ref->{hdr} =~ s/^\s*\n+//mg;  # remove blank lines
            print "\n#after#\n" . $ref->{hdr} . "\n#after#\n";

            # validate and correct for common issues with mail addresses (-fudge)
            my $mailTo = Email::Valid->address( "-address" => $ref->{dst}, "-fudge" => 1);
            my $mailFrom = Email::Valid->address( "-address" => $ref->{src}, "-fudge" => 1);
            if (!$mailTo || !$mailFrom) {
                print "Skipping invalid address: $ref->{dst} $ref->{src}\n"; 
                $failed->execute($ref->{id});
                $dbh->commit;
                next;
            }

            my $api = {};
            $api->{category} = $category || "Unknown";
            $api->{unique_args}->{"wikia-db"} = $host;
            $api->{unique_args}->{"wikia-email-id"} = $ref->{id};
            $api->{unique_args}->{"wikia-email-city-id"} = $ref->{city_id};
            $api->{unique_args}->{"wikia-server-name"} = $servername if $servername;
            $api->{unique_args}->{"wikia-token"} = $token || "";

            print "Sending $ref->{id}\n";
            my $smtp = get_smtp();
            unless ( $smtp->mail($mailFrom) ) { print "$mailFrom invalid\n"; $failed->execute($ref->{id}); $dbh->commit; next; }
            unless ( $smtp->to($mailTo) ) { print "$mailTo invalid\n"; $failed->execute($ref->{id}); $dbh->commit; next; }
            $smtp->data() || die "SMTP Error $!:" . __LINE__;
            $smtp->datasend("X-SMTPAPI: " . encode_json($api). " \n");
            $smtp->datasend("X-Wikia-Id: $ref->{city_id}:$ref->{id}\n");
            $smtp->datasend($ref->{hdr}) || die "SMTP Error $!:" . __LINE__;
            $smtp->datasend("\r\n\r\n") || die "SMTP Error $!:" . __LINE__;

            $smtp->datasend($ref->{msg}) || die "SMTP Error $!:" . __LINE__;
            $smtp->dataend() || die "SMTP Error $!:" . __LINE__;
            $smtp->quit || die "SMTP Error $!:" . __LINE__;
            $update->execute($ref->{id});
            $dbh->commit;
        }
	$cleanup->execute();
	$dbh->commit;
    };
    if ($@) {
        print "Error sending mail $@\n";
        $unlock->execute($id);
        $dbh->commit;
        sleep 10;
        next;
    }
    print "Sleeping\n";
    sleep 3;
}

use Net::SMTP::SSL;
sub get_smtp {
    my $smtp = Net::SMTP::SSL->new("smtp.sendgrid.net", Port => 465,  Debug => 3, Hello => "mail.wikia.com") || die "SMTP Error $!:" . __LINE__;
    $smtp->auth($sg_id, $sg_pw) || die "SMTP Error $!:" . __LINE__;
    return $smtp;
}

1;






