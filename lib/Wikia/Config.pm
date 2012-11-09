package Wikia::Config;
use strict;
use base qw(Class::Accessor);

use MIME::Lite;
    
Wikia::Config->mk_accessors(qw(logfile csvfile));

sub log ($$;$) {
	my ($self, $text, $tofile) = @_;
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
	
	if ( $tofile ) {
		open (INFILE, ">>".$self->logfile);
		print INFILE sprintf ("%4.4d-%2.2d-%2.2d %2.2d:%2.2d:%2.2d",$year+1900,$mon+1,$mday,$hour,$min,$sec)."\t".$text."\n";
		close (INFILE);
	} else {
		my $msg = "\t".$text."\n";
		print $msg;
	}
}

sub output_csv {
	my ($self, $text) = @_;

	open (CSVFILE, ">>".$self->csvfile);
	print CSVFILE $text."\n";
	close (CSVFILE);
}

sub send_file {
	my ($self, $from, $subject, $emails) = @_;
	
	if ( $emails ) {
		my @emails = split(",", $emails);
		if ( scalar @emails ) {
			foreach (@emails) {
				my $msg = MIME::Lite->new(
					From     => $from,
					To       => $_,
					Subject  => $subject,
					Path     => $self->csvfile
				);
				$msg->send;
				print "send email $subject to " . $_ . " \n";
			}
		}
	}	
}

1;
__END__
