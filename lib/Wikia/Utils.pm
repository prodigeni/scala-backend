package Wikia::Utils;

use strict;

use common::sense;

use Exporter 'import';
our @EXPORT_OK = qw( note intval datetime TS_DB fixutf strtolower );

use Data::Dumper;
use DateTime;
use Mail::Sender;
use LWP::UserAgent;
use JSON::XS;
use MediaWiki::API;
use HTTP::Cookies;
use utf8;
use Encode;
use Compress::Raw::Zlib;
use Time::HiRes qw(gettimeofday tv_interval);

use base qw(Class::Accessor);

Wikia::Utils->mk_accessors(qw( ));

use constant NS_IMAGE => '6';
use constant NS_VIDEO => '400';

use constant TS_UNIX => 0;
use constant TS_MW => 1;
use constant TS_DB => 2;
use constant TS_RFC2822 => 3;
use constant TS_ISO_8601 => 4;
use constant TS_EXIF => 5;
use constant TS_ORACLE => 6;
use constant TS_POSTGRES => 7;
use constant TS_DB2 => 8;

# Get our script's name without any path components
our ($SCRIPT_NAME) = $0 =~ m!([^/]+)$!;

sub note {
	my (@str) = @_;
	local $|= 1;
	print $SCRIPT_NAME." ($$): ".join('', @str)."\n";
}

=to do
=cut
sub date_format {
	my ($self, $date, $format) = @_;
	$format = "%04d-%02d-%02d" unless $format;
	my $y = substr ($date,0,4);
	my $m = substr ($date,4,2);
	my $d = substr ($date,6,2);

	$date = sprintf ($format, $y, $m, $d);
	return ($date) ;
}

sub datetime_format {
	my ($self, $date, $format) = @_;
	$format = "%04d-%02d-%02d %02d:%02d:%02d" unless $format;
	my $y = substr ($date,0,4);
	my $m = substr ($date,4,2);
	my $d = substr ($date,6,2);
	my $h = substr ($date,8,2);
	my $i = substr ($date,10,2);
	my $s = substr ($date,12,2);

	$date = sprintf ($format, $y, $m, $d, $h, $i, $s);
	return ($date) ;
}

=item datetime

return DateTime object from given string

=cut
sub datetime {
	my( $self, $string, $format ) = @_;

	$format = TS_MW unless defined $format;

	my ( $y, $m, $d, $h, $i, $s ) = 0;

	given( $format ) {
		when( TS_MW ) {
			( $y, $m, $d, $h, $i, $s ) = $string =~ /(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})/;
		}
		when( TS_DB ) {
			( $y, $m, $d, $h, $i, $s ) = $string =~ /(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})/;
		}
	}

	return DateTime->new(
		year       => $y,
		month      => $m,
        day        => $d,
		hour       => $h,
        minute     => $i,
        second     => $s,
        nanosecond => 0,
        time_zone  => 'GMT',
	);
}

=to do
=cut
sub next_month {
	my ($self, $date, $format) = @_;
	$format = "%04d%02d00000000" unless $format;
	my $y = substr ($date,0,4);
	my $m= substr ($date,4,2);
	$m++; $m=1 if ($m > 12);
	$y++ if ($m == 1);
	$date = sprintf ("%04d%02d00000000", $y, $m);
	return ($date) ;
}

=to do
=cut
sub prev_month {
	my ($self, $date, $format) = @_;our @EXPORT    = qw(reversec seqlen);
our @EXPORT_OK = qw($EcoRI);
	$format = "%04d%02d00000000" unless $format;
	my $y = substr ($date,0,4);
	my $m= substr ($date,4,2);

	$m--; if ($m < 1) { $m=12; $y--; }
	$date = sprintf ($format, $y, $m);
	return ($date) ;
}

=to do
=cut
sub parse_article {
	my ($self, $article) = @_;
	# strip bold/italic formatting
	$article =~ s/\'\'+//go ;

	# strip <...> html
	$article =~ s/\<[^\>]+\>//go ;
	#     $article2 =~  s/[\xc0-\xdf][\x80-\xbf]|
	#                     [\xe0-\xef][\x80-\xbf]{2}|
	#                     [\xf0-\xf7][\x80-\xbf]{3}/{x}/gxo ;
	$article =~  s/[\xc0-\xf7][\x80-\xbf]+/{x}/gxo ;

	# count html chars as one char
	$article =~ s/\&\w+\;/x/go ;
	$article =~ s/\&\#\d+\;/x/go ;

	# strip image links
	# $article =~ s/\[\[ $imagetag \: [^\]]* \]\]//gxoi ;

	# strip interwiki links
	# $article =~ s/\[\[ .. \: [^\]]* \]\]//gxo ;
	#

	# strip image/category/interwiki links
	# a few internal links with colon in title will get lost too
	$article =~ s/\[\[ [^\:\]]+ \: [^\]]* \]\]//gxoi ;

	# strip external links
	$article =~ s/http \: [\w\.\/]+//gxoi ;
	#     $article4 = $article2 ; # move one down

	return $article;
}

=to do
=cut
sub clear_article {
	my ($self, $article) = @_;

	$article =~ s/\{x\}/x/g ;
	# strip headers
	$article =~ s/\=\=+ [^\=]* \=\=+//gxo ;
	# strip linebreaks + unordered list tags (other lists are relatively scarce)
	$article =~ s/\n\**//go ;
	# remove extra spaces
	$article =~ s/\s+/ /go ;

	return $article;
}

=to do
=cut
sub htmlchars {
	my ($self, $text) = @_;
	# unescape xml
	$text =~ s/</&lt;/sg;
	$text =~ s/>/&gt;/sg;
	#$text =~ s/'/&apos;/sg;
	#$text =~ s/"/&quot;/sg;
	$text =~ s/&/&amp;/sg;
	# $text =~ s/'/\\'/sg;
	$text =~ s/\#\*\$\@/\\\'/sg ; # use encoded single quotes needed for old sql format
	return $text;              # to differentiate between quotes in text and added by dump
}

=to do
=cut
sub unhtmlchars {
	my ($self, $text) = @_;
	# unescape xml
	$text =~ s/&lt;/</sg;
	$text =~ s/&gt;/>/sg;
	$text =~ s/&apos;/'/sg;
	$text =~ s/&quot;/"/sg;
	$text =~ s/&amp;/&/sg;
	# escape sql
	$text =~ s/\\/\\\\/sg;
	$text =~ s/\n/\\n/sg;
	$text =~ s/\r/\\r/sg;
	$text =~ s/\0/\\0/sg;
	$text =~ s/\x1A/\\Z/sg;
	$text =~ s/"/\\"/sg;
	# $text =~ s/'/\\'/sg;
	$text =~ s/\\\'/\#\*\$\@/sg; # use encoded single quotes needed for old sql format
	return $text;              # to differentiate between quotes in text and added by dump
}

=to do
=cut
sub get_value {
	my ($self, $text) = @_;
	$text =~ s/s:\d+:\"//g;
	$text =~ s/\";//g;
	return $text;
}

=to do
=cut
sub sendmail {
	my ($self, $to, $from, $subject, $body) = @_;

	my $sendmail = '/usr/sbin/sendmail';
	my $switch = '-t';
	open MAIL, "|$sendmail $switch" or return $!;

   	my $message = <<_EMAIL_;
To: $to
From: $from
Subject: $subject

$body
_EMAIL_

	print MAIL $message;
   	close MAIL;

	return;
}

=get list of dates
params:
$daysago - number of days to check
$results - get every day|week|month of searching dates period
$end - date of "now"
=cut
sub get_dates {
	my ($self, $daysago, $results, $end, $format) = @_;
	$end = "now" unless $end;
	$format = "$daysago days ago" unless $format;

	my $mask = "0:0:1:0:0:0:0";
	given( $results ) {
		when ( 'day' ) { $mask = "0:0:0:1:0:0:0"; }
		when ( 'week' ) { $mask = "0:0:1:0:0:0:0"; }
		when ( 'month' ) { $mask = "0:1:0:0:0:0:0"; }
	}
	my @dates = &ParseRecur($mask, $format, $format, $end);

	return \@dates;
}

=to do
=cut
sub get_increase {
	my ($self, $curvalue, $prevalue) = @_;
	my $val = 100.00;
	$val = sprintf("%0.2f", 100*($curvalue-$prevalue)/$curvalue) if ($prevalue && $curvalue);
	$val = -100.00 if (!$curvalue);
	return $val;
}

=send file
=cut
sub send_file {
	my ($self, $title, $emails, $file_path) = @_;
	my $sender = new Mail::Sender {smtp => 'localhost', from => 'moli@wikia-inc.com'};
	$sender->MailFile({to => 'moli@wikia-inc.com', subject => $title, msg => $title, file => $file_path});
	if ( $emails ) {
		my @emails = split(",", $emails);
		if ( scalar @emails ) {
			foreach (@emails) {
				$sender->MailFile({to => $_, subject => $title, msg => $title, file => $file_path});
				print "send email $title to " . $_ . " \n";
			}
		}
	}
}

=is ip
=cut
sub is_ip {
	my ($self, $sIP) = @_;
	my @ip = split /\./, $sIP;
	my $is_ip = (4 ne scalar @ip || 4 ne scalar map { $_ =~ /^(0|[1-9]\d*)$/ && $1 < 256 ? 1 : () } @ip) ? 0 : 1;
	return $is_ip;
}

=get subdomain
=cut
sub get_subdomain {
	my ($self, $address) = @_;
	my $res = "";
	my @dom = ('com', 'org', 'biz', 'net', 'info', 'co', 'name', 'edu', 'gov', 'int', 'mil', 'name', 'pro');
	my @match = split(/\./, $address);
	if ($match[0] eq 'www' && scalar(@match) == 3) {
		$res = $match[1];
	} elsif (scalar(@match) < 3) {
		$res = $match[0];
	} else {
		if ( (length($match[scalar(@match)-1]) == 2) && (scalar(@match) == 3) ) {
			# google.com.uk
			$res = $match[1];
		} elsif (grep /^\Q$match[scalar(@match)-2]\E$/,@dom) {
			# XXX.xXX.com.pl
			$res = $match[scalar(@match)-3];
		} else {
			$res = $match[scalar(@match)-2];
		}
	}

	return $res;
}

sub makePrevDate {
	my ($self, $ago, $format) = @_;

	my $one_month = 31;
	#
	$format = "%04d%02d00000000" unless ($format);
	#
	my $ago_new = ($ago * $one_month);
	my @ltime_prev = localtime(time - $ago_new * 24 * 60 * 60);
	my ($sec_prev, $min_prev, $hour_prev, $mday_prev, $mon_prev, $year_prev) = @ltime_prev;
	$mon_prev = 12 if ($mon_prev == 0);
	$year_prev = $year_prev - 1 if ($mon_prev == 0);
	my $prev_date = sprintf($format, $year_prev+1900, $mon_prev);
	return $prev_date;
}

sub makeCorrectDate {
	my ($self, $date) = @_;
	my $y = substr($date,0,4);
	my $m= substr($date,4,2);
	my $d = substr($date,6,2);
	$date = sprintf("%04d%02d%02d000000", $y, $m, $d);
	return ($date) ;
}

sub makeCorrectMonth {
	my ($self, $date) = @_;
	my $y = substr($date,0,4);
	my $m = substr ($date,4,2);
	$date = sprintf("%04d%02d00000000", $y, $m);
	return($date);
}

sub makeNextMonth {
	my ($self, $date) = @_;
	my $y = substr ($date,0,4) ;
	my $m= substr ($date,4,2) ;

	$m++; $m=1 if ($m > 12);
	$y++ if ($m == 1);

	$date = sprintf ("%04d%02d00000000", $y, $m);
	return ($date) ;
}

sub makeShortDate {
	my ($self, $date) = @_;
	my $y = substr ($date,0,4) ;
	my $m= substr ($date,4,2) ;

	$date = sprintf ("%04d-%02d", $y, $m);
	return ($date) ;
}

sub encodeEntities {
	my ($self, $text) = @_;
	$text =~ s/\*\{\|\}\*/\`/g;
	$text =~ s^\#\*\$\@^'^g;
	$text =~ s/\\r/\r/go;
	$text =~ s/\\n/\n/go;
	$text =~ s/\\"/"/go;

	return $text;
}

sub getImagetag {
	my ($self, $language) = @_;

	my $imagetag = "image";

	# in the future update these directly from language dependant php files
	# check codes at http://cvs.sourceforge.net/viewcvs.py/wikipedia/phase3/languages/
	if ($language eq "af") { $imagetag = "Beeld" ; }
	if ($language eq "ca") { $imagetag = "Imatge" ; }
	if ($language eq "cs") { $imagetag = "Soubor" ; }
	if ($language eq "de") { $imagetag = "Bild" ; }
	if ($language eq "da") { $imagetag = "Billede" ; }
	if ($language eq "eo") { $imagetag = "Dosiero" ; }
	if ($language eq "es") { $imagetag = "Imagen" ; }
	if ($language eq "et") { $imagetag = "Pilt" ; }
	if ($language eq "eu") { $imagetag = "irudi" ; }
	if ($language eq "fi") { $imagetag = "Kuva" ; }
	if ($language eq "fy") { $imagetag = "Ofbyld" ; }
	if ($language eq "he") { $imagetag = "\xD7\xAA\xD7\x9E\xD7\x95\xD7\xA0\xD7\x94" ; }
	if ($language eq "hi") { $imagetag = "\xE4\x9A\xE4\xBF\xE4\xA4\xE5\x8D\xE4\xB0" ; }
	if ($language eq "hu") { $imagetag = "K&eacute;p" ; } # or unicode "K\xC3\xA9p"
	if ($language eq "ia") { $imagetag = "Imagine" ; }
	if ($language eq "it") { $imagetag = "Immagine" ; }
	if ($language eq "ja") { $imagetag = "\xE7\x94\xBB\xE5\x83\x8F" ; }
	if ($language eq "la") { $imagetag = "Imago" ; }
	if ($language eq "nl") { $imagetag = "Afbeelding" ; }
	if ($language eq "no") { $imagetag = "Bilde" ; }
	if ($language eq "oc") { $imagetag = "Image" ; }
	if ($language eq "pl") { $imagetag = "Grafika" ; }
	if ($language eq "pt") { $imagetag = "Imagem" ; }
	if ($language eq "ro") { $imagetag = "Imagine" ; }
	if ($language eq "ru") { $imagetag = "\xD0\x98\xD0\xB7\xD0\xBE\xD0\xB1\xD1\x80\xD0\xB0\xD0\xB6\xD0\xB5\xD0\xBD\xD0\xB8\xD0\xB5" ; }
	if ($language eq "sk") { $imagetag = "Obr&aacute;zok" ; } # or unicode "obr\xE1\x7Azok"
	if ($language eq "sl") { $imagetag = "Slika" ; }
	if ($language eq "sq") { $imagetag = "Figura" ; }
	if ($language eq "sr") { $imagetag = "\xD0\xA1\xD0\xBB\xD0\xB8\xD0\xBA\xD0\xB0" ; }
	if ($language eq "uk") { $imagetag = "\xD0\x98\xD0\xB7\xD0\xBE\xD0\xB1\xD1\x80\xD0\xB0\xD0\xB6\xD0\xB5\xD0\xBD\xD0\xB8\xD0\xB5" ; }
	if ($language eq "sv") { $imagetag = "Bild" ; }
	if ($language eq "wa") { $imagetag = "Im\xc3\xa5dje" ; }

	return $imagetag;
}

sub getCategoryTag {
	my ($self, $language) = @_;

	my $categorytag = "category";
	if ($language eq "af") { $categorytag = "category" ; }
	if ($language eq "ca") { $categorytag = "Categoria" ; }
	if ($language eq "cs") { $categorytag = "Kategorie" ; }
	if ($language eq "de") { $categorytag = "Kategorie" ; }
	if ($language eq "da") { $categorytag = "Kategori" ; }
	if ($language eq "eo") { $categorytag = "Kategorio" ; }
	if ($language eq "es") { $categorytag = "Categor\xC3\xADa" ; }
	if ($language eq "et") { $categorytag = "Kategooria" ; }
	# if ($language eq "eu") { $categorytag = "irudi" ; } no language file in CVS ?
	if ($language eq "fi") { $categorytag = "Luokka" ; }
	if ($language eq "fy") { $categorytag = "Kategorie" ; }
	# if ($language eq "he") { $categorytag = "\xD7\xAA\xD7\x9E\xD7\x95\xD7\xA0\xD7\x94" ; }
	# if ($language eq "hi") { $categorytag = "\xE4\x9A\xE4\xBF\xE4\xA4\xE5\x8D\xE4\xB0" ; }
	# if ($language eq "hu") { $categorytag = "?" ; } # or unicode "K\xC3\xA9p"
	# if ($language eq "ia") { $categorytag = "" ; } # not tag specified yet
	if ($language eq "it") { $categorytag = "Categoria" ; }
	# if ($language eq "ja") { $categorytag = "\xE7\x94\xBB\xE5\x83\x8F" ; }
	# if ($language eq "la") { $categorytag = "?" ; } # not tag specified yet
	if ($language eq "nl") { $categorytag = "Categorie" ; }
	if ($language eq "no") { $categorytag = "Kategori" ; }
	# if ($language eq "oc") { $categorytag = "?" ; }  # not tag specified yet
	if ($language eq "pl") { $categorytag = "Kategoria" ; }
	if ($language eq "pt") { $categorytag = "Categoria" ; }
	if ($language eq "ro") { $categorytag = "Categorie" ; }
	# if ($language eq "ru") { $categorytag = "\xD0\x98\xD0\xB7\xD0\xBE\xD0\xB1\xD1\x80\xD0\xB0\xD0\xB6\xD0\xB5\xD0\xBD\xD0\xB8\xD0\xB5" ; }
	# if ($language eq "sk") { $categorytag = "?" ; }  # not tag specified yet
	# if ($language eq "sl") { $categorytag = "?" ; }  # not tag specified yet
	# if ($language eq "sq") { $categorytag = "?" ; }  # not tag specified yet
	# if ($language eq "sr") { $categorytag = "\xD0\xA1\xD0\xBB\xD0\xB8\xD0\xBA\xD0\xB0" ; }
	# if ($language eq "uk") { $categorytag = "\xD0\x98\xD0\xB7\xD0\xBE\xD0\xB1\xD1\x80\xD0\xB0\xD0\xB6\xD0\xB5\xD0\xBD\xD0\xB8\xD0\xB5" ; }
	if ($language eq "sv") { $categorytag = "Kategori" ; }
	# if ($language eq "wa") { $categorytag = "im\xc3\xa5dje" ; }

	return $categorytag;
}

sub first_datetime {
	my ($self, $date) = @_;

	my ($year, $month) = $date =~ m/^(\d{4})\-(\d\d)$/;

	if ( !$year && !$month ) {
		($year, $month) = $date =~ m/^(\d{4})(\d\d)$/;
	}

	return DateTime->new(year => $year, month => $month, day => 1)->strftime('%Y-%m-%d 00:00:00');
}

sub last_datetime {
	my ($self, $date) = @_;

	my ($year, $month) = $date =~ m/^(\d{4})\-(\d\d)$/;

	if ( !$year && !$month ) {
		($year, $month) = $date =~ m/^(\d{4})(\d\d)$/;
	}

	return DateTime->last_day_of_month( year => $year, month => $month )->strftime('%Y-%m-%d 23:59:59');
}

# Note: there is a Perl function for this.  These are equivalent:
#
#  $int = Wikia::Utils->intval($val);
#  $int = int($val);
#
sub intval {
	my ($self, $val) = @_;
	return ( $val ) ? sprintf("%d", $val) : 0;
}

sub floatval ($$;$) {
	my ($self, $val, $dec) = @_;
	$dec = 2 unless $dec;
	return ( $val ) ? sprintf("%0.".$dec."f", $val) : 0;
}

sub urlencode($$) {
	my ($self, $str ) = @_;
	$str =~ s/([^A-Za-z0-9])/sprintf("%%%02X", ord($1))/seg;
	return $str;
}

sub urldecode($$) {
	my ($self, $str ) = @_;
	$str =~ s/%([A-Fa-f0-9]{2})/pack('C', hex($1))/seg;
	return $str;
}

sub parse_referrer($;$) {
	my ($self, $referrer, $parse) = @_;

	return "Invalid" unless $referrer;

	$referrer =~ m|(\w+)://([^/:]+)(:\d+)?/(.*)|;
	my $domainName = $2;

	if ( !$parse ) {
		$domainName = 'Browser' unless $domainName;
	} else {
		my ($nothing, $maindomain, $com) = "";

		my @parts = split(/\./, $domainName);
		my $length = scalar(@parts);

		$maindomain = $parts[$length-2] if ($length == 2);
		($nothing, $maindomain, $com) = $domainName =~ /^([^\.]+)\.(.*)(\..+)$/ if ($length > 2);

		#check is correct domain
		@parts = split(/\./, $maindomain); $length = scalar(@parts);
		if ($length == 2 && length($parts[$length-1]) <= 3) {
			$maindomain = $parts[$length-2];
		} elsif ($length == 2 && length($parts[$length-1]) > 3) {
			$maindomain = $parts[$length-1];
		}

		#--- no referer - just write url in browser
		if ( !$maindomain ) {
			$maindomain = $domainName if ($domainName);
		}
		$maindomain = 'Invalid_or_browser' unless $maindomain;

		#--- return
		$domainName = $maindomain;
	}

	return $domainName;
}

sub fetch_json_page ($;$$) {
	my ($self, $json_url, $post) = @_;
	my $ua = LWP::UserAgent->new();
	$ua->cookie_jar({});
	$ua->default_header("Accept-Encoding" => "gzip, deflate");
	my $json_text = undef;
	my $response = undef;
	if ( $post ) {
		$response = $ua->post( $json_url );
	} else {
		$response = $ua->get( $json_url );
	}
	my $allowed_content = undef;
	if ( $response->is_success ) {
		$allowed_content = ( $response->{_headers}->{'content-type'} =~ /json/ ) if ( $response->{_headers} );
		if ( defined ( $allowed_content ) && $allowed_content ne '' ) {
			my $content = $response->decoded_content( charset => 'none' );
			#my $content = $request->content;
			$json_text = $self->json_decode($content);
		}
	}
	return $json_text;
}

sub get_api_namespaces($$) {
	my ($self, $json_url) = @_;

	my $jsonTxt = $self->fetch_json_page($json_url);
	my $res = {};

	if ( $jsonTxt->{query} ) {
		my $namespacealiases = $jsonTxt->{query}->{namespacealiases};
		my $namespaces = $jsonTxt->{query}->{namespaces};

		if ( scalar(@$namespacealiases) ) {
			foreach ( @$namespacealiases ) {
				push @{$res->{$_->{id}}}, $_->{'*'} if ( $_->{'*'} ) ;
			}
		}

		if ( scalar( keys %$namespaces) ) {
			foreach ( values %$namespaces ) {
				push @{$res->{$_->{id}}}, $_->{'*'} if ( $_->{'*'} ) ;
				push @{$res->{$_->{id}}}, $_->{'canonical'} if ( $_->{'canonical'} && ( $_->{'canonical'} ne $_->{'*'} ) ) ;
			}
		}
	}

	return $res;
}

sub get_namespace_by_server($$) {
	my ($self, $server) = @_;
	my $namespaces = {};

	return $namespaces unless $server;

	my $url = sprintf("http://%s/api.php?action=query&meta=%s&siprop=%s&format=json",
		$server,
		"siteinfo",
		"namespaces|namespacealiases"
	);
	$namespaces = $self->get_api_namespaces($url);
	return $namespaces;
}

# Only construct the JSON de/serializer once
our $CODER = JSON::XS->new->ascii->pretty->allow_nonref;
sub json_decode($$) { $CODER->decode($_[1]) }
sub json_encode($$) { $CODER->encode($_[1]) }

# Note: the Perl function for this is 'grep'.  These are equivalent (and the
# perl built-in faster since it doesn't do an array copy):
#
#  if ( Wikia::Utils->in_array($search_term, $array) ) {}
#  if ( grep { /$search_term/ } @$array ) {}
# Moli: This code was moved from old version of MW's wikiastats project - most of code was not changed.
# this code was used in wikiastats project, so we left this code :)

sub in_array {
	my ($self, $search_for, $arr) = @_;
	my %items = map {$_ => 1} @$arr;
	return ( exists( $items{$search_for} ) ) ? 1 : 0;
}

sub call_mw_api($$$;$$) {
	my ($self, $url, $params, $login, $priority) = @_;

	unless ( $url =~ /^http\:\/\// ) {
		$url = sprintf("http://%s", $url);
	}

	unless ( $url =~ /\/api.php$/ ) {
		$url = sprintf("%s/api.php", $url);
	}

	my $mw = MediaWiki::API->new();
	$mw->{config}->{api_url} = $url;

	# set proxy
	$mw->{ua}->max_redirect( 7 );
	$mw->{ua}->proxy('http', 'http://squid-proxy.local:3128');
	$mw->{ua}->default_header('Authenticate' => '1');
	$mw->{ua}->timeout(120);
	$mw->{ua}->env_proxy;
	#$mw->{ua}->proxy('http', 'http://varnish-s3:80');

	if ( $priority == 1 ) {
		my $cookie_jar = new HTTP::Cookies;
		$cookie_jar->set_cookie(0,"wikicities-wikia-colocation","iowa","/",".wikia.com");
		$mw->{ua}->cookie_jar($cookie_jar);
	}

	# log in to the wiki
	if ( $login ) {
		my $res = $mw->login( { lgname => $login->{username}, lgpassword => $login->{password} } );
		if ( !$res ) {
			print $mw->{error}->{code} . ': ' . $mw->{error}->{details} . "\n";
			return undef;
		}
	}

	my $res = $mw->api( $params );
	if ( !$res ) {
		print $mw->{error}->{code} . ': ' . $mw->{error}->{details} . "\n";

		if ( $mw->{response}->is_redirect() ) {
			my $redirect = $mw->{response}->header( 'Location' );
			my $uri = $mw->{response}->request->uri->as_string;
			if ( $redirect ne $uri ) {
				my $uri = $mw->{response}->request->uri->as_string;
				$url =~ s/$uri/$redirect/g;
				print "Redirect to" . $redirect . " \n";
				return $self->call_mw_api( $url, $params, $login, $priority );
			} else {
				return undef;
			}
        } else {
			return undef
		}
	}

	#logout
	if ( $login ) {
		$mw->logout();
	}

	return $res;
}

sub date_YM($$) {
	my ( $self, $day1 ) = @_;
	print Dumper($day1);
	my ($year, $month) = $day1 =~ m/^(\d{4})\-(\d\d)/;

	if ( !$year && !$month ) {
		($year, $month) = $day1 =~ m/^(\d{4})(\d\d)/;
	}
	return ($year, $month);
}

sub month_between_dates($$$) {
	my ( $self, $date1, $date2 ) = @_;

	my ($year1, $month1) = $self->date_YM($date1);
	my ($year2, $month2) = $self->date_YM($date2);
	my $day1 = sprintf("%04d%02d", $year1, $month1);
	my $day2 = sprintf("%04d%02d", $year2, $month2);

	my @dates = ();
	if ( $day1 <= $day2 ) {
		while ($day1 <= $day2) {
			my ($y, $m) = $self->date_YM($day1);
			push @dates, $day1;
			$m++; $m=1 if ($m > 12);
			$y++ if ($m == 1);
			$day1 = DateTime->new(year => $y, month => $m , day => 1)->strftime('%Y%m');
		}
	}
	return \@dates;
}

sub days_between_dates($$;$$) {
	my ( $self, $start, $end, $days ) = @_;

	$end = $start unless( $end );
	$days = 7 unless( $days );

	my @result = ();
	push @result, $start;
	while ( $start lt $end ) {
		my ( $year, $month, $day ) = ();
		if ( $start =~ m/(\d{4})\-(\d{2})\-(\d{2})/ ) {
			( $year, $month, $day ) = ( $1, $2, $3 );
		}
		$start = sprintf("%s", DateTime->new(
					 year 	=> $year,
					 month	=> $month,
					 day	=> $day
		)->add( days => $days )->strftime('%F'));
		push @result, $start if ( $start lt $end ) ;
	}

	return \@result;
}

sub between_dates_days_ago ($$) {
	my ( $self, $days ) = @_;

	my $start_date = DateTime->now()->subtract( days => $days )->strftime('%F');
	my $end_date = DateTime->now()->subtract( days => 1 )->strftime('%F');

	my ($year, $month, $day) = $start_date =~ m/^(\d{4})-(\d{2})-(\d{2})$/;
	$start_date = DateTime->new(year => $year, month => $month, day => $day)->strftime('%Y-%m-%d 00:00:00');

	($year, $month, $day) = $end_date =~ m/^(\d{4})-(\d{2})-(\d{2})$/;
	$end_date = DateTime->new(year => $year, month => $month, day => $day)->strftime('%Y-%m-%d 23:59:59');
	my @date = ($start_date, $end_date);

	return \@date;
}

sub strtolower($$) {
	my ( $self, $text ) = @_;

	if ( !utf8::is_utf8 ( $text ) ) {
		$text = decode( "utf-8", $text );
	}

	return lc $text;
}

sub strip_wikitext {
	my $class = shift;
	my ($text) = @_;

    # Bold text - to plain
    $text =~ s/'''(.+?)'''/$1/g;

    # Italic text - to plain
    $text =~ s/''(.+?)''/$1/g;

    # Files - remove
    $text =~ s/\[\[(?:File|Media):[^\]]+\]\]//g;

    # Wiki Link - link
    $text =~ s/\[\[([^\[\|]+)\]\]/$1/g;

	# Wiki link with title - link title
	$text =~ s/\[\[[^\|]+\|([^\[]+)\]\]/$1/g;

    # External link - link
    $text =~ s/\[([^\[]+)\s*\]/$1/g;

    # External link w/ title - link title
    $text =~ s/\[\S+\s+([^\[]+)\]/$1/g;

    # Headlines - to plain
    $text =~ s/(=+)\s*([^=\n]+)\s*\1/$2/g;

	# Templates - remove
	$text =~ s/\{\{[^\}]+\}\}//g;

    # Breaks - remove
    $text =~ s/^---//;

    # Single HTML tags - remove
    $text =~ s!<[^>]+/\s*>!!g;
    $text =~ s!<\s*(br|hr)\s*>!!g;

    # HTML tags - replace w/ contents
    while ($text =~ s!<\s*(\S+)[^>]*>([^<]*)</\1>!$2!gi) {}

	# Tables - to plain
	# - start and end
	$text =~ s/^{\|.+$//mg;
	$text =~ s/^\|\}//mg;
	# - Row dividers
	$text =~ s/^\|\-.*//mg;
	# - Headers
	$text =~ s/^!(.+)\n/{my $m=$1; $m=s!^[^\|]+\|!!g; $m}/meg;
	# - Captions
	$text =~ s/^\|\+([^\|]+\|)?//meg;
	# - Content
	$text =~ s/^\|(.+)/{my $m=$1; $m=~s!\|\|!!g; $m=~s!^[^\|]+\|!!g; $m}/meg;

	$text =~ s/__NOEDITSECTION__//g;

    return $text;
}

sub fixutf($$) {
	my ( $self, $text ) = @_;

	if ( !utf8::is_utf8 ( $text ) ) {
		$text = decode( "utf-8", $text );
	}

	return $text;
}

#
# PHP compatible gzdeflate function
# @author Krzysztof Krzyżaniak (eloy)
#
sub gzdeflate {
	my( $self, $buffer, $level ) = @_;

	$level ||= Z_DEFAULT_COMPRESSION;
	$level = Z_DEFAULT_COMPRESSION if $level == -1; # PHP compatibility

	my $output = undef;

	my( $deflate, $status ) = Compress::Raw::Zlib::Deflate->new( -Level => $level, -WindowBits => 0 - MAX_WBITS );
	$deflate->deflate( $buffer, \$output );
	$deflate->flush( \$output );

	return $output
}


#
# PHP compatible inflate function
# @author Krzysztof Krzyżaniak (eloy)
#
sub gzinflate {
	my( $self, $buffer, $length ) = @_;

	$length ||= 0;

	my $output = undef;

	my( $inflate, $status ) = Compress::Raw::Zlib::Inflate->new( -WindowBits => 0 - MAX_WBITS, -LimitOutput => $length );
	if( $status == Z_OK ) {
		$status = $inflate->inflate( $buffer, \$output );
		if( $status == Z_OK ) {
			$status = $inflate->inflateSync( $buffer );
		}
	}
	if( $status == Z_OK || $status == Z_STREAM_END ) {
		return $output;
	}
	else {
		# throw exception
		die "Can't gzinflate string, error $status\n";
	}
}

sub interval_time {
	my ( $self, $t_start ) = @_;
	return tv_interval( $t_start, [ $self->current_time() ] );
}

sub current_time {
	my $self = shift;
	return gettimeofday();
}
1;
__END__
