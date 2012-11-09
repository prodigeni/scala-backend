#!/usr/bin/perl -w

use common::sense;
use feature "say";
use encoding "UTF-8";

use FindBin qw/$Bin/;
use lib "$Bin/../../lib/";
use Getopt::Long;
use Data::Dumper;
use PHP::Serialization qw(serialize unserialize);

use Wikia::Utils;
use Wikia::DB;
use Wikia::LB;
use Wikia::Title;

my $destdir = './';
my $verbose;

GetOptions(
	'destination|dest|dir=s' => \$destdir,
	'verbose' => \$verbose,
);

$destdir .= "/" if $destdir !~ /\/^/;

my $DEFAULT_SLOT = 1;
my $MIN_SLOT = 1;
my $MAX_SLOT = 4;
my $SHORT_URLS = '/$1';
my $LONG_URLS = '/wiki/$1';
my $WG_ARTICLE_PATH = 15;

my @wildcards = qw( .wikia.com .wikia.net .wikia.org .wikia.info .wikia.at 
	.wikia.be .wikia.ch .wikia.cn .wikia.de .wikia.jp .wikia.lt 
	.wikia.no .wikia.pl .wikia.tw .wikia.co.uk .wikia.us 
	.wikicities.com .wikicities.net .wikicities.org
	.uncyclopedia.org .memory-alpha.org .wowwiki.com );
#print Dumper(@wildcards);
my $dbr = Wikia::LB->instance->getConnection( Wikia::LB::DB_SLAVE, undef, Wikia::LB::EXTERNALSHARED );

my ($swikis,$sdomains,$spaths);

print "getting wikis...\n" if $verbose;
$swikis = get_dict($dbr,qq(SELECT city_id, city_path, city_url FROM city_list;),'city_id');
print "getting domains...\n" if $verbose;
$sdomains = get_dict_array($dbr,qq(SELECT * FROM city_domains;),'city_id','city_domain');
print "getting wgArticlePaths...\n" if $verbose;
$spaths = get_dict($dbr,qq(SELECT * FROM city_variables WHERE cv_variable_id = $WG_ARTICLE_PATH;),'cv_city_id','cv_value');

print "sorting data...\n" if $verbose;
my $wikis = {};
foreach my $id (keys %$swikis) {
	my $wiki = $swikis->{$id};
	my $slot = get_slot($wiki->{city_path},$DEFAULT_SLOT);
	my $path = get_article_path($spaths->{$id},$LONG_URLS);
	$path = $LONG_URLS if ( $path ne $SHORT_URLS and $path ne $LONG_URLS );
	$wikis->{$slot}->{$path}->{$id} = {
		id => $wiki->{city_id},
		slot => $slot,
		url => strip_url($wiki->{city_url}),
		urls => $sdomains->{$id} || (),
		path => $path,
	}
}

print "writing files...\n" if $verbose;
my @wildcards_copy = @wildcards;
map { s/\./\\./g } @wildcards_copy;
my $wildcards_regex = join '|', @wildcards_copy;


use Data::Dumper;
print Dumper(@wildcards) if $verbose;
print Dumper($wildcards_regex) if $verbose;

for (my $slot = $MIN_SLOT; $slot <= $MAX_SLOT; $slot++) {
	my $content = '';
	$content .= template("header",());
	foreach my $path (($SHORT_URLS,$LONG_URLS)) { # order is important!
		my $long_urls = $path eq $LONG_URLS;
		my $docroot = get_docroot($slot,$path);
		my $wildwikiacom = 0;
		my $is_default_slot = ($slot == $DEFAULT_SLOT) && ($path eq $LONG_URLS);
		my @names = ();
		push @names, ($long_urls ? 'long' : 'short') . '.slot' . $slot . '.wikia.com';
		push @names, 'slot'. $slot . '.wikia.com' if $long_urls;
		foreach (keys %{$wikis->{$slot}->{$path}}) {
			my $wiki = $wikis->{$slot}->{$path}->{$_};
			my @wiki_urls = ($wiki->{url},@{$wiki->{urls}});
			my %hash = map { $_ => 1 } @wiki_urls;
			@wiki_urls = keys %hash;
			
			foreach my $wiki_url (@wiki_urls) {
				if ( ! ($is_default_slot and $wiki_url =~ /($wildcards_regex)$/ ) ) {
					push @names, $wiki_url;
				}
			}
		}
		
		next unless scalar @names;
		
		$content .= template_vhost($slot,$path,\@names);
	}
	file_write($destdir."slot$slot.conf",$content);
}

# Wildcard domains go into slot9.conf
my @domains = ('fake.com',map { "*$_" } @wildcards );
my $content = '';
$content .= template('header');
$content .= template_vhost($DEFAULT_SLOT,$LONG_URLS,\@domains);
file_write($destdir."slot9.conf",$content);

 
1;


sub get_dict {
	my ($db, $q, $key, $vkey) = (shift,shift,shift,shift);
	my $stt = $db->prepare($q);
	return unless $stt->execute();
	
	my $data = {};
	my $value;
	while (my $r = $stt->fetchrow_hashref) {
		$data->{$r->{$key}} = $vkey ? $r->{$vkey} : $r;
	}
	$stt->finish();
	return $data;
}

sub get_dict_array {
	my ($db, $q, $key, $vkey) = (shift,shift,shift,shift);
	my $stt = $db->prepare($q);
	return unless $stt->execute();
	
	my $data = {};
	while (my $r = $stt->fetchrow_hashref) {
		$data->{$r->{$key}} = () unless $data->{$r->{$key}};
		push @{$data->{$r->{$key}}}, $vkey ? $r->{$vkey} : $r;
	}
	$stt->finish();
	return $data;
}

sub get_article_path {
	my ($value,$default) = (shift,shift);
	return $value ? unserialize($value) : $default;
}

sub get_slot {
	my ($slot,$default) = (shift,shift);
	return ( $slot =~ /^slot([0-9]+)$/) ? $1 : $default;
}

sub strip_url {
	my $url = shift;
	for ($url) {
		s#^http[^/]//##;
		s#/*$##;
	}
	return $url;
}

sub get_docroot {
	my ($slot,$path) = (shift,shift);
	my $root = "/usr/wikia/slot$slot/docroot";
	return $root;
}

sub get_urls {
	my $w = shift;
	my ($url,@urls) = ($w->{url},$w->{urls});
	my @data = ();
	push @data, $url;
	foreach (@urls) {
		push @data, $_ if $url ne $_;
	}
	return @data;
}

my $template_texts = {};
sub get_template_text {
	my $name = shift;
	if ( ! $template_texts->{$name}) {
		my $file_name = "$Bin/templates/$name.conf";
		open(TPL, $file_name) || die("Could not open template: $file_name");
		my @lines = <TPL>;
		close(TPL);
		my $content = '';
		foreach (@lines) { $content .= $_; }
		$template_texts->{$name} = $content;
	}
	return $template_texts->{$name};
}

sub template {
	my ($name,$data) = (shift,shift);
	my $text = get_template_text($name);
	
	foreach (keys %$data) {
		my ($search,$replace) = ('\{\{\{' . $_ . '\}\}\}', $data->{$_});
		s/$search/$replace/g for $text;
	}
	return $text;
}

sub template_vhost {
	my ($slot,$path,$names) = @_;
	
	my $servernames = '';
	foreach (@{$names}) {
		$servernames .= "\t" . ($servernames ? 'ServerAlias' : 'ServerName') . " $_\n";
	}
	
	my $tpl = ($path eq $LONG_URLS) ? "long-domain" : "short-domain";
	return template($tpl,{
		ServerNameSection => $servernames,
		DocumentRoot => get_docroot($slot,$path),
	});
}

sub file_write {
	my ($name,$content) = (shift,shift);
	open(F,">".$name);
	print F $content;
	close(F);
}
