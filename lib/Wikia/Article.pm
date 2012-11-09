package Wikia::Article;

use FindBin qw/$Bin/;
use lib "$Bin/../../lib";

use strict;
use Carp;
use DBI;
use IO::File;
use Switch;
use Data::Dumper;
use Wikia::DB;
use Wikia::Utils;

use base qw(Class::Accessor);
__PACKAGE__->mk_accessors(qw(wikia title ns domain namespaces page));
our $VERSION = '0.01';

sub new {
    my $class  = shift;
    my $self   = $class->SUPER::new(@_);

	$self->title("") unless $self->title;
	$self->ns(0) unless $self->ns;
	$self->wikia(0) unless $self->wikia;

	return undef unless ( $self->wikia );
	# wikia domain
	$self->domain($self->get_domain());
	return undef unless ( $self->domain );
	# wikia namespaces
	$self->namespaces($self->get_namespaces());

	# article info
	$self->page($self->get_pageinfo());

    return $self;
}

sub get_page_url($) {
	my ($self) = @_;
}

sub get_domain($) {
	my ($self) = @_;
	my $url = sprintf("http://community.wikia.com/api.php?action=query&list=wkdomains&wkwikia=%d&format=json", $self->{wikia});
	my $response = Wikia::Utils->fetch_json_page($url);
	my $domain = "";
	if ( $response->{query} ) {
		$domain = $response->{query}->{wkdomains}->{$self->{wikia}}->{domain};
	}	
	return $domain;
}

sub get_namespaces($) {
	my ($self) = @_;
	my $url = sprintf("http://%s/api.php?action=query&meta=%s&siprop=%s&format=json",
		$self->domain,
		"siteinfo",
		"namespaces"
	);
	my $response = Wikia::Utils->fetch_json_page($url);
	my $res = {};

	if ( $response->{query} ) {
		my $namespaces = $response->{query}->{namespaces};
		if ( scalar( keys %$namespaces) ) {
			foreach my $ns_id ( keys %$namespaces ) {
				$res->{$ns_id} = $namespaces->{$ns_id}->{'*'} if ( $namespaces->{$ns_id}->{'*'} ) ;
				$res->{$ns_id} = $namespaces->{$ns_id}->{'canonical'} if ( $namespaces->{$ns_id}->{'canonical'} && ( !$namespaces->{$ns_id}->{'*'} ) ) ;
			}
		}
	}
	
	return $res;
}

sub get_namespace($) {
	my ($self) = @_;
	return $self->namespaces->{$self->ns};
}

sub get_namespace_inx($) {
	my ($self) = @_;
	return $self->ns;
}

sub get_title($) {
	my ($self) = @_;
	return $self->title;
}

sub get_full_title($) {
	my ($self) = @_;
	my $title = "";
	$title = $self->get_namespace() . ":" if ( $self->ns ) ;
	return $title . $self->get_title();
}

sub get_pageinfo($) {
	my ($self) = @_;
	my $url = sprintf("http://%s/api.php?action=query&prop=info&titles=%s&inprop=views|revcount|url|created&format=json",
		$self->domain,
		$self->get_full_title()
	);

	my $pageinfo = {};
	my $response = Wikia::Utils->fetch_json_page($url);
	my $domain = "";
	if ( $response->{query} ) {
		if ( values %{$response->{query}->{pages}} ) {
			my $key = [keys %{$response->{query}->{pages}}]->[0];
			$pageinfo = $response->{query}->{pages}->{$key};
		}
	}
	return $pageinfo;
}

sub get_id($) {
	my ($self) = @_;
	return $self->page->{pageid};
}

sub get_full_url($) {
	my ($self) = @_;
	return $self->page->{fullurl};
}

sub get_edit_url($) {
	my ($self) = @_;
	return $self->page->{editurl};
}

sub get_create_date($) {
	my ($self) = @_;
	return $self->page->{created};
}

sub get_last_revid($) {
	my ($self) = @_;
	return $self->page->{lastrevid};
}

sub get_length($) {
	my ($self) = @_;
	return $self->page->{length};
}

sub get_pviews($) {
	my ($self) = @_;
	return $self->page->{views};
}

sub is_redirect($) {
	my ($self) = @_;
	return $self->page->{redirect} || 0;
}

sub get_rev_count($) {
	my ($self) = @_;
	return $self->page->{revcount};
}

sub get_last_touched($) {
	my ($self) = @_;
	return $self->page->{touched};
}

1;
__END__
