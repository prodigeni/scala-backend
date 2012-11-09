package Wikia::KnifeSearch;

use Moose;
use JSON::XS;

__PACKAGE__->meta->make_immutable;

use Data::Dump;
sub hosts_by_search {
	my $self = shift;
	my( $cmd ) = @_;

	my $data = $self->_knife_cmd( $cmd );

	my @a = ();
	for my $info ( @{ $data->{ "rows" } } ) {
		push @a, { "name" => $info->{ "automatic"}->{ "hostname"}, "ip" => $info->{automatic}->{ipaddress} };
	}
	return \@a;
}

sub _knife_cmd {
	my $self = shift;
	my( $cmd ) = @_;

	my $search = sprintf('knife search node "%s" -F json', $cmd );

	my $result_json;
	open(my $pipe, "-|", "$search" ) or log_fatal("Can't run '$cmd': $!\n");
	{
		local $/ = undef;
		$result_json = <$pipe>;
	}
	close($pipe) or die "Command quit unexpectedly: $!\n";

	# Die if we get an error back rather than a json string
	die "Knife query failed: $result_json\n" if $result_json =~ /^ERROR/;

	my $data = JSON::XS::decode_json($result_json);
	return $data;
}

1;
