package Wikia::DW::ETL::SpecialPage;

use strict;
use warnings;

=pod

=head1 NAME

Wikia::DW::ETL::SpecialPage

=head1 DESCRIPTION

This module manages the dimension_specialpages table.  Requesting a special page's ID by its name from name_to_id($name) will return the ID if it has already been mapped, or it will create a new mapping and return that.

The id_to_name($id) method will do the reverse mapping, however it will not auto create a special page given an unknown ID.

=cut

use Wikia::DW::Common;

our $LOADED = 0;
our (%SPECIALS, %SPECIALS_BY_ID);

sub id_to_name {
	my ($id) = @_;
	load_mapping() unless $LOADED;

	return $SPECIALS_BY_ID{$id};
}

sub name_to_id {
	my ($name) = @_;
	load_mapping() unless $LOADED;

	if (! exists $SPECIALS{$name}) {
		my $dbh = Wikia::DW::Common::statsdb();
		my $sth = $dbh->prepare("INSERT INTO dimension_specialpages (name) VALUES (?)");

		my $rv = $sth->execute($name)
			or die "Failed to insert new special page: ".$sth->errstr;

		my $id = $dbh->{mysql_insertid};
		$SPECIALS{$name} = $id;
		$SPECIALS_BY_ID{$id} = $name;

	    $dbh->do('COMMIT');
		$dbh->disconnect;
	}

	return $SPECIALS{$name};
}

sub load_mapping {
	my $dbh = Wikia::DW::Common::statsdb();
	my $sth = $dbh->prepare("SELECT id, name FROM dimension_specialpages");

	my $rv = $sth->execute()
		or die "Could not load special page name to ID mapping: ".$sth->errstr;

	while (my $row = $sth->fetchrow_arrayref()) {
		$SPECIALS{$row->[1]} = $row->[0];
		$SPECIALS_BY_ID{$row->[0]} = $row->[1];
	}

	$dbh->disconnect;

	$LOADED = 1;
	return;
}

1;
