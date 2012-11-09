package Wikia::TokyoCabinet;

use strict;
use Carp;
use DBI;
use IO::File;
use Switch;
use Data::Dumper;
use TokyoCabinet;
use Switch;

use base qw(Class::Accessor);
__PACKAGE__->mk_accessors(qw(db dbname path type mode index));
our $VERSION = '0.01';

use constant wgDefDB => 'default';
use constant wgDBPath => '/tmp/';

sub new {
    my $class  = shift;
    my $self   = $class->SUPER::new(@_);
	
	$self->type('hash') unless $self->type;
	$self->dbname(wgDefDB) unless $self->dbname;
	$self->path(wgDBPath) unless $self->path;

	my $db = undef;
	my $ext = ($self->dbname =~ m/([^.]*$)/)[0];
	switch ($self->type) {
		case 'btree' 	{ 
				$db = TokyoCabinet::BDB->new();
				$ext = 'tcb';
			}
		case 'hash' 	{ 
				$db = TokyoCabinet::HDB->new();
				$ext = 'tch';
			}
		case 'memory' 	{ 
				$db = TokyoCabinet::ADB->new();
			}
		case 'table' 	{ 
				$db = TokyoCabinet::TDB->new() ;
				$ext = 'tct';
			}
	}
	$ext = 'tch' unless $ext;
	my $dbmode = $db->OREADER;
	my $dbpath = sprintf("%s/%s.%s", $self->path, $self->dbname, $ext);

	if ( !$self->mode ) {
		$dbmode = $db->OWRITER | $db->OREADER | $db->OCREAT ;
	} else {
		my @_mode = split(",", $self->mode);
		foreach (@_mode) {
			switch ( $_ ) {
				case 'read'  	{
					$dbmode |= $db->OREADER;
				}
				case 'write'  	{
					$dbmode |= $db->OWRITER;
				}
				case 'nolock'  	{
					#it opens the database file without file locking
					$dbmode |= $db->ONOLCK;
				}
				case 'noblock'		{
					#locking is performed without blocking
					$dbmode |= $db->OLCKNB
				}
				case 'create'	{
					#creates a new database if not exist
					$dbmode |= $db->OCREAT;
				}
				case 'trunc'	{
					#creates a new database regardless if one exists (!)
					$dbmode |= $db->OTRUNC;
				}
				case 'tsync'	{
					#every transaction synchronizes updated contents with the device
					$dbmode |= $db->OTSYNC;
				}
			}
		}
	}

	if ( !$db->open( $dbpath, $dbmode ) ) {
		my $ecode = $db->ecode();
		print "TokyoCabinet: open error: ". $db->errmsg($ecode) . "\n" ;
		return 0;
	}

	if ( $self->index && $self->type eq 'table' ) {
	    foreach my $col (keys %{$self->index}) {
	    	my $type = $db->ITDECIMAL;
	    	if ( $self->index->{$col} ) {
				switch ($self->index->{$col}) {
					case 'pk' 	{ $col = ''; $type = $db->ITDECIMAL; }
					case 'int' 	{ $type = $db->ITDECIMAL; }
					case 'str' 	{ $type = $db->ITDECIMAL; }
					case 'bool' { $type = $db->ITTOKEN; }
					case 'text' { $type = $db->ITQGRAM; }    		
				}
			} else {
				$type = $db->ITVOID;
			}

			if ( !$db->setindex($col, $type) ) {
				my $ecode = $db->ecode();
				print "Could not set index on column ".$col.": ".$db->errmsg($ecode)." \n" ;
				return 0;
			}
		} 
	}

	$self->db($db);
    return $self->db;
}

1;
__END__
