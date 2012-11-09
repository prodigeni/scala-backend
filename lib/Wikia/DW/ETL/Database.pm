package Wikia::DW::ETL::Database;

use strict;
use warnings;

use Exporter 'import';

our @ISA = qw(Exporter);
our @EXPORT_OK = qw(statsdb statsdb_pg);

use File::Spec;
use Wikia::DW::ETL::CSVWriter;
use Wikia::DW::ETL::Query;
use Wikia::DB;
use Wikia::LB;
use YAML::XS;

sub new {
    my $class = shift;
    my (%params) = @_; 
    my $self = bless \%params, ref $class || $class;

    # Find the location of this source file; use to locate the database config file
    my ($volume, $path, $file) = File::Spec->splitpath(__FILE__);
    $self->{databases} = YAML::XS::LoadFile("$path/../config/database.yml");
    $self->initialize;
    return $self;
}

sub initialize {
    my $self = shift;

    # Connect to the database; check config databases, but default to Wikia::DB instances
    if (my $source = $self->{databases}->{$self->{source}}) {
        $self->{dbh} = DBI->connect( "DBI:$source->{adapter}:database=$source->{database};host=$source->{host};port=$source->{port}",
                                     $source->{username},
                                     $source->{password},
                                     $source->{options}   );  
    } else {
        my $lb = Wikia::LB->instance;
        my $conn;
        if ($self->{master}) {
            $conn = $lb->getConnection(Wikia::LB::DB_MASTER, undef, $self->{source}, 0, 1) || die 'Failed to get connection';
        } else {
            $conn = $lb->getConnection(Wikia::LB::DB_SLAVE, undef, $self->{source}, 0, 1) || die 'Failed to get connection'; 
        }   
        my $db = Wikia::DB->new( {'dbh' => $conn } ); 
        $self->{dbh} = $db->handler();
    }   
    $self->{dbh}->{'mysql_use_result'} = 1;
}
sub do {
    my ($self, $sql) = @_;
    if (ref($sql) ne 'ARRAY') {
        $sql = [ $sql ];
    }
    my $result;
    foreach my $stmt (@$sql) {
        chomp($stmt);
        $result = $self->{dbh}->do($stmt) if $stmt ne '';
    }
    return $result;
}

sub arrayref {
    my ($self, $sql) = @_;
    return $self->{dbh}->selectall_arrayref($sql, { Slice => {} } );
}

sub disconnect {
    my $self = shift;
    $self->{dbh}->disconnect if $self->{dbh};
}

sub statsdb {
    return Wikia::DW::ETL::Database->new( source => 'statsdb' );
}

sub statsdb_pg {
    return Wikia::DW::ETL::Database->new( source => 'statsdb_pg' );
}

sub query2csv {
    my ($self, $query, $filepath) = @_;

    my $w = Wikia::DW::ETL::CSVWriter->new( filepath => $filepath );

    my $q  = Wikia::DW::ETL::Query->new( database   => $self,
                                         query      => $query,
                                         processors => [ $w ] );
    $q->run;
    $q->finalize;
}

sub finalize {
    my $self = shift;
    $self->{sth}->finish     if $self->{sth};
    $self->{dbh}->disconnect if $self->{dbh};
}

sub DESTROY {
    my $self = shift;
    $self->finalize();
}

1;
