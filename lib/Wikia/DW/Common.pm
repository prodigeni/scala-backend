package Wikia::DW::Common;

use strict;

use DBI;
use DBD::mysql;
use Wikia::DW::ETL::Database;
use Wikia::DW::ETL::CSVWriter;

our $DEBUG = 0;

sub globals {
    my $globals = Wikia::DW::Common::statsdb_hashref('SELECT setting, value FROM statsdb_etl.etl_globals', 'setting');
    foreach my $s (keys %$globals) {
        $globals->{$s} = $globals->{$s}->{value};
    }
    return $globals;
}

sub statsdb {
    my $db = shift || 'statsdb';
    return DBI->connect("DBI:mysql:database=$db;host=dw-s1", 'statsdb', '', {AutoCommit => 0, PrintError => 0, RaiseError => 1, mysql_auto_reconnect => 1});
} 

sub statsdb_mart {
    return DBI->connect('DBI:mysql:database=statsdb_mart;host=dw-s1', 'statsdb', '', {AutoCommit => 0, PrintError => 0, RaiseError => 1, mysql_auto_reconnect => 1});
} 

sub statsdb_do {
    my $sql = shift;
    if (ref($sql) ne 'ARRAY') {
        $sql = [ $sql ];
    }
    my $dbh = Wikia::DW::Common::statsdb;
    my $result;
    foreach my $stmt (@$sql) {
        Wikia::DW::Common::log("[DEBUG]\n$stmt\n") if $Wikia::DW::Common::DEBUG;
        $result = $dbh->do($stmt);
    }
    $dbh->disconnect;
    return $result;
}

# Retrieve a single value
sub statsdb_value {
    my $sql = shift;
    Wikia::DW::Common::log("[DEBUG]\n$sql\n") if $Wikia::DW::Common::DEBUG;
    my $dbh = Wikia::DW::Common::statsdb;
    my $result = $dbh->selectall_arrayref($sql)->[0][0];
    $dbh->disconnect;
    return $result;
}

# Retrieve a reference to an array of values
sub statsdb_arrvalue {
    my $sql = shift;
    Wikia::DW::Common::log("[DEBUG]\n$sql\n") if $Wikia::DW::Common::DEBUG;
    my $dbh = Wikia::DW::Common::statsdb;
    my $arrayref = $dbh->selectall_arrayref($sql);
    $dbh->disconnect;
    foreach (@$arrayref) { $_ = $_->[0] };
    return $arrayref;
}

sub statsdb_arrayref {
    my ($sql, $type) = @_;
    Wikia::DW::Common::log("[DEBUG]\n$sql\n") if $Wikia::DW::Common::DEBUG;
    my $dbh = Wikia::DW::Common::statsdb;
    my $arrayref;
    if ($type && $type eq 'hash') {
        $arrayref = $dbh->selectall_arrayref($sql, { Slice => {} } );
    } else {
        $arrayref = $dbh->selectall_arrayref($sql);
    }

    $dbh->disconnect;
    return $arrayref;
}

sub statsdb_hashref {
    my ($sql, $key) = @_;
    Wikia::DW::Common::log("[DEBUG]\n$sql\n") if $Wikia::DW::Common::DEBUG;
    my $dbh = Wikia::DW::Common::statsdb;
    my $result = $dbh->selectall_hashref($sql, $key);
    $dbh->disconnect;
    return $result;
}

sub statsdb_row {
    my ($sql, $key) = @_;
    return Wikia::DW::Common::statsdb_arrayref($sql, 'hash')->[0];
}

sub query2csv {
    my ($source, $sql, $file) = @_;
    Wikia::DW::Common::log("[DEBUG]\n$sql\n") if $Wikia::DW::Common::DEBUG;
    my $q = Wikia::DW::ETL::Query->new( database   => Wikia::DW::ETL::Database->new( source => $source ),
                                        query      => $sql,
                                        processors => [ Wikia::DW::ETL::CSVWriter->new( filepath => $file ) ] );
    $q->run;
    $q->finalize;
}

sub load_query_file {
    my ($table, $type, $params) = @_;

    my $path = $table;
    $path =~ s!^([^_]+)(.+)!/usr/wikia/backend/lib/Wikia/DW/SQL/$1/$1$2_$type.sql!;

    if (-e $path) {
        open QUERY_FILE, "< $path" || die "Couldn't open $type file for $table";
            my @sql = <QUERY_FILE>;
        close QUERY_FILE;
        my $sql = join('', @sql);
        for my $k (keys %$params) {
            $sql =~ s/\[$k\]/$params->{$k}/ge;
        }
        return $sql;
    } else {
        # TODO: throw an exception here
        return '';
    }
}

sub exit_if_running {
	my (%param) = @_;
	my $prog = $0;
	$prog =~ s!^.*/!!;

	my $with = $param{with} ? '.+'.$param{with} : '';

	my $running = `ps -ef | egrep 'perl.+$prog$with' | grep -v grep | wc -l`;
	chomp($running);
	
	if ($running > 1) {
    	Wikia::DW::Common::log("  script is already running");
    	exit();
	}
}

sub log {
    my $msg = shift;
    chomp($msg);
    my $ts = `date +"%Y-%m-%d %H:%M:%S"`;
    chomp($ts);
    print "$ts : $msg\n";
}

sub debug {
    my $msg = shift;
    chomp($msg);
    Wikia::DW::Common::log($msg) if $DEBUG > 0;
}

1;
