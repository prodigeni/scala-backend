package Wikia::BinLog;

my $bincmd = '/usr/bin/mysqlbinlog';

sub new {
	my $class = shift;
	my (%param) = @_;
	my $self = bless {}, ref $class || $class;
	
	$self->{user}     = $param{user};
	$self->{pass}     = $param{pass};
	$self->{host}     = $param{host};
	$self->{database} = $param{database};
	
	$self->{file}   = $param{file};
	$self->{start}  = $param{start};
	$self->{decode} = $param{decode} || 0;
	
	$self->{binlog_pipe}   = undef;
	$self->{cur_record}    = undef;
	$self->{cur_position}  = 0;
	$self->{next_position} = 0;
	
	$self->open_binlog;
	$self->skip_to_first_position;
	
	return $self;
}

sub DESTROY {
	my $self = shift;
	
	# Exit mysqlbinlog if we destroy this object before the end of the program
	close($self->{binlog_pipe});
}

sub open_binlog {
	my $self = shift;
	my @args;
	
	# Optional args
	push @args, '--start-position '.$self->{start} if $self->{start};
	push @args, '--database '.$self->{database} if $self->{database};
	push @args, '-u'.$self->{user} if $self->{user};
	push @args, '-p'.$self->{pass} if $self->{pass};
	push @args, '--host '.$self->{host} if $self->{host};
	push @args, '-R' if $self->{host};
	if ($self->{decode}) {
		push @args, '--base64-output=DECODE-ROWS';
		push @args, '--verbose';
	}

	# Required args
	push @args, '--to-last-log';
	push @args, $self->{file};
	
	my $full_cmd = $bincmd.' '.join(' ', @args);

	open($self->{binlog_pipe}, "$full_cmd|") or die "Can't run '$bincmd': $!\n";
}

sub skip_to_first_position {
	my $self = shift;
	my $pipe = $self->{binlog_pipe};
	
	while (my $line = <$pipe>) {
		# Look for the next position marker
		if ($line =~ /^# at (\d+)/) {
			$self->{next_position} = $1;
			last;
		}
	}
}

sub read_record {
	my $self = shift;
	
	# If there's no next position we're done
	return unless $self->{next_position};

	$self->{cur_position} = $self->{next_position};
	$self->{next_position} = undef;

	my @record;
	my $pipe = $self->{binlog_pipe};
	while (my $line = <$pipe>) {
		# Look for the next position marker
		if ($line =~ /^# at (\d+)/) {
			$self->{next_position} = $1;
			last;
		}

		chomp($line);
		push @record, $line;
	}
	$self->{cur_record} = \@record;

	return $self->{cur_record};
}

sub goto_position {
	my $self = shift;
	my ($pos) = @_;
	
	while ($self->{cur_position} != $pos) {
		# Read the next record and return with nothing if we hit the end without
		# finding it
		my $rec = $self->read_record;
		return unless $rec;
	}

	return $self->{cur_record};
}

sub get_position {
	my $self = shift;
	return $self->{cur_position};
}

sub cur_record {
	my $self = shift;
	return $self->{cur_record};
}

sub next_record {
	my $self = shift;
	return unless $self->read_record;
	return $self->cur_record;
}

1;