#
# Autogenerated by Thrift
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
require 5.6.0;
use strict;
use warnings;
use Thrift;

use Scribe::Thrift::Types;
use Facebook::FB303::FacebookService;

# HELPER FUNCTIONS AND STRUCTURES

package Scribe::Thrift::scribe_Log_args;
use base qw(Class::Accessor);
Scribe::Thrift::scribe_Log_args->mk_accessors( qw( messages ) );

sub new {
  my $classname = shift;
  my $self      = {};
  my $vals      = shift || {};
  $self->{messages} = undef;
  if (UNIVERSAL::isa($vals,'HASH')) {
    if (defined $vals->{messages}) {
      $self->{messages} = $vals->{messages};
    }
  }
  return bless ($self, $classname);
}

sub getName {
  return 'scribe_Log_args';
}

sub read {
  my ($self, $input) = @_;
  my $xfer  = 0;
  my $fname;
  my $ftype = 0;
  my $fid   = 0;
  $xfer += $input->readStructBegin(\$fname);
  while (1) 
  {
    $xfer += $input->readFieldBegin(\$fname, \$ftype, \$fid);
    if ($ftype == TType::STOP) {
      last;
    }
    SWITCH: for($fid)
    {
      /^1$/ && do{      if ($ftype == TType::LIST) {
        {
          my $_size0 = 0;
          $self->{messages} = [];
          my $_etype3 = 0;
          $xfer += $input->readListBegin(\$_etype3, \$_size0);
          for (my $_i4 = 0; $_i4 < $_size0; ++$_i4)
          {
            my $elem5 = undef;
            $elem5 = new Scribe::Thrift::LogEntry();
            $xfer += $elem5->read($input);
            push(@{$self->{messages}},$elem5);
          }
          $xfer += $input->readListEnd();
        }
      } else {
        $xfer += $input->skip($ftype);
      }
      last; };
        $xfer += $input->skip($ftype);
    }
    $xfer += $input->readFieldEnd();
  }
  $xfer += $input->readStructEnd();
  return $xfer;
}

sub write {
  my ($self, $output) = @_;
  my $xfer   = 0;
  $xfer += $output->writeStructBegin('scribe_Log_args');
  if (defined $self->{messages}) {
    $xfer += $output->writeFieldBegin('messages', TType::LIST, 1);
    {
      $output->writeListBegin(TType::STRUCT, scalar(@{$self->{messages}}));
      {
        foreach my $iter6 (@{$self->{messages}}) 
        {
          $xfer += ${iter6}->write($output);
        }
      }
      $output->writeListEnd();
    }
    $xfer += $output->writeFieldEnd();
  }
  $xfer += $output->writeFieldStop();
  $xfer += $output->writeStructEnd();
  return $xfer;
}

package Scribe::Thrift::scribe_Log_result;
use base qw(Class::Accessor);
Scribe::Thrift::scribe_Log_result->mk_accessors( qw( success ) );

sub new {
  my $classname = shift;
  my $self      = {};
  my $vals      = shift || {};
  $self->{success} = undef;
  if (UNIVERSAL::isa($vals,'HASH')) {
    if (defined $vals->{success}) {
      $self->{success} = $vals->{success};
    }
  }
  return bless ($self, $classname);
}

sub getName {
  return 'scribe_Log_result';
}

sub read {
  my ($self, $input) = @_;
  my $xfer  = 0;
  my $fname;
  my $ftype = 0;
  my $fid   = 0;
  $xfer += $input->readStructBegin(\$fname);
  while (1) 
  {
    $xfer += $input->readFieldBegin(\$fname, \$ftype, \$fid);
    if ($ftype == TType::STOP) {
      last;
    }
    SWITCH: for($fid)
    {
      /^0$/ && do{      if ($ftype == TType::I32) {
        $xfer += $input->readI32(\$self->{success});
      } else {
        $xfer += $input->skip($ftype);
      }
      last; };
        $xfer += $input->skip($ftype);
    }
    $xfer += $input->readFieldEnd();
  }
  $xfer += $input->readStructEnd();
  return $xfer;
}

sub write {
  my ($self, $output) = @_;
  my $xfer   = 0;
  $xfer += $output->writeStructBegin('scribe_Log_result');
  if (defined $self->{success}) {
    $xfer += $output->writeFieldBegin('success', TType::I32, 0);
    $xfer += $output->writeI32($self->{success});
    $xfer += $output->writeFieldEnd();
  }
  $xfer += $output->writeFieldStop();
  $xfer += $output->writeStructEnd();
  return $xfer;
}

package Scribe::Thrift::scribeIf;

use strict;
use base qw(Facebook::FB303::FacebookServiceIf);

sub Log{
  my $self = shift;
  my $messages = shift;

  die 'implement interface';
}

package Scribe::Thrift::scribeRest;

use strict;
use base qw(Facebook::FB303::FacebookServiceRest);

sub Log{
  my ($self, $request) = @_;

  my $messages = ($request->{'messages'}) ? $request->{'messages'} : undef;
  return $self->{impl}->Log($messages);
}

package Scribe::Thrift::scribeClient;

use base qw(Facebook::FB303::FacebookServiceClient);
use base qw(Scribe::Thrift::scribeIf);
sub new {
  my ($classname, $input, $output) = @_;
  my $self      = {};
  $self = $classname->SUPER::new($input, $output);
  return bless($self,$classname);
}

sub Log{
  my $self = shift;
  my $messages = shift;

    $self->send_Log($messages);
  return $self->recv_Log();
}

sub send_Log{
  my $self = shift;
  my $messages = shift;

  $self->{output}->writeMessageBegin('Log', TMessageType::CALL, $self->{seqid});
  my $args = new Scribe::Thrift::scribe_Log_args();
  $args->{messages} = $messages;
  $args->write($self->{output});
  $self->{output}->writeMessageEnd();
  $self->{output}->getTransport()->flush();
}

sub recv_Log{
  my $self = shift;

  my $rseqid = 0;
  my $fname;
  my $mtype = 0;

  $self->{input}->readMessageBegin(\$fname, \$mtype, \$rseqid);
  if ($mtype == TMessageType::EXCEPTION) {
    my $x = new TApplicationException();
    $x->read($self->{input});
    $self->{input}->readMessageEnd();
    die $x;
  }
  my $result = new Scribe::Thrift::scribe_Log_result();
  $result->read($self->{input});
  $self->{input}->readMessageEnd();

  if (defined $result->{success} ) {
    return $result->{success};
  }
  die "Log failed: unknown result";
}
package Scribe::Thrift::scribeProcessor;

use strict;
use base qw(Facebook::FB303::FacebookServiceProcessor);

sub process {
    my ($self, $input, $output) = @_;
    my $rseqid = 0;
    my $fname  = undef;
    my $mtype  = 0;

    $input->readMessageBegin(\$fname, \$mtype, \$rseqid);
    my $methodname = 'process_'.$fname;
    if (!$self->can($methodname)) {
      $input->skip(TType::STRUCT);
      $input->readMessageEnd();
      my $x = new TApplicationException('Function '.$fname.' not implemented.', TApplicationException::UNKNOWN_METHOD);
      $output->writeMessageBegin($fname, TMessageType::EXCEPTION, $rseqid);
      $x->write($output);
      $output->writeMessageEnd();
      $output->getTransport()->flush();
      return;
    }
    $self->$methodname($rseqid, $input, $output);
    return 1;
}

sub process_Log {
    my ($self, $seqid, $input, $output) = @_;
    my $args = new Scribe::Thrift::scribe_Log_args();
    $args->read($input);
    $input->readMessageEnd();
    my $result = new Scribe::Thrift::scribe_Log_result();
    $result->{success} = $self->{handler}->Log($args->messages);
    $output->writeMessageBegin('Log', TMessageType::REPLY, $seqid);
    $result->write($output);
    $output->writeMessageEnd();
    $output->getTransport()->flush();
}

1;
