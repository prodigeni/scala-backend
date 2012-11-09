package Wikia::Scribe;
use strict;
use base qw(Class::Accessor);

Wikia::Scribe->mk_accessors(qw(
	$scribeKeys
));

use constant EDIT_CATEGORY 			=> 'log_edit';
use constant CREATEPAGE_CATEGORY	=> 'log_create';
use constant UNDELETE_CATEGORY		=> 'log_undelete';
use constant DELETE_CATEGORY		=> 'log_delete';
use constant UPLOAD_CATEGORY		=> 'log_upload';
use constant PHALANX_CATEGORY		=> 'log_phalanx';

our $scribeKeys = {
	Wikia::Scribe::EDIT_CATEGORY 		=> 1, 
	Wikia::Scribe::CREATEPAGE_CATEGORY 	=> 2, 
	Wikia::Scribe::DELETE_CATEGORY		=> 3,
	Wikia::Scribe::UNDELETE_CATEGORY	=> 4, 
	Wikia::Scribe::UPLOAD_CATEGORY		=> 5 
};

use constant SCRIBE_EVENTS_TABLE	=> 'scribe_events';
use constant SCRIBE_PHALANX_TABLE	=> 'phalanx_stats';

1;
__END__
