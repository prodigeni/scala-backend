<?php
function __parseArgv(Array $arguments, Array $optionsWithArgs) {
	$args = array();
	$options = array();

	for( $arg = reset( $arguments ); $arg !== false; $arg = next( $arguments ) ) {
		if ( $arg == '--' ) {
			# End of options, remainder should be considered arguments
			$arg = next( $arguments );
			while( $arg !== false ) {
				$args[] = $arg;
				$arg = next( $arguments );
			}
			break;
		} elseif ( substr( $arg, 0, 2 ) == '--' ) {
			# Long options
			$option = substr( $arg, 2 );
			if ( in_array( $option, $optionsWithArgs ) ) {
				$param = next( $arguments );
				if ( $param === false ) {
					echo "$arg needs a value after it\n";
					die( -1 );
				}
				$options[$option] = $param;
			} else {
				$bits = explode( '=', $option, 2 );
				if( count( $bits ) > 1 ) {
					$option = $bits[0];
					$param = $bits[1];
				} else {
					$param = 1;
				}
				$options[$option] = $param;
			}
		} elseif ( substr( $arg, 0, 1 ) == '-' ) {
			# Short options
			for ( $p=1; $p<strlen( $arg ); $p++ ) {
				$option = $arg{$p};
				if ( in_array( $option, $optionsWithArgs ) ) {
					$param = next( $arguments );
					if ( $param === false ) {
						echo "$arg needs a value after it\n";
						die( -1 );
					}
					$options[$option] = $param;
				} else {
					$options[$option] = 1;
				}
			}
		} else {
			$args[] = $arg;
		}
	}

	return $options;
}
