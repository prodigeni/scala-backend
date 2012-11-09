<?php

include('config.php');

if(isset($argv[1])) {
	if( $argv[1] == '--dir' ) {
		$dirName = $argv[2];
	}
	else {
		$fileName = $argv[1];
	}
}

if( empty( $fileName ) && empty( $dirName )) {
	echo "Usage:\n";
	echo " {$argv[0]} log_file_path\n";
	echo " {$argv[0]} --dir log_dir_path\n\n";
	exit;
}
else {
	$startTime = time();
	if( !empty( $dirName ) ) {
		$result = processLogDir( $dirName );
		$filesCnt = $result['filesCnt'];

		unset($result['filesCnt']);
		var_dump( $result );
	}
	else {
		$result = processLogFile( $fileName );
		$filesCnt = 1;
		var_dump( $result );
	}
	__getTimestampedLog("PROCESSING TIME: " . ( time() - $startTime ) . " sec. ( $filesCnt file(s) )\n");
}


function processLogDir( $dirName ) {
	if($dir = opendir( $dirName)) {
		__getTimestampedLog( "Processing directory: $dirName\n");
		$stats = array();
		$filesCnt = 0;
		while (false !== ($file = readdir($dir))) {
			if ($file != "." && $file != "..") {
				$result = processLogFile( $dirName . '/' . $file );
				if( is_array( $result ) ) {
					$filesCnt++;
					foreach( $result as $evtType => $evtCnt ) {
						$stats[$evtType] = isset( $stats[$evtType] ) ? ( $stats[$evtType] + $evtCnt ) : $evtCnt;
					}
				}
			}
		}
		closedir($dir);
		$stats['filesCnt'] = $filesCnt;
		return $stats;
	}
	else {
		__getTimestampedLog( "Error opening log directory: $dirName" );
		return false;
	}
}


function processLogFile( $fileName ) {
	global $EVENT_TYPES;
	if( file_exists( $fileName) ) {
		__getTimestampedLog("Processing file: $fileName ...");

		$file = gzopen( $fileName, 'r' );

		$db = @mysql_connect( DB_HOST, DB_USER, DB_PASS );
		if (!$db) {
			die('Could not connect: ' . mysql_error());
		}
		mysql_select_db( DB_NAME );

		$beaconCnt = 0;
		$eventsCnt = array( 'unknown' => 0 );

		//while( ( $line = fgets($file, 4096) ) !== false) {
		while( !gzeof($file) ) {
			$line = gzgets($file, 4096);
			$lineChunks = preg_split( '/\s+/', $line );
			$eventDate = date( 'Y-m-d H:i:s', strtotime( $lineChunks[0] . " " . $lineChunks[1] . " " . $lineChunks[2] ) );

			if($lineChunks[6] == 'BEACON:') {
				$trackData = __getTrackData( $lineChunks[8] );
				if($trackData === false) {
					// no event type found, ignore
					continue;
				}

				$beaconCnt++;
				$beaconId = $lineChunks[7];

				if( isset( $trackData['type']) && in_array( $trackData['type'], $EVENT_TYPES ) ) {
					// it's search event
					$eventType = $trackData['type'];
					if(isset($eventsCnt[$eventType])) {
						$eventsCnt[$eventType]++;
					}
					else {
						$eventsCnt[$eventType] = 1;
					}

					$result = @mysql_query( "INSERT INTO event (date,type,beacon,wiki_id,lang,sterm,rver,pos,stype)
					 VALUES (
					  '" . $eventDate . "',
					  '" . $eventType . "',
					  '" . $beaconId . "',
					  '" . $trackData['c'] . "',
					  '" . $trackData['lc'] . "',
					  '" . addslashes( urldecode( $trackData['sterm'] ) ). "',
					  '" . $trackData['rver'] . "',
					  '" . ( isset($trackData['pos']) ? $trackData['pos'] : 0 ) . "',
					  " . ( isset($trackData['stype']) ? ( "'" . $trackData['stype'] . "'" ) : 'NULL' ). ")" );
					if (!$result) {
						die("Invalid query: " . mysql_error());
					}
				}
				else {
					$eventsCnt['unknown']++;
				}
			}
			else {
				echo "Something's wrong withe the log file!\n";
			}
			//echo $line . "\n\n";
			//if($beaconCnt > 3) break;
		}
		gzclose($file);

		mysql_close($db);
		$eventsCnt['beaconCnt'] = $beaconCnt;
		return $eventsCnt;
	}
	else {
		__getTimestampedLog("File: {$fileName} doesn't exists.\n");
		return false;
	}
}



function __getTrackData( $string ) {
	$string = strtr( $string, array( '&amp;' => '&' ) );
	$stringParts = explode( '?', $string );
	$eventTypeParts = explode( '/', $stringParts[0]);

	$trackRawData = preg_split( '/&/', substr( $string, strpos( $string, '?' )+1 ) );

	$trackData = array();
	foreach( $trackRawData as $data ) {
		$dataChunk = preg_split( '/=/', $data );
		if(isset($dataChunk[0]) && isset($dataChunk[1])) {
			$trackData[$dataChunk[0]] = $dataChunk[1];
		}
	}

	if( !isset($trackData['type']) ) {
		if( isset($eventTypeParts[3]) ) {
			$trackData['type'] = $eventTypeParts[3];
		}
		else {
			// fatal error
			__getTimestampedLog( "ERROR: No event type found!\nLOG LINE: $string\n" );
			return false;
		}
	}

	return $trackData;
}



function __getTimestampedLog( $msg ) {
	echo date('[H:i:s]') . " $msg\n";
}
