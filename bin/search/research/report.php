<?php

include('config.php');

$OPTION_WITH_ARGS = array( 'wiki_id', 'from', 'to' );

$options = __parseArgv( $argv, $OPTION_WITH_ARGS );

if( count($options) == 0 ) {
	echo "Usage:\n";
	echo " " . $argv[0] . " [--wiki_id=NNN] [--from=\"datetime_string\"] [--to=\"datetime_string\"] [--rver_id=NNN] [--rver_not_id=NNN]\n\n";
	exit;
}

if( array_key_exists( 'wiki_id', $options )) {
	$wikiId = $options['wiki_id'];
}
else {
	$wikiId = false;
}

if( array_key_exists( 'from', $options ) ) {
	$dateFrom = date('Y-m-d H:i:s', strtotime($options['from']));
}

if( array_key_exists( 'to', $options ) ) {
	$dateTo = date('Y-m-d H:i:s', strtotime($options['to']));
}

if( array_key_exists( 'rver_id', $options ) ) {
	$rverId = $options['rver_id'];
}

if( array_key_exists( 'rver_not_id', $options ) ) {
	$rverNotId = $options['rver_not_id'];
}

if( !empty( $dateFrom ) || !empty( $dateTo ) ) {
	echo "--- DATA RANGE:\n" . ( !empty( $dateFrom ) ? ( " FROM: " . $dateFrom . "\n" ) : "" ) . ( !empty( $dateTo ) ? ( "   TO: " . $dateTo . "\n" ) : "" );
}
if( !empty( $rverId ) ) {
	echo "--- RELEVANCY FUNCTION ID: $rverId\n";
}


$events = getEventStats( $wikiId );
getReciprocalRank( $wikiId, $events['search_start'], $events['search_start_gomatch'] );
getADisRank($wikiId);
if( empty( $rverId ) ) {
	getReciprocalRank( $wikiId, $events['search_start'], 0 );
	if( !empty( $wikiId ) ) {
		$terms = getTopTerms( $wikiId );
	}
}
//foreach( $terms as $term => $cnt ) {
	//$events = getEventStats( $wikiId, $term );
	//getReciprocalRank( $wikiId, $events['search_start'], $events['search_start_gomatch'] );
	//getReciprocalRank( $wikiId, $events['search_start'], 0 );
//}



function getTopTerms( $wikiId, $limit = 20 ) {
	$db = __getDb();

	$where = "WHERE type IN ('search_start','search_start_nomatch')";
	if(!empty($wikiId)) {
		$where .= " AND wiki_id='" . $wikiId. "'";
	}
	$where .= __getDateRangeClause();

	$result = mysql_query("SELECT count(*) AS count, sterm AS term FROM event $where GROUP BY sterm HAVING count > 0 ORDER BY count DESC LIMIT $limit");

	if(!$result) {
		echo mysql_error();
	}

	echo "--- TOP $limit SEARCHED TERMS" . ( !empty($wikiId) ? " (WIKI ID=$wikiId)" : "" ) . ":\n";
	echo "COUNT TERM\n";
	$terms = array();
	while ($row = mysql_fetch_assoc($result)) {
		$terms[$row['term']] = $row['count'];
		echo $row['count'] . " " . $row['term'] . "\n";
	}
	echo "\n";

	mysql_free_result($result);
	mysql_close($db);
	return $terms;
}



function getEventStats( $wikiId, $searchTerm = '') {
	global $EVENT_TYPES;
	$db = __getDb();

	echo "--- EVENTS PER TYPE" . ( !empty($wikiId) ? " (WIKI ID=$wikiId)" : "" ) . ( !empty($searchTerm) ? " - SEARCH TERM: $searchTerm" : "" ) . ":\n";
	$events = array();
	$total = 0;
	foreach( $EVENT_TYPES as $type ) {
		$query = "SELECT count(*) AS count FROM event WHERE type='" . $type ."'" . ( !empty($wikiId) ? " AND wiki_id='" . $wikiId ."'" : "" ) . ( !empty($searchTerm) ? " AND sterm='" . addslashes($searchTerm). "'" : "" ) . __getDateRangeClause() . __getRelevancyVerClause();
		//echo $query . "\n";
		$result = mysql_query($query);
		$row = mysql_fetch_assoc($result);
		$events[$type] = $row['count'];
		$total += $row['count'];
	}

	arsort($events);

	echo "COUNT TYPE\n";
	foreach( $events as $type => $count ) {
		echo $count . " " . $type . " (" . __getPercent($count, $total) . "%)\n";
	}
	echo "TOTAL: $total\n\n";

	mysql_free_result($result);
	mysql_close($db);
	return $events;
}



function getReciprocalRank( $wikiId, $startEvents = 0, $goSearchEvents = 0 ) {
	$db = __getDb();

	$query = "SELECT SUM(1/pos) AS mrp, count(*) AS cnt FROM event WHERE type='search_click'". ( !empty($wikiId) ? " AND wiki_id='" . $wikiId ."'" : "" ) . __getDateRangeClause() . __getRelevancyVerClause();
	$result = mysql_query($query);

	$row = mysql_fetch_assoc($result);

	$startToClickDiff = $startEvents - $row['cnt'];
	$cnt = $row['cnt'] + $goSearchEvents + $startToClickDiff;
	$mrp = !empty($row['mrp']) ? $row['mrp'] : 0;
	$mrp += $goSearchEvents;

	echo "--- MEAN RECIPROCAL RANK" . ( !empty($wikiId) ? " (WIKI ID=$wikiId)" : "" ) . ( !empty($goSearchEvents) ? " - \"GO-SEARCH\" EVENTS INCLUDED" : "" ) . ":\n";
	echo " MRR: $mrp [clicks={$row['mrp']}+go_search={$goSearchEvents}] / $cnt [clicks_cnt={$row['cnt']}+go_search_cnt={$goSearchEvents}+diff(start_cnt-clicks_cnt)={$startToClickDiff}] = " . round( ($mrp/$cnt), 4). "\n\n";
}

function getADisRank( $wikiId ) {
	$db = __getDb();

	$query = "SELECT SUM(1/pos) AS mrp, count(*) AS cnt FROM event WHERE type='search_click'". ( !empty($wikiId) ? " AND wiki_id='" . $wikiId ."'" : "" ) . __getDateRangeClause() . __getRelevancyVerClause();
	$result = mysql_query($query);

	$row = mysql_fetch_assoc($result);

	$cnt = $row['cnt'];
	$mrp = !empty($row['mrp']) ? $row['mrp'] : 0;

	echo "--- ADi's RANK" . ( !empty($wikiId) ? " (WIKI ID=$wikiId)" : "" ). ":\n";
	echo " AR: $mrp [clicks={$row['mrp']}] / $cnt [clicks_cnt={$row['cnt']}] = " . round( ($mrp/$cnt), 4). "\n\n";
}



function __getPercent( $value, $total ) {
	return ($total > 0 ) ? round( (( 100 * $value ) / $total ), 2 ) : 0;
}



function __getDb() {
	$db = @mysql_connect( DB_HOST, DB_USER, DB_PASS );
	mysql_select_db( DB_NAME );

	return $db;
}



function __getDateRangeClause() {
	global $dateFrom, $dateTo;

	$clause = "";
	if(!empty($dateFrom)) {
		$clause = " AND UNIX_TIMESTAMP(date) > ". strtotime($dateFrom);
	}

	if(!empty($dateTo)) {
		$clause .= ( !empty($clause) ? " AND" : "" ) . " UNIX_TIMESTAMP(date) < ". strtotime($dateTo);
	}

	return $clause;
}



function __getRelevancyVerClause() {
	global $rverId, $rverNotId;

	$clause = "";
	if(!empty($rverId)) {
		$clause = " AND rver='". $rverId . "'";
	}
	if(!empty($rverNotId)) {
		$clause .= " AND rver NOT IN (". $rverNotId . ")";
	}
	return $clause;
}



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
