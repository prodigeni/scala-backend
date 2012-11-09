<?php
/**
 * search load testing script (for inter- and intra-wiki searching)
 * @author ADi
 */

include('config.php');
include('../common.php');

$OPTION_WITH_ARGS = array( 'type', 'phrase_file', 'pages_num' );

$options = __parseArgv( $argv, $OPTION_WITH_ARGS );

if( count($options) == 0 ) {
	echo "Usage:\n";
	echo " " . $argv[0] . " [--type=inter|intra] [--phrase_file=FILENAME] [--pages_num=N]\n\n";
	exit;
}

if( file_exists( $options['phrase_file'] )) {
	$phrases = explode("\n", file_get_contents($options['phrase_file']));
}
else {
	echo "Phrase file: \"{$options['phrase_file']}\" not found.\n\n";
	exit;
}

$type = empty($options['type']) ? 'inter' : $options['type'];
$pagesNum = empty($options['pages_num']) ? 3 : $options['pages_num'];
$phrasesNum = count($phrases);

echo "Number of phrases: " . $phrasesNum . "\n";

$timeAll = 0;
$phraseNum = 1;
foreach($phrases as $phrase) {
	echo "[ $phraseNum / $phrasesNum ] Searching for: " . $phrase . "\n";
	if(!empty($phrase)) {
		$timeStart = microtime(true);
		doSearch( $phrase, $type, $pagesNum );
		$timeTotal = microtime(true) - $timeStart;
		echo "Time elapsed: " . number_format( $timeTotal, 2 ) . "\n";
		$timeAll += $timeTotal;

		echo "Total: " . number_format( $timeAll, 2 ) . " (Avg. per request: " . number_format( ( $timeAll / ($phraseNum * $pagesNum) ), 2 ) . ")\n";
		$phraseNum++;
	}
}

echo "Total time: " . number_format( $timeAll, 2 ) . "\n";
echo "Avg. Time per request: " . number_format( ( $timeAll / ($phraseNum * $pagesNum) ), 2 ) . "\n";

function doSearch($phrase, $type = 'inter', $pagesNum = 3) {
	$curl = curl_init();

	for($i = 1; $i <= $pagesNum; $i++) {
		$params = array(
			'query' => $phrase,
			'crossWikia' => (($type == 'inter') ? 1 : 0),
			'page' => $i
		);

		curl_setopt($curl, CURLOPT_URL, SEARCH_URL);
		curl_setopt($curl, CURLOPT_HEADER, false);
		curl_setopt($curl, CURLOPT_POST, 1);
		curl_setopt($curl, CURLOPT_POSTFIELDS, $params);
		curl_setopt($curl, CURLOPT_RETURNTRANSFER, true);

		$output = curl_exec($curl);
		$matches = null;

		preg_match_all( '<section class="Result">', $output, $matches);
		if(count($matches[0]) == 0) {
			preg_match( '<div class="TechnicalStuff">', $output, $matches);
			if(count($matches)) {
				echo "-- Phrase: $phrase (Page: $i) - ERROR Page returned\n";
				//print_r($output);
			}
		}
		else {
			echo "-- Phrase: $phrase (Page: $i, Results: " . count($matches[0]) . ")\n";
		}
	}
}

