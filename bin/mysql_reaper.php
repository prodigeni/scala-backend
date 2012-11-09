<?php
/* Don't fear the reaper.
   Script to kill long running queries.
*/
ini_set('display_errors', true);

$usage=<<<EOU
  Usage:
  --username	Database Username (must have kill priveledge [SUPER]) {Default ''}
  --password	Database Password {Default ''}
  --host	Database Hostname {Default 'localhost'}
  --database	Only kill queries in this Database {OPTIONAL}
  --timelimit	Number of seconds query is allowed to run {REQUIRED}
  --pattern	Regexp to match against the query {REQUIRED}. Ex: '/\/\* DPL/'
  --mailto	Mail these addresses if a process is killed

EOU;

// ########## Parse Arguments
$defaults=array(
  'username'=>'',
  'password'=>'',
  'host'=>'',
  'database'=>'',
  'timelimit'=>0,
  'pattern'=>'',
  'mailto'=>''
);
$options=getOptions($argv, $defaults);

if (empty($options['host'])){
  $options['host']='localhost';
}
if (empty($options['timelimit'])){
  echo "Error: You must specify a timelimit.\n\n";
  echo $usage;
  exit;
}
if (empty($options['pattern'])){
  echo "Error: You must specify a pattern.\n\n";
  echo $usage;
  exit;
}

set_error_handler('WikiaWarningHandler', E_WARNING);
 
function WikiaWarningHandler ($errnr, $errmsg, $errfile, $errline){
	global $options;
	$error = array( 
		'number' => $errnr,
		'message' => $errmsg,
		'line' => $errline,
		'file' => $errfile
	);
	$msg = "Warning: host: " . $options['host'] . ", ";
	if ( isset( $options['database'] ) ) {
		$msg .= "database: " . $options['database'] . ", ";
	}
	$msg .= "Error: ";
	foreach ( $error as $key => $val ) {
		$msg .= "{$key}: {$val} ";
	}
    echo $msg . "\n";
    error_log ( $msg );
}

// ########## Connect to DB
try {
	$db = mysql_connect( $options['host'], $options['username'], $options['password'] );
	if ( !$db ) {
		throw new Exception('Error connecting to database: ' . mysql_error());
	}
} 
catch ( Exception $e ) {
	$msg = "MySQL: {$options['host']}, username: {$options['username']}, msg: ". $e->getMessage() . "\n";
	echo $msg;
	error_log ( $msg );
	exit;
}

if (! $result=mysql_query("SHOW FULL PROCESSLIST")){
  echo "Error issuing 'SHOW FULL PROCESSLIST' query: " . mysql_error() . "\n";
  exit;
}

// Fake queries for testing
//SELECT BENCHMARK(10000000,ENCODE('jim','bob'));

$counts=array();
while ($row=mysql_fetch_assoc($result)){
  @$counts['Queries Checked']++;
  //print_r($row);

  if ($row['Command'] !='Query'){
    @$counts['Processes skipped - not a query']++;
    continue;
  }
 
  // Database
  if (!empty($options['database']) && $row['db'] != $options['database']){ 
    // Database does not match
    @$counts['Processes skipped - database did not match']++;
    continue;
  }
  // Pattern
  if (!preg_match($options['pattern'], $row['Info'])){ 
    @$counts['Processes skipped - pattern did not match']++;
    // Not the right Query pattern. 
    continue;
  }

  // Time
  if ($row['Time'] < $options['timelimit']){ 
    // Not enough time
    @$counts['Processes skipped - not enough elapsed time']++;
    continue;
  }

  // All conditions met, query needs to die.
  if (! $kill=mysql_query("KILL {$row['Id']}")){
    echo "Error issuing 'KILL {$row['Id']}' query: " . mysql_error() . "\n";
    @$counts['Kills Failed']++;
  } else {
    @$counts['Queries Killed']++;
  }
}

print_r($counts);

if (!empty($options['mailto'])){
  mail($options['mailto'], "mysql_reaper found hung queries", print_r($counts, true));
}

// DONE




/* Pass $argv from command line, and it will return an associative array with name/value pairs.
  Supported formats:
        -o (short)
        -o value (short with arg)
        --option (long)
        --option=value (long with arg)

  This was intentionally designed to be quiet about errors - just graceful failure. 

  It's assumed that the calling program will check to see if critical options are set.

  FIXME: Move this to a central file?
*/
function getOptions($args, $defaults) {
  if (!is_array($defaults)){
    $a=array();
  } else {
    $a=$defaults;
  }
 
  $previousShortArg='';
  for ($i=1; $i<sizeof($args); $i++){
    if (substr($args[$i], 0, 2)=='--'){
      if (preg_match_all('/(--)([^=]+)(=)"?(.+)"?/', $args[$i], $matches)){
        // long with arg
        $option=$matches[2][0];
        $a[$option]=$matches[4][0];
      } else {
        // long
        $option=substr($args[$i], 2);
        $a[$option]=true;
      }
    } else if (substr($args[$i], 0, 1)=='-'){
      // short
      $option=$args[$i]{1};
      $a[$option]=true;
      $previousShortArg=$option;
    } else if (!empty($previousShortArg)){
      // short with arg
      $a[$previousShortArg]=$args[$i];
    }
  }
  return $a;
}



?>
