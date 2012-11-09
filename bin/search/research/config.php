<?php
define('DB_HOST', 'localhost');
define('DB_NAME', 'search_log');
define('DB_USER', 'log');
define('DB_PASS', 'log');

$EVENT_TYPES = array(
	'search_start',
	'search_start_gomatch',
	'search_start_suggest',
	'search_start_nomatch',
	'search_start_google',
	'search_click'
);

date_default_timezone_set('UTC');
