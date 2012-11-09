CREATE TABLE etl_kn_file_loads (
	file_id			INT,
	
	load_ts			TIMESTAMP,
	min_event_ts	DATETIME,
	max_event_ts	DATETIME,

	PRIMARY KEY (file_id)
);


-- Describes the agent taking the measurement (e.g. Boston Verizon TxP)
--
CREATE TABLE dimension_kn_agent (
	agent_id	INTEGER,

	backbone	VARCHAR(128),
	weight		TINYINT,
	country		VARCHAR(64),
	region		VARCHAR(64),
	city		VARCHAR(64),
	description	VARCHAR(255),
	
	PRIMARY KEY (agent_id)
);

-- Describes one of many instances of an agent (e.g multiple hosts with different IPs)
--
CREATE TABLE dimension_kn_agent_instance (
	agent_id	INTEGER,
	instance_id	INTEGER,

	ip			VARCHAR(15),

	PRIMARY KEY (agent_id, instance_id)
);

-- Describes a configured measurement (e.g. the Jim Henson page on muppet)
--
CREATE TABLE dimension_kn_slot (
	slot_id		INTEGER,

	slot_alias	VARCHAR(255),
	pages		TINYINT,
	subservice	VARCHAR(16),
	
	PRIMARY KEY (slot_id)
);

-- Describes the one (or more) pages that will be retrieved while taking a
-- measurement. More than one page could be retrieved if there is a login step
-- first followed by browsing to a content page.  Alternately, virtual pages
-- will show up as separate pages for a measurement
--
CREATE TABLE dimension_kn_page (
	slot_id		INTEGER,
	page_seq	TINYINT,

	page_url	TEXT,
	page_alias	VARCHAR(255),
	
	PRIMARY KEY (slot_id, page_seq)
);

-- Summarizes a measurement taken
--
CREATE TABLE fact_kn_measurement (
	measurement_id			BIGINT,
	file_id					INTEGER,

	agent_id				INTEGER,
	instance_id				INTEGER,
	slot_id					INTEGER,

	target					INTEGER,
	profile					TINYINT,

	bandwidth				FLOAT,
	delta					INTEGER,
	cache_delta				INTEGER,
	delta_user				INTEGER,

	domain_count			INTEGER,
	privacy_cookies_count	INTEGER,
	element_count			INTEGER,
	connection_count		INTEGER,

	content_errors			INTEGER,

	bytes					INTEGER,
	created					DATETIME,
	
	PRIMARY KEY (measurement_id)
);

-- Summarizes one page of a measurement taken
--
CREATE TABLE fact_kn_measurement_page (
	measurement_id			BIGINT,
	page_seq				SMALLINT,
	file_id					INTEGER,

	privacy_cookies_count	SMALLINT,
	delta					MEDIUMINT,
	dom_load_time			MEDIUMINT,
	ms_first_paint			MEDIUMINT,
	delta_user				MEDIUMINT,
	connection_count		SMALLINT,
	first_packet_delta		MEDIUMINT,
	domain_count			MEDIUMINT,
	start_time				MEDIUMINT,
	dns_lookup				MEDIUMINT,
	dom_content_load_time	MEDIUMINT,
	connect_delta			MEDIUMINT,
	first_byte				MEDIUMINT,
	dom_complete			MEDIUMINT,
	remain_packets_delta	MEDIUMINT,
	bandwidth				FLOAT,
	dom_interactive			MEDIUMINT,
	system_delta			MEDIUMINT,
	request_delta			MEDIUMINT,
	cache_delta				MEDIUMINT,

	element_count			MEDIUMINT,
	page_bytes				INTEGER,
	content_errors			MEDIUMINT,
	
	PRIMARY KEY (measurement_id, page_seq)
);

--  A measurement of a single URL within a page
--
CREATE TABLE fact_kn_page (
	measurement_id		BIGINT,
	page_seq			TINYINT,
	record_seq			SMALLINT,
	file_id				INTEGER,

	page				TINYINT,
	msmt_conn_id		SMALLINT,

	ip_address			VARCHAR(15),
	
	first_packet_delta	MEDIUMINT,
	system_delta		MEDIUMINT,
	connect_delta		MEDIUMINT,
	request_delta		MEDIUMINT,
	dns_delta			MEDIUMINT,
	start				MEDIUMINT,
	element_delta		MEDIUMINT,
	remain_packets_delta	MEDIUMINT,

	element_cached		TINYINT,

	request_bytes		INTEGER,
	content_bytes		INTEGER,
	header_bytes		INTEGER,

	content_type		VARCHAR(64),	-- 72
	header_code			TINYINT,		-- 1	ip_address			VARCHAR(15),
	
	conn_string_text	VARCHAR(255),	-- http://images.wikia.nocookie.net
	object_text			TEXT,			-- /__am/52265/sass/color-body%3D%2523000000%26color-page%3D%2523ffffff%26color-buttons%3D%2523337800%26color-links%3D%25230148c2%26color-header%3D%2523285f00%26background-image%3Dhttp%253A%252F%252Fimages4.wikia.nocookie.net%252F__cb129%252Fmuppet%252Fimages%252F5%252F50%252FWiki-background%26background-align%3Dcenter%26background-tiled%3Dfalse%26background-fixed%3Dtrue%26page-opacity%3D100/skins/oasis/css/oasis.scss
	
	status_code			TINYINT,

	PRIMARY KEY (measurement_id, page_seq, record_seq)
);