CREATE TABLE event (
	`id` INT(11) NOT NULL PRIMARY KEY AUTO_INCREMENT,
	`date` DATETIME,
	`type` VARCHAR(32) NOT NULL,
	`beacon` VARCHAR(16) NOT NULL,
	`wiki_id` INT(11) NOT NULL,
	`lang` VARCHAR(6),
	`sterm` VARCHAR(255) DEFAULT NULL,
	`stype` VARCHAR(8) DEFAULT NULL,
	`rver` INT DEFAULT 0,
	pos INT DEFAULT 0,
	KEY `date_idx` (`date`),
	KEY `date_wiki_idx` (`date`,`wiki_id`),
	KEY `wiki_idx` (`wiki_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
