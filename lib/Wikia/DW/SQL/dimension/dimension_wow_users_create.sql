CREATE TABLE [schema].[table] (
    user_id               INTEGER UNSIGNED NOT NULL,
    user_name             VARCHAR(255) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT '',
    user_real_name        VARCHAR(255) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT '',
    user_email            TINYTEXT,
    user_email_authenticated CHAR(14),
    user_editcount        INTEGER,
    user_registration     DATETIME,
    user_is_bot           BOOLEAN,
    user_is_bot_global    BOOLEAN,
    user_marketingallowed BOOLEAN,
    PRIMARY KEY (user_id)
) ENGINE=InnoDB;

