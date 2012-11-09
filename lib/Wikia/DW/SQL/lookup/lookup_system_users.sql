DROP TABLE IF EXISTS lookup_system_users;

CREATE TABLE lookup_system_users (
    user_id INTEGER UNSIGNED,
    PRIMARY KEY (user_id)
) ENGINE=InnoDB;

INSERT INTO lookup_system_users VALUES
    (22439),
    (49312),
    (269919),
    (375130),
    (929702),
    (4663069),
    (5028050),
    (5284841);

COMMIT;

