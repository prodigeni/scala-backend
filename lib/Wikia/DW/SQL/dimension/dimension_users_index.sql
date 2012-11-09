CREATE INDEX dimension_users_user_name  ON [schema].[table] (user_name);
CREATE INDEX dimension_users_user_email ON [schema].[table] (user_email(40));
CREATE INDEX dimension_users_user_registration ON [schema].[table] (user_registration);
