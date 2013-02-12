# Wikifactory is a scala library used to create connections do Wikia databases

Look at com.wikia.wikifactory.DB: connect method creates database session factory.
It requires either explict LBFactoryConf or it will load it from file pointed by
enviroment variable WIKIA_DB_YML, or /usr/wikia/conf/current/DB.yml if it's not set.

Tests:
* unittests - use sbt test
* create a real connection using WIKIA_DB_YML as above and test with sbt run:

```
$ sbt run
...
wikifactory test ok
...
```

