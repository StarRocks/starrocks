# Paimon Hive catalog SQL test

`test_paimon_hms_files` registers the existing `pk_merge_v1` fixture as an
external Paimon table in a unique temporary Hive database. It queries the
`$files` system table through an HMS-backed Paimon catalog with the JNI reader
and checks the reader trace. HiveServer2 is used to create the database; the
table metadata is registered directly in HMS without requiring Paimon classes
on the HiveServer2 classpath.

The case requires the existing OSS fixture, HiveServer2 and an HMS configured
in `test/conf/sr.conf`. Cleanup drops the temporary table metadata and database
without deleting fixture data. For local HDFS tests, override
`paimon_hms_prefix`, `paimon_hms_warehouse` and
`paimon_hms_fixture_location` in a private SQL-tester config.
