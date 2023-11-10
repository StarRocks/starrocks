---
displayed_sidebar: "English"
---

# All statements

This topic lists all the SQL statements supported by StarRocks and categorizes these statements by their functions.

- [All statements](#all-statements)
  - [User account management](#user-account-management)
  - [Cluster management](#cluster-management)
    - [FE, BE, CN, Broker, process](#fe-be-cn-broker-process)
    - [Resource group](#resource-group)
    - [Storage volume](#storage-volume)
    - [Table, tablet, replica](#table-tablet-replica)
    - [File, index, variable](#file-index-variable)
    - [SQL Blacklist](#sql-blacklist)
    - [Plugin](#plugin)
  - [Loading, unloading](#loading-unloading)
    - [Routine load](#routine-load)
    - [Other load](#other-load)
    - [Unloading](#unloading)
    - [ETL task](#etl-task)
  - [Catalog, database, resource](#catalog-database-resource)
    - [Catalog](#catalog)
    - [Database](#database)
    - [Resource](#resource)
  - [Create table, partition](#create-table-partition)
  - [View, materialized view](#view-materialized-view)
    - [View](#view)
    - [Materialized view](#materialized-view)
  - [Function, SELECT](#function-select)
  - [CBO statistics](#cbo-statistics)
  - [Backup and restore](#backup-and-restore)
  - [Utility commands](#utility-commands)

## User account management

Manages users, roles, and privileges.

- [ALTER USER](./account-management/ALTER_USER.md)
- [CREATE ROLE](./account-management/CREATE_ROLE.md)
- [CREATE USER](./account-management/CREATE_USER.md)
- [DROP ROLE](./account-management/DROP_ROLE.md)
- [DROP USER](./account-management/DROP_USER.md)
- [EXECUTE AS](./account-management/EXECUTE_AS.md)
- [GRANT](./account-management/GRANT.md)
- [REVOKE](./account-management/REVOKE.md)
- [SET DEFAULT ROLE](./account-management/SET_DEFAULT_ROLE.md)
- [SET PASSWORD](./account-management/SET_PASSWORD.md)
- [SET PROPERTY](./account-management/SET_PROPERTY.md)
- [SET ROLE](./account-management/SET_ROLE.md)
- [SHOW AUTHENTICATION](./account-management/SHOW_AUTHENTICATION.md)
- [SHOW GRANTS](./account-management/SHOW_GRANTS.md)
- [SHOW PROPERTY](./account-management/SHOW_PROPERTY.md)
- [SHOW ROLES](./account-management/SHOW_ROLES.md)
- [SHOW USERS](./account-management/SHOW_USERS.md)

## Cluster management

Manages clusters, including FEs, BEs, compute nodes, brokers, resource groups, storage volumes, tables, tablets, replicas, files, indexes, variables, and plugins.

### FE, BE, CN, Broker, process

- [ADMIN SET CONFIG](./Administration/ADMIN_SET_CONFIG.md)
- [ADMIN SHOW CONFIG](./Administration/ADMIN_SHOW_CONFIG.md)
- [ALTER SYSTEM](./Administration/ALTER_SYSTEM.md)
- [CANCEL DECOMMISSION](./Administration/CANCEL_DECOMMISSION.md)
- [KILL](./Administration/KILL.md)
- [SHOW BACKENDS](./Administration/SHOW_BACKENDS.md)
- [SHOW BROKER](./Administration/SHOW_BROKER.md)
- [SHOW COMPUTE NODES](./Administration/SHOW_COMPUTE_NODES.md)
- [SHOW FRONTENDS](./Administration/SHOW_FRONTENDS.md)
- [SHOW PROC](./Administration/SHOW_PROC.md)
- [SHOW PROCESSLIST](./Administration/SHOW_PROCESSLIST.md)

### Resource group

- [CREATE RESOURCE GROUP](./Administration/CREATE_RESOURCE_GROUP.md)
- [ALTER RESOURCE GROUP](./Administration/ALTER_RESOURCE_GROUP.md)
- [DROP RESOURCE GROUP](./Administration/DROP_RESOURCE_GROUP.md)
- [SHOW RESOURCE GROUP](./Administration/SHOW_RESOURCE_GROUP.md)

### Storage volume

- [ALTER STORAGE VOLUME](./Administration/ALTER_STORAGE_VOLUME.md)
- [CREATE STORAGE VOLUME](./Administration/CREATE_STORAGE_VOLUME.md)
- [DESC STORAGE VOLUME](./Administration/DESC_STORAGE_VOLUME.md)
- [DROP STORAGE VOLUME](./Administration/DROP_STORAGE_VOLUME.md)
- [SET DEFAULT STORAGE VOLUME](./Administration/SET_DEFAULT_STORAGE_VOLUME.md)
- [SHOW STORAGE VOLUMES](./Administration/SHOW_STORAGE_VOLUMES.md)

### Table, tablet, replica

- [ADMIN CANCEL REPAIR TABLE](./Administration/ADMIN_CANCEL_REPAIR.md)
- [ADMIN CHECK TABLET](./Administration/ADMIN_CHECK_TABLET.md)
- [ADMIN REPAIR TABLE](./Administration/ADMIN_REPAIR.md)
- [ADMIN SET REPLICA STATUS](./Administration/ADMIN_SET_REPLICA_STATUS.md)
- [ADMIN SHOW REPLICA DISTRIBUTION](./Administration/ADMIN_SHOW_REPLICA_DISTRIBUTION.md)
- [ADMIN SHOW REPLICA STATUS](./Administration/ADMIN_SHOW_REPLICA_STATUS.md)
- [RECOVER](./data-definition/RECOVER.md)
- [SHOW TABLE STATUS](./Administration/SHOW_TABLE_STATUS.md)

### File, index, variable

- [CREATE FILE](./Administration/CREATE_FILE.md)
- [CREATE INDEX](./data-definition/CREATE_INDEX.md)
- [DROP FILE](./Administration/DROP_FILE.md)
- [DROP INDEX](./data-definition/DROP_INDEX.md)
- [SET (variable)](./Administration/SET.md)
- [SHOW FILE](./Administration/SHOW_FILE.md)
- [SHOW FULL COLUMNS](./Administration/SHOW_FULL_COLUMNS.md)
- [SHOW INDEX](./Administration/SHOW_INDEX.md)
- [SHOW VARIABLES](./Administration/SHOW_VARIABLES.md)

### SQL Blacklist

- [ADD SQLBLACKLIST](./Administration/ADD_SQLBLACKLIST.md)
- [SHOW SQLBLACKLIST](./Administration/SHOW_SQLBLACKLIST.md)
- [DELETE SQLBLACKLIST](./Administration/DELETE_SQLBLACKLIST.md)

### Plugin

- [INSTALL PLUGIN](./Administration/INSTALL_PLUGIN.md)
- [SHOW PLUGINS](./Administration/SHOW_PLUGINS.md)
- [UNINSTALL PLUGIN](./Administration/UNINSTALL_PLUGIN.md)

## Loading, unloading

### Routine load

- [ALTER ROUTINE LOAD](./data-manipulation/alter-routine-load.md)
- [CREATE ROUTINE LOAD](./data-manipulation/CREATE_ROUTINE_LOAD.md)
- [PAUSE ROUTINE LOAD](./data-manipulation/PAUSE_ROUTINE_LOAD.md)
- [RESUME ROUTINE LOAD](./data-manipulation/RESUME_ROUTINE_LOAD.md)
- [SHOW ROUTINE LOAD](./data-manipulation/SHOW_ROUTINE_LOAD.md)
- [SHOW ROUTINE LOAD TASK](./data-manipulation/SHOW_ROUTINE_LOAD_TASK.md)
- [STOP ROUTINE LOAD](./data-manipulation/STOP_ROUTINE_LOAD.md)

### Other load

- [ALTER LOAD](./data-manipulation/ALTER_LOAD.md)
- [BROKER LOAD](./data-manipulation/BROKER_LOAD.md)
- [CANCEL LOAD](./data-manipulation/CANCEL_LOAD.md)
- [INSERT](./data-manipulation/insert.md)
- [SHOW LOAD](./data-manipulation/SHOW_LOAD.md)
- [SHOW TRANSACTION](./data-manipulation/SHOW_TRANSACTION.md)
- [SPARK LOAD](./data-manipulation/SPARK_LOAD.md)
- [STREAM LOAD](./data-manipulation/STREAM_LOAD.md)

### Unloading

- [EXPORT](./data-manipulation/EXPORT.md)
- [CANCEL EXPORT](./data-manipulation/CANCEL_EXPORT.md)
- [SHOW EXPORT](./data-manipulation/SHOW_EXPORT.md)

### ETL task

- [DROP TASK](./data-manipulation/DROP_TASK.md)
- [SUBMIT TASK](./data-manipulation/SUBMIT_TASK.md)

## Catalog, database, resource

### Catalog

- [CREATE EXTERNAL CATALOG](./data-definition/CREATE_EXTERNAL_CATALOG.md)
- [DROP CATALOG](./data-definition/DROP_CATALOG.md)
- [SET CATALOG](./data-definition/SET_CATALOG.md)
- [SHOW CATALOGS](./data-manipulation/SHOW_CATALOGS.md)
- [SHOW CREATE CATALOG](./data-manipulation/SHOW_CREATE_CATALOG.md)

### Database

- [ALTER DATABASE](./data-definition/ALTER_DATABASE.md)
- [CREATE DATABASE](./data-definition/CREATE_DATABASE.md)
- [DROP DATABASE](./data-definition/DROP_DATABASE.md)
- [SHOW CREATE DATABASE](./data-manipulation/SHOW_CREATE_DATABASE.md)
- [SHOW DATA](./data-manipulation/SHOW_DATA.md)
- [SHOW DATABASES](./data-manipulation/SHOW_DATABASES.md)

### Resource

- [ALTER RESOURCE](./data-definition/ALTER_RESOURCE.md)
- [CREATE RESOURCE](./data-definition/CREATE_RESOURCE.md)
- [DROP RESOURCE](./data-definition/DROP_RESOURCE.md)
- [SHOW RESOURCES](./data-definition/SHOW_RESOURCES.md)

## Create table, partition

- [ALTER TABLE](./data-definition/ALTER_TABLE.md)
- [CANCEL ALTER TABLE](./data-definition/CANCEL_ALTER_TABLE.md)
- [CREATE TABLE](./data-definition/CREATE_TABLE.md)
- [CREATE TABLE AS SELECT](./data-definition/CREATE_TABLE_AS_SELECT.md)
- [CREATE TABLE LIKE](./data-definition/CREATE_TABLE_LIKE.md)
- [DROP TABLE](./data-definition/DROP_TABLE.md)
- [REFRESH EXTERNAL TABLE](./data-definition/REFRESH_EXTERNAL_TABLE.md)
- [TRUNCATE TABLE](./data-definition/TRUNCATE_TABLE.md)
- [DELETE](./data-manipulation/DELETE.md)
- [SHOW ALTER TABLE](./data-manipulation/SHOW_ALTER.md)
- [SHOW CREATE TABLE](./data-manipulation/SHOW_CREATE_TABLE.md)
- [SHOW DELETE](./data-manipulation/SHOW_DELETE.md)
- [SHOW DYNAMIC PARTITION TABLES](./data-manipulation/SHOW_DYNAMIC_PARTITION_TABLES.md)
- [SHOW PARTITIONS](./data-manipulation/SHOW_PARTITIONS.md)
- [SHOW TABLES](./data-manipulation/SHOW_TABLES.md)
- [SHOW TABLET](./data-manipulation/SHOW_TABLET.md)
- [UPDATE](./data-manipulation/UPDATE.md)

## View, materialized view

### View

- [ALTER VIEW](./data-definition/ALTER_VIEW.md)
- [CREATE VIEW](./data-definition/CREATE_VIEW.md)
- [SHOW CREATE VIEW](./data-manipulation/SHOW_CREATE_VIEW.md)
- [DROP VIEW](./data-definition/DROP_VIEW.md)

### Materialized view

- [ALTER MATERIALIZED VIEW](./data-definition/ALTER_MATERIALIZED_VIEW.md)
- [CREATE MATERIALIZED VIEW](./data-definition/CREATE_MATERIALIZED_VIEW.md)
- [DROP MATERIALIZED VIEW](./data-definition/DROP_MATERIALIZED_VIEW.md)
- [CANCEL REFRESH MATERIALIZED VIEW](./data-manipulation/CANCEL_REFRESH_MATERIALIZED_VIEW.md)
- [REFRESH MATERIALIZED VIEW](./data-manipulation/REFRESH_MATERIALIZED_VIEW.md)
- [SHOW ALTER MATERIALIZED VIEW](./data-manipulation/SHOW_ALTER_MATERIALIZED_VIEW.md)
- [SHOW CREATE MATERIALIZED VIEW](./data-manipulation/SHOW_CREATE_MATERIALIZED_VIEW.md)
- [SHOW MATERIALIZED VIEWS](./data-manipulation/SHOW_MATERIALIZED_VIEW.md)

## Function, SELECT

- [CREATE FUNCTION](./data-definition/create-function.md)
- [DROP FUNCTION](./data-definition/drop-function.md)
- [SHOW FUNCTION](./data-definition/show-functions.md)
- [SELECT](./data-manipulation/SELECT.md)

## CBO statistics

- [ANALYZE TABLE](./data-definition/ANALYZE_TABLE.md)
- [CREATE ANALYZE](./data-definition/CREATE_ANALYZE.md)
- [DROP ANALYZE](./data-definition/DROP_ANALYZE.md)
- [DROP STATS](./data-definition/DROP_STATS.md)
- [KILL ANALYZE](./data-definition/KILL_ANALYZE.md)
- [SHOW ANALYZE JOB](./data-definition/SHOW_ANALYZE_JOB.md)
- [SHOW ANALYZE STATUS](./data-definition/SHOW_ANALYZE_STATUS.md)
- [SHOW META](./data-definition/SHOW_META.md)

## Backup and restore

- [BACKUP](./data-definition/BACKUP.md)
- [CANCEL BACKUP](./data-definition/CANCEL_BACKUP.md)
- [CANCEL RESTORE](./data-definition/CANCEL_RESTORE.md)
- [CREATE REPOSITORY](./data-definition/CREATE_REPOSITORY.md)
- [DROP REPOSITORY](./data-definition/DROP_REPOSITORY.md)
- [RECOVER](./data-definition/RECOVER.md)
- [RESTORE](./data-definition/RESTORE.md)
- [SHOW BACKUP](./data-manipulation/SHOW_BACKUP.md)
- [SHOW REPOSITORIES](./data-manipulation/SHOW_REPOSITORIES.md)
- [SHOW RESTORE](./data-manipulation/SHOW_RESTORE.md)
- [SHOW SNAPSHOT](./data-manipulation/SHOW_SNAPSHOT.md)

## Utility commands

- [DESC](./Utility/DESCRIBE.md)
- [EXPLAIN](./Administration/EXPLAIN.md)
- [USE](./data-definition/USE.md)
