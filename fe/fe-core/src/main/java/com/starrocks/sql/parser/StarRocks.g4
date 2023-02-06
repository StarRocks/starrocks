// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

grammar StarRocks;
import StarRocksLex;

sqlStatements
    : singleStatement* EOF
    ;

singleStatement
    : statement (MINUS_SYMBOL MINUS_SYMBOL)? SEMICOLON? | emptyStatement
    ;

emptyStatement
    : SEMICOLON
    ;

statement
    // Query Statement
    : queryStatement

    // Warehouse Statement
    | useWarehouseStatement
    | createWarehouseStatement
    | dropWarehouseStatement
    | showWarehousesStatement
    | alterWarehouseStatement
    | showClustersStatement
    | suspendWarehouseStatement
    | resumeWarehouseStatement

    // Database Statement
    | useDatabaseStatement
    | useCatalogStatement
    | showDatabasesStatement
    | alterDbQuotaStatement
    | createDbStatement
    | dropDbStatement
    | showCreateDbStatement
    | alterDatabaseRenameStatement
    | recoverDbStmt
    | showDataStmt

    // Table Statement
    | createTableStatement
    | createTableAsSelectStatement
    | createTableLikeStatement
    | showCreateTableStatement
    | dropTableStatement
    | recoverTableStatement
    | truncateTableStatement
    | showTableStatement
    | descTableStatement
    | showTableStatusStatement
    | showColumnStatement
    | refreshTableStatement
    | alterTableStatement
    | cancelAlterTableStatement
    | showAlterStatement

    // View Statement
    | createViewStatement
    | alterViewStatement
    | dropViewStatement

    // Partition Statement
    | showPartitionsStatement
    | recoverPartitionStatement

    // Index Statement
    | createIndexStatement
    | dropIndexStatement
    | showIndexStatement

    // Task Statement
    | submitTaskStatement

    // Materialized View Statement
    | createMaterializedViewStatement
    | showMaterializedViewStatement
    | dropMaterializedViewStatement
    | alterMaterializedViewStatement
    | refreshMaterializedViewStatement
    | cancelRefreshMaterializedViewStatement

    // Catalog Statement
    | createExternalCatalogStatement
    | dropExternalCatalogStatement
    | showCatalogsStatement
    | showCreateExternalCatalogStatement

    // DML Statement
    | insertStatement
    | updateStatement
    | deleteStatement

    // Routine Statement
    | createRoutineLoadStatement
    | alterRoutineLoadStatement
    | stopRoutineLoadStatement
    | resumeRoutineLoadStatement
    | pauseRoutineLoadStatement
    | showRoutineLoadStatement
    | showRoutineLoadTaskStatement

    // StreamLoad Statement
    | showStreamLoadStatement

    // Admin Statement
    | adminSetConfigStatement
    | adminSetReplicaStatusStatement
    | adminShowConfigStatement
    | adminShowReplicaDistributionStatement
    | adminShowReplicaStatusStatement
    | adminRepairTableStatement
    | adminCancelRepairTableStatement
    | adminCheckTabletsStatement
    | killStatement
    | syncStatement

    // Cluster Management Statement
    | alterSystemStatement
    | cancelAlterSystemStatement
    | showComputeNodesStatement

    // Analyze Statement
    | analyzeStatement
    | dropStatsStatement
    | createAnalyzeStatement
    | dropAnalyzeJobStatement
    | analyzeHistogramStatement
    | dropHistogramStatement
    | showAnalyzeStatement
    | showStatsMetaStatement
    | showHistogramMetaStatement
    | killAnalyzeStatement

    // Resource Group Statement
    | createResourceGroupStatement
    | dropResourceGroupStatement
    | alterResourceGroupStatement
    | showResourceGroupStatement

    // External Resource Statement
    | createResourceStatement
    | alterResourceStatement
    | dropResourceStatement
    | showResourceStatement

    // UDF Statement
    | showFunctionsStatement
    | dropFunctionStatement
    | createFunctionStatement

    // Load Statement
    | loadStatement
    | showLoadStatement
    | showLoadWarningsStatement
    | cancelLoadStatement
    | alterLoadStatement

    // Show Statement
    | showAuthorStatement
    | showBackendsStatement
    | showBrokerStatement
    | showCharsetStatement
    | showCollationStatement
    | showDeleteStatement
    | showDynamicPartitionStatement
    | showEventsStatement
    | showEnginesStatement
    | showFrontendsStatement
    | showPluginsStatement
    | showRepositoriesStatement
    | showOpenTableStatement
    | showProcedureStatement
    | showProcStatement
    | showProcesslistStatement
    | showStatusStatement
    | showTabletStatement
    | showTransactionStatement
    | showTriggersStatement
    | showUserPropertyStatement
    | showVariablesStatement
    | showWarningStatement
    | helpStatement

    // Privilege Statement
    | createUserStatement
    | dropUserStatement
    | alterUserStatement
    | showUserStatement
    | showAuthenticationStatement
    | executeAsStatement
    | createRoleStatement
    | dropRoleStatement
    | showRolesStatement
    | grantRoleStatement
    | revokeRoleStatement
    | grantPrivilegeStatement
    | revokePrivilegeStatement
    | showGrantsStatement

    // Backup Restore Statement
    | backupStatement
    | cancelBackupStatement
    | showBackupStatement
    | restoreStatement
    | cancelRestoreStatement
    | showRestoreStatement
    | showSnapshotStatement
    | createRepositoryStatement
    | dropRepositoryStatement

    // Sql BlackList And WhiteList Statement
    | addSqlBlackListStatement
    | delSqlBlackListStatement
    | showSqlBlackListStatement
    | showWhiteListStatement

    // Export Statement
    | exportStatement
    | cancelExportStatement
    | showExportStatement

    // Plugin Statement
    | installPluginStatement
    | uninstallPluginStatement

    // File Statement
    | createFileStatement
    | dropFileStatement
    | showSmallFilesStatement

    // Set Statement
    | setStatement
    | setUserPropertyStatement
    | setRoleStatement

    //Unsupported Statement
    | unsupportedStatement
    ;

// ---------------------------------------- DataBase Statement ---------------------------------------------------------

useDatabaseStatement
    : USE qualifiedName
    ;

useCatalogStatement
    : USE CATALOG identifierOrString
    ;

showDatabasesStatement
    : SHOW DATABASES ((FROM | IN) catalog=qualifiedName)? ((LIKE pattern=string) | (WHERE expression))?
    | SHOW SCHEMAS ((LIKE pattern=string) | (WHERE expression))?
    ;

alterDbQuotaStatement
    : ALTER DATABASE identifier SET DATA QUOTA identifier
    | ALTER DATABASE identifier SET REPLICA QUOTA INTEGER_VALUE
    ;

createDbStatement
    : CREATE (DATABASE | SCHEMA) (IF NOT EXISTS)? identifier
    ;

dropDbStatement
    : DROP (DATABASE | SCHEMA) (IF EXISTS)? identifier FORCE?
    ;

showCreateDbStatement
    : SHOW CREATE (DATABASE | SCHEMA) identifier
    ;

alterDatabaseRenameStatement
    : ALTER DATABASE identifier RENAME identifier
    ;

recoverDbStmt
    : RECOVER (DATABASE | SCHEMA) identifier
    ;

showDataStmt
    : SHOW DATA
    | SHOW DATA FROM qualifiedName
    ;

// ------------------------------------------- Table Statement ---------------------------------------------------------

createTableStatement
    : CREATE EXTERNAL? TABLE (IF NOT EXISTS)? qualifiedName
          '(' columnDesc (',' columnDesc)* (',' indexDesc)* ')'
          engineDesc?
          charsetDesc?
          keyDesc?
          comment?
          partitionDesc?
          distributionDesc?
          orderByDesc?
          rollupDesc?
          properties?
          extProperties?
     ;

columnDesc
    : identifier type charsetName? KEY? aggDesc? (NULL | NOT NULL)? defaultDesc? comment?
    ;

charsetName
    : CHAR SET identifier
    | CHARSET identifier
    ;

defaultDesc
    : DEFAULT (string | NULL | CURRENT_TIMESTAMP | '(' qualifiedName '(' ')' ')')
    ;

indexDesc
    : INDEX indexName=identifier identifierList indexType? comment?
    ;

engineDesc
    : ENGINE EQ identifier
    ;

charsetDesc
    : DEFAULT? CHARSET EQ? identifierOrString
    ;


keyDesc
    : (AGGREGATE | UNIQUE | PRIMARY | DUPLICATE) KEY identifierList
    ;

orderByDesc
    : ORDER BY identifierList
    ;

aggDesc
    : SUM
    | MAX
    | MIN
    | REPLACE
    | HLL_UNION
    | BITMAP_UNION
    | PERCENTILE_UNION
    | REPLACE_IF_NOT_NULL
    ;

rollupDesc
    : ROLLUP '(' rollupItem (',' rollupItem)* ')'
    ;

rollupItem
    : rollupName=identifier identifierList (dupKeys)? (fromRollup)? properties?
    ;

dupKeys
    : DUPLICATE KEY identifierList
    ;

fromRollup
    : FROM identifier
    ;

createTableAsSelectStatement
    : CREATE TABLE (IF NOT EXISTS)? qualifiedName
        ('(' identifier (',' identifier)* ')')?
        keyDesc?
        comment?
        partitionDesc?
        distributionDesc?
        properties?
        AS queryStatement
        ;

dropTableStatement
    : DROP TABLE (IF EXISTS)? qualifiedName FORCE?
    ;

alterTableStatement
    : ALTER TABLE qualifiedName alterClause (',' alterClause)*
    | ALTER TABLE qualifiedName ADD ROLLUP rollupItem (',' rollupItem)*
    | ALTER TABLE qualifiedName DROP ROLLUP identifier (',' identifier)*
    ;

createIndexStatement
    : CREATE INDEX indexName=identifier
        ON qualifiedName identifierList indexType?
        comment?
    ;

dropIndexStatement
    : DROP INDEX indexName=identifier ON qualifiedName
    ;

indexType
    : USING BITMAP
    ;

showTableStatement
    : SHOW FULL? TABLES ((FROM | IN) db=qualifiedName)? ((LIKE pattern=string) | (WHERE expression))?
    ;

showCreateTableStatement
    : SHOW CREATE (TABLE | VIEW | MATERIALIZED VIEW) table=qualifiedName
    ;

showColumnStatement
    : SHOW FULL? (COLUMNS | FIELDS) ((FROM | IN) table=qualifiedName) ((FROM | IN) db=qualifiedName)?
        ((LIKE pattern=string) | (WHERE expression))?
    ;

showTableStatusStatement
    : SHOW TABLE STATUS ((FROM | IN) db=qualifiedName)? ((LIKE pattern=string) | (WHERE expression))?
    ;

refreshTableStatement
    : REFRESH EXTERNAL TABLE qualifiedName (PARTITION '(' string (',' string)* ')')?
    ;

showAlterStatement
    : SHOW ALTER TABLE (COLUMN | ROLLUP) ((FROM | IN) db=qualifiedName)?
        (WHERE expression)? (ORDER BY sortItem (',' sortItem)*)? (limitElement)?
    | SHOW ALTER MATERIALIZED VIEW ((FROM | IN) db=qualifiedName)?
              (WHERE expression)? (ORDER BY sortItem (',' sortItem)*)? (limitElement)?
    ;

descTableStatement
    : (DESC | DESCRIBE) table=qualifiedName ALL?
    ;

createTableLikeStatement
    : CREATE (EXTERNAL)? TABLE (IF NOT EXISTS)? qualifiedName LIKE qualifiedName
    ;

showIndexStatement
    : SHOW (INDEX | INDEXES | KEY | KEYS) ((FROM | IN) table=qualifiedName) ((FROM | IN) db=qualifiedName)?
    ;

recoverTableStatement
    : RECOVER TABLE qualifiedName
    ;

truncateTableStatement
    : TRUNCATE TABLE qualifiedName partitionNames?
    ;

cancelAlterTableStatement
    : CANCEL ALTER TABLE (COLUMN | ROLLUP)? FROM qualifiedName ('(' INTEGER_VALUE (',' INTEGER_VALUE)* ')')?
    | CANCEL ALTER MATERIALIZED VIEW FROM qualifiedName
    ;

showPartitionsStatement
    : SHOW TEMPORARY? PARTITIONS FROM table=qualifiedName
    (WHERE expression)?
    (ORDER BY sortItem (',' sortItem)*)? limitElement?
    ;

recoverPartitionStatement
    : RECOVER PARTITION identifier FROM table=qualifiedName
    ;

// ------------------------------------------- View Statement ----------------------------------------------------------

createViewStatement
    : CREATE VIEW (IF NOT EXISTS)? qualifiedName
        ('(' columnNameWithComment (',' columnNameWithComment)* ')')?
        comment? AS queryStatement
    ;

alterViewStatement
    : ALTER VIEW qualifiedName
    ('(' columnNameWithComment (',' columnNameWithComment)* ')')?
    AS queryStatement
    ;

dropViewStatement
    : DROP VIEW (IF EXISTS)? qualifiedName
    ;

// ------------------------------------------- Task Statement ----------------------------------------------------------

submitTaskStatement
    : SUBMIT setVarHint* TASK qualifiedName?
    AS createTableAsSelectStatement
    ;

// ------------------------------------------- Materialized View Statement ---------------------------------------------

createMaterializedViewStatement
    : CREATE MATERIALIZED VIEW (IF NOT EXISTS)? mvName=qualifiedName
    comment?
    materializedViewDesc*
    AS queryStatement
    ;

materializedViewDesc
    : (PARTITION BY primaryExpression)
    | distributionDesc
    | refreshSchemeDesc
    | properties
    ;

showMaterializedViewStatement
    : SHOW MATERIALIZED VIEW ((FROM | IN) db=qualifiedName)? ((LIKE pattern=string) | (WHERE expression))?
    ;

dropMaterializedViewStatement
    : DROP MATERIALIZED VIEW (IF EXISTS)? mvName=qualifiedName
    ;

alterMaterializedViewStatement
    : ALTER MATERIALIZED VIEW mvName=qualifiedName (refreshSchemeDesc | tableRenameClause | modifyTablePropertiesClause)
    ;

refreshMaterializedViewStatement
    : REFRESH MATERIALIZED VIEW mvName=qualifiedName (PARTITION partitionRangeDesc)? FORCE?
    ;

cancelRefreshMaterializedViewStatement
    : CANCEL REFRESH MATERIALIZED VIEW mvName=qualifiedName
    ;

// ------------------------------------------- Admin Statement ---------------------------------------------------------

adminSetConfigStatement
    : ADMIN SET FRONTEND CONFIG '(' property ')'
    ;
adminSetReplicaStatusStatement
    : ADMIN SET REPLICA STATUS properties
    ;
adminShowConfigStatement
    : ADMIN SHOW FRONTEND CONFIG (LIKE pattern=string)?
    ;

adminShowReplicaDistributionStatement
    : ADMIN SHOW REPLICA DISTRIBUTION FROM qualifiedName partitionNames?
    ;

adminShowReplicaStatusStatement
    : ADMIN SHOW REPLICA STATUS FROM qualifiedName partitionNames? (WHERE where=expression)?
    ;

adminRepairTableStatement
    : ADMIN REPAIR TABLE qualifiedName partitionNames?
    ;

adminCancelRepairTableStatement
    : ADMIN CANCEL REPAIR TABLE qualifiedName partitionNames?
    ;

adminCheckTabletsStatement
    : ADMIN CHECK tabletList properties
    ;

killStatement
    : KILL (CONNECTION? | QUERY) INTEGER_VALUE
    ;

syncStatement
    : SYNC
    ;

// ------------------------------------------- Cluster Management Statement ---------------------------------------------

alterSystemStatement
    : ALTER SYSTEM alterClause
    ;

cancelAlterSystemStatement
    : CANCEL DECOMMISSION BACKEND string (',' string)*
    ;

showComputeNodesStatement
    : SHOW COMPUTE NODES
    ;

// ------------------------------------------- Catalog Statement -------------------------------------------------------

createExternalCatalogStatement
    : CREATE EXTERNAL CATALOG catalogName=identifierOrString comment? properties
    ;

showCreateExternalCatalogStatement
    : SHOW CREATE CATALOG catalogName=identifierOrString
    ;

dropExternalCatalogStatement
    : DROP CATALOG catalogName=identifierOrString
    ;

showCatalogsStatement
    : SHOW CATALOGS
    ;


// ---------------------------------------- Warehouse Statement ---------------------------------------------------------

createWarehouseStatement
    : CREATE (WAREHOUSE) (IF NOT EXISTS)? warehouseName=identifierOrString
    properties?
    ;

showWarehousesStatement
    : SHOW WAREHOUSES ((LIKE pattern=string) | (WHERE expression))?
    ;

useWarehouseStatement
    : USE WAREHOUSE identifierOrString
    ;

dropWarehouseStatement
    : DROP WAREHOUSE (IF EXISTS)? warehouseName=identifierOrString
    ;

alterWarehouseStatement
    : ALTER WAREHOUSE identifier ADD CLUSTER
    | ALTER WAREHOUSE identifier REMOVE CLUSTER
    | ALTER WAREHOUSE identifier SET propertyList
    ;

showClustersStatement
    : SHOW CLUSTERS FROM WAREHOUSE identifier
    ;

suspendWarehouseStatement
    : SUSPEND WAREHOUSE (IF EXISTS)? identifier
    ;

resumeWarehouseStatement
    : RESUME WAREHOUSE (IF EXISTS)? identifier
    ;

// ------------------------------------------- Alter Clause ------------------------------------------------------------

alterClause
    //Alter system clause
    : addFrontendClause
    | dropFrontendClause
    | modifyFrontendHostClause
    | addBackendClause
    | dropBackendClause
    | decommissionBackendClause
    | modifyBackendHostClause
    | addComputeNodeClause
    | dropComputeNodeClause
    | modifyBrokerClause
    | alterLoadErrorUrlClause

    //Alter table clause
    | createIndexClause
    | dropIndexClause
    | tableRenameClause
    | swapTableClause
    | modifyTablePropertiesClause
    | addColumnClause
    | addColumnsClause
    | dropColumnClause
    | modifyColumnClause
    | columnRenameClause
    | reorderColumnsClause
    | rollupRenameClause

    //Alter partition clause
    | addPartitionClause
    | dropPartitionClause
    | distributionClause
    | truncatePartitionClause
    | modifyPartitionClause
    | replacePartitionClause
    | partitionRenameClause
    ;

// ---------Alter system clause---------

addFrontendClause
   : ADD (FOLLOWER | OBSERVER) string
   ;

dropFrontendClause
   : DROP (FOLLOWER | OBSERVER) string
   ;

modifyFrontendHostClause
  : MODIFY FRONTEND HOST string TO string
  ;

addBackendClause
   : ADD BACKEND string (',' string)*
   ;

dropBackendClause
   : DROP BACKEND string (',' string)* FORCE?
   ;

decommissionBackendClause
   : DECOMMISSION BACKEND string (',' string)*
   ;

modifyBackendHostClause
   : MODIFY BACKEND HOST string TO string
   ;

addComputeNodeClause
   : ADD COMPUTE NODE string (',' string)*
   ;

dropComputeNodeClause
   : DROP COMPUTE NODE string (',' string)*
   ;

modifyBrokerClause
    : ADD BROKER identifierOrString string (',' string)*
    | DROP BROKER identifierOrString string (',' string)*
    | DROP ALL BROKER identifierOrString
    ;

alterLoadErrorUrlClause
    : SET LOAD ERRORS HUB properties?
    ;

// ---------Alter table clause---------

createIndexClause
    : ADD INDEX indexName=identifier identifierList indexType? comment?
    ;

dropIndexClause
    : DROP INDEX indexName=identifier
    ;

tableRenameClause
    : RENAME identifier
    ;

swapTableClause
    : SWAP WITH identifier
    ;

modifyTablePropertiesClause
    : SET propertyList
    ;

addColumnClause
    : ADD COLUMN columnDesc (FIRST | AFTER identifier)? ((TO | IN) rollupName=identifier)? properties?
    ;

addColumnsClause
    : ADD COLUMN '(' columnDesc (',' columnDesc)* ')' ((TO | IN) rollupName=identifier)? properties?
    ;

dropColumnClause
    : DROP COLUMN identifier (FROM rollupName=identifier)? properties?
    ;

modifyColumnClause
    : MODIFY COLUMN columnDesc (FIRST | AFTER identifier)? (FROM rollupName=identifier)? properties?
    ;

columnRenameClause
    : RENAME COLUMN oldColumn=identifier newColumn=identifier
    ;

reorderColumnsClause
    : ORDER BY identifierList (FROM rollupName=identifier)? properties?
    ;

rollupRenameClause
    : RENAME ROLLUP rollupName=identifier newRollupName=identifier
    ;

// ---------Alter partition clause---------

addPartitionClause
    : ADD TEMPORARY? (singleRangePartition | PARTITIONS multiRangePartition) distributionDesc? properties?
    | ADD TEMPORARY? (singleItemListPartitionDesc | multiItemListPartitionDesc) distributionDesc? properties?
    ;

dropPartitionClause
    : DROP TEMPORARY? PARTITION (IF EXISTS)? identifier FORCE?
    ;

truncatePartitionClause
    : TRUNCATE partitionNames
    ;

modifyPartitionClause
    : MODIFY PARTITION (identifier | identifierList | '(' ASTERISK_SYMBOL ')') SET propertyList
    | MODIFY PARTITION distributionDesc
    ;

replacePartitionClause
    : REPLACE parName=partitionNames WITH tempParName=partitionNames properties?
    ;

partitionRenameClause
    : RENAME PARTITION parName=identifier newParName=identifier
    ;

// ------------------------------------------- DML Statement -----------------------------------------------------------

insertStatement
    : explainDesc? INSERT (INTO | OVERWRITE) qualifiedName partitionNames?
        (WITH LABEL label=identifier)? columnAliases?
        (queryStatement | (VALUES expressionsWithDefault (',' expressionsWithDefault)*))
    ;

updateStatement
    : explainDesc? withClause? UPDATE qualifiedName SET assignmentList fromClause (WHERE where=expression)?
    ;

deleteStatement
    : explainDesc? withClause? DELETE FROM qualifiedName partitionNames? (USING using=relations)? (WHERE where=expression)?
    ;

// ------------------------------------------- Routine Statement -----------------------------------------------------------
createRoutineLoadStatement
    : CREATE ROUTINE LOAD (db=qualifiedName '.')? name=identifier ON table=qualifiedName
        (loadProperties (',' loadProperties)*)?
        jobProperties?
        FROM source=identifier
        dataSourceProperties?
    ;

alterRoutineLoadStatement
    : ALTER ROUTINE LOAD FOR (db=qualifiedName '.')? name=identifier
        (loadProperties (',' loadProperties)*)?
        jobProperties?
        dataSource?
    ;

dataSource
    : FROM source=identifier dataSourceProperties
    ;

loadProperties
    : colSeparatorProperty
    | rowDelimiterProperty
    | importColumns
    | WHERE expression
    | partitionNames
    ;

colSeparatorProperty
    : COLUMNS TERMINATED BY string
    ;

rowDelimiterProperty
    : ROWS TERMINATED BY string
    ;

importColumns
    : COLUMNS columnProperties
    ;

columnProperties
    : '('
        (qualifiedName | assignment) (',' (qualifiedName | assignment))*
      ')'
    ;

jobProperties
    : properties
    ;

dataSourceProperties
    : propertyList
    ;

stopRoutineLoadStatement
    : STOP ROUTINE LOAD FOR (db=qualifiedName '.')? name=identifier
    ;

resumeRoutineLoadStatement
    : RESUME ROUTINE LOAD FOR (db=qualifiedName '.')? name=identifier
    ;

pauseRoutineLoadStatement
    : PAUSE ROUTINE LOAD FOR (db=qualifiedName '.')? name=identifier
    ;

showRoutineLoadStatement
    : SHOW ALL? ROUTINE LOAD (FOR (db=qualifiedName '.')? name=identifier)?
        (FROM db=qualifiedName)?
        (WHERE expression)? (ORDER BY sortItem (',' sortItem)*)? (limitElement)?
    ;

showRoutineLoadTaskStatement
    : SHOW ROUTINE LOAD TASK
        (FROM db=qualifiedName)?
        WHERE expression
    ;

showStreamLoadStatement
    : SHOW ALL? STREAM LOAD (FOR (db=qualifiedName '.')? name=identifier)?
        (FROM db=qualifiedName)?
        (WHERE expression)? (ORDER BY sortItem (',' sortItem)*)? (limitElement)?
    ;
// ------------------------------------------- Analyze Statement -------------------------------------------------------

analyzeStatement
    : ANALYZE (FULL | SAMPLE)? TABLE qualifiedName ('(' identifier (',' identifier)* ')')?
        (WITH (SYNC | ASYNC) MODE)?
        properties?
    ;

dropStatsStatement
    : DROP STATS qualifiedName
    ;

analyzeHistogramStatement
    : ANALYZE TABLE qualifiedName UPDATE HISTOGRAM ON identifier (',' identifier)*
        (WITH (SYNC | ASYNC) MODE)?
        (WITH bucket=INTEGER_VALUE BUCKETS)?
        properties?
    ;

dropHistogramStatement
    : ANALYZE TABLE qualifiedName DROP HISTOGRAM ON identifier (',' identifier)*
    ;

createAnalyzeStatement
    : CREATE ANALYZE (FULL | SAMPLE)? ALL properties?
    | CREATE ANALYZE (FULL | SAMPLE)? DATABASE db=identifier properties?
    | CREATE ANALYZE (FULL | SAMPLE)? TABLE qualifiedName ('(' identifier (',' identifier)* ')')? properties?
    ;

dropAnalyzeJobStatement
    : DROP ANALYZE INTEGER_VALUE
    ;

showAnalyzeStatement
    : SHOW ANALYZE (JOB | STATUS)? (WHERE expression)?
    ;

showStatsMetaStatement
    : SHOW STATS META (WHERE expression)?
    ;

showHistogramMetaStatement
    : SHOW HISTOGRAM META (WHERE expression)?
    ;

killAnalyzeStatement
    : KILL ANALYZE INTEGER_VALUE
    ;

// ------------------------------------------- Work Group Statement ----------------------------------------------------

createResourceGroupStatement
    : CREATE RESOURCE GROUP (IF NOT EXISTS)? (OR REPLACE)? identifier
        TO classifier (',' classifier)*  WITH '(' property (',' property)* ')'
    ;

dropResourceGroupStatement
    : DROP RESOURCE GROUP identifier
    ;

alterResourceGroupStatement
    : ALTER RESOURCE GROUP identifier ADD classifier (',' classifier)*
    | ALTER RESOURCE GROUP identifier DROP '(' INTEGER_VALUE (',' INTEGER_VALUE)* ')'
    | ALTER RESOURCE GROUP identifier DROP ALL
    | ALTER RESOURCE GROUP identifier WITH '(' property (',' property)* ')'
    ;

showResourceGroupStatement
    : SHOW RESOURCE GROUP identifier
    | SHOW RESOURCE GROUPS ALL?
    ;

createResourceStatement
    : CREATE EXTERNAL? RESOURCE resourceName=identifierOrString properties?
    ;

alterResourceStatement
    : ALTER RESOURCE resourceName=identifierOrString SET properties
    ;

dropResourceStatement
    : DROP RESOURCE resourceName=identifierOrString
    ;

showResourceStatement
    : SHOW RESOURCES
    ;

classifier
    : '(' expressionList ')'
    ;

// ------------------------------------------- Function ----------------------------------------------------

showFunctionsStatement
    : SHOW FULL? (BUILTIN|GLOBAL)? FUNCTIONS ((FROM | IN) db=qualifiedName)? ((LIKE pattern=string) | (WHERE expression))?
    ;

dropFunctionStatement
    : DROP GLOBAL? FUNCTION qualifiedName '(' typeList ')'
    ;

createFunctionStatement
    : CREATE GLOBAL? functionType=(TABLE | AGGREGATE)? FUNCTION qualifiedName '(' typeList ')' RETURNS returnType=type (INTERMEDIATE intermediateType =  type)? properties?
    ;

typeList
    : type?  ( ',' type)* (',' DOTDOTDOT) ?
    ;

// ------------------------------------------- Load Statement ----------------------------------------------------------

loadStatement
    : LOAD LABEL label=labelName
        data=dataDescList?
        broker=brokerDesc?
        (BY system=identifierOrString)?
        (PROPERTIES props=propertyList)?
    | LOAD LABEL label=labelName
        data=dataDescList?
        resource=resourceDesc
        (PROPERTIES props=propertyList)?
    ;

labelName
    : (db=identifier '.')? label=identifier
    ;

dataDescList
    : '(' dataDesc (',' dataDesc)* ')'
    ;

dataDesc
    : DATA INFILE srcFiles=stringList
        NEGATIVE?
        INTO TABLE dstTableName=identifier
        partitions=partitionNames?
        (COLUMNS TERMINATED BY colSep=string)?
        format=fileFormat?
        colList=columnAliases?
        (COLUMNS FROM PATH AS colFromPath=identifierList)?
        (SET colMappingList=classifier)?
        (WHERE where=expression)?
    | DATA FROM TABLE srcTableName=identifier
        NEGATIVE?
        INTO TABLE dstTableName=identifier
        partitions=partitionNames?
        (SET colMappingList=classifier)?
        (WHERE where=expression)?
    ;

brokerDesc
    : WITH BROKER props=propertyList?
    | WITH BROKER name=identifierOrString props=propertyList?
    ;

resourceDesc
    : WITH RESOURCE name=identifierOrString props=propertyList?
    ;

showLoadStatement
    : SHOW LOAD (FROM identifier)? (WHERE expression)? (ORDER BY sortItem (',' sortItem)*)? limitElement?
    ;

showLoadWarningsStatement
    : SHOW LOAD WARNINGS (FROM identifier)? (WHERE expression)? limitElement?
    | SHOW LOAD WARNINGS ON string
    ;

cancelLoadStatement
    : CANCEL LOAD (FROM identifier)? (WHERE expression)?
    ;

alterLoadStatement
    : ALTER LOAD FOR (db=qualifiedName '.')? name=identifier
        jobProperties?
    ;

// ------------------------------------------- Show Statement ----------------------------------------------------------

showAuthorStatement
    : SHOW AUTHORS
    ;

showBackendsStatement
    : SHOW BACKENDS
    ;

showBrokerStatement
    : SHOW BROKER
    ;

showCharsetStatement
    : SHOW (CHAR SET | CHARSET) ((LIKE pattern=string) | (WHERE expression))?
    ;

showCollationStatement
    : SHOW COLLATION ((LIKE pattern=string) | (WHERE expression))?
    ;

showDeleteStatement
    : SHOW DELETE ((FROM | IN) db=qualifiedName)?
    ;

showDynamicPartitionStatement
    : SHOW DYNAMIC PARTITION TABLES ((FROM | IN) db=qualifiedName)?
    ;

showEventsStatement
    : SHOW EVENTS ((FROM | IN) catalog=qualifiedName)? ((LIKE pattern=string) | (WHERE expression))?
    ;

showEnginesStatement
    : SHOW ENGINES
    ;

showFrontendsStatement
    : SHOW FRONTENDS
    ;

showPluginsStatement
    : SHOW PLUGINS
    ;

showRepositoriesStatement
    : SHOW REPOSITORIES
    ;

showOpenTableStatement
    : SHOW OPEN TABLES
    ;

showProcedureStatement
    : SHOW PROCEDURE STATUS ((LIKE pattern=string) | (WHERE where=expression))?
    ;

showProcStatement
    : SHOW PROC path=string
    ;

showProcesslistStatement
    : SHOW FULL? PROCESSLIST
    ;

showStatusStatement
    : SHOW varType? STATUS ((LIKE pattern=string) | (WHERE expression))?
    ;

showTabletStatement
    : SHOW TABLET INTEGER_VALUE
    | SHOW TABLET FROM qualifiedName partitionNames? (WHERE expression)? (ORDER BY sortItem (',' sortItem)*)? (limitElement)?
    ;

showTransactionStatement
    : SHOW TRANSACTION ((FROM | IN) db=qualifiedName)? (WHERE expression)?
    ;

showTriggersStatement
    : SHOW FULL? TRIGGERS ((FROM | IN) catalog=qualifiedName)? ((LIKE pattern=string) | (WHERE expression))?
    ;

showUserPropertyStatement
    : SHOW PROPERTY (FOR string)? (LIKE string)?
    ;

showVariablesStatement
    : SHOW varType? VARIABLES ((LIKE pattern=string) | (WHERE expression))?
    ;

showWarningStatement
    : SHOW (WARNINGS | ERRORS) (limitElement)?
    ;

helpStatement
    : HELP identifierOrString
    ;

// ------------------------------------------- Privilege Statement -----------------------------------------------------

// [deprecated] grant select on *
// [deprecated] grant select on *.*
// grant select on db1.tbl1,db2.tbl2
// grant select on db1,db2
tableDbPrivilegeObjectNameList
    : identifierOrStringOrStar                                  #deprecatedDbPrivilegeObject
    | identifierOrStringOrStar '.' identifierOrStringOrStar     #deprecatedTablePrivilegeObject
    | tablePrivilegeObjectNameList                              #tablePrivilegeObjectList
    | identifierOrStringList                                    #defaultPrivilegeObjectList
    ;

userList
    : user (',' user)*
    ;

tablePrivilegeObjectNameList
    : tablePrivilegeObjectName (',' tablePrivilegeObjectName)*
    ;

tablePrivilegeObjectName
    : identifierOrString '.' identifierOrString
    ;

// the last one is deprecated
privilegeObjectNameList
    : tablePrivilegeObjectNameList
    | identifierOrStringList
    | userList
    | ASTERISK_SYMBOL
    ;

identifierOrStringOrStar
    : ASTERISK_SYMBOL
    | identifier
    | string
    ;

privilegeActionReserved
    : ADMIN
    | ALTER
    | CREATE
    | DROP
    | GRANT
    | LOAD
    | SELECT
    | INSERT
    | DELETE
    | USAGE
    | CREATE_DATABASE
    | UPDATE
    | EXPORT
    | REPOSITORY
    | CREATE_MATERIALIZED_VIEW
    | ALL
    ;

privilegeActionList
    :  privilegeAction (',' privilegeAction)*
    ;

privilegeAction
    : privilegeActionReserved
    | identifier
    ;

privilegeTypeReserved
    : SYSTEM
    | TABLE
    | DATABASE
    | CATALOG
    | DATABASES
    | FUNCTION
    | RESOURCE_GROUP
    ;

privilegeType
    : privilegeTypeReserved
    | identifier
    ;

grantRevokeClause
    : (user | ROLE identifierOrString ) (WITH GRANT OPTION)?
    ;


grantPrivilegeStatement
    : GRANT IMPERSONATE ON user TO grantRevokeClause                                                     #grantImpersonateBrief
    | GRANT privilegeActionList ON tableDbPrivilegeObjectNameList TO grantRevokeClause                   #grantTablePrivBrief
    | GRANT privilegeActionList ON privilegeType (privilegeObjectNameList)? TO grantRevokeClause         #grantPrivWithType
    | GRANT privilegeActionList ON GLOBAL? privilegeType qualifiedName '(' typeList ')' TO grantRevokeClause     #grantPrivWithFunc
    | GRANT privilegeActionList ON ALL privilegeType (IN ALL privilegeType)* (IN privilegeType identifierOrString)? TO grantRevokeClause   #grantOnAll
    | GRANT privilegeActionList ON ALL GLOBAL FUNCTIONS TO grantRevokeClause   #grantOnAllGlobalFunctions
    ;

revokePrivilegeStatement
    : REVOKE IMPERSONATE ON user FROM grantRevokeClause                                                  #revokeImpersonateBrief
    | REVOKE privilegeActionList ON tableDbPrivilegeObjectNameList FROM grantRevokeClause                #revokeTablePrivBrief
    | REVOKE privilegeActionList ON privilegeType (privilegeObjectNameList)? FROM grantRevokeClause      #revokePrivWithType
    | REVOKE privilegeActionList ON GLOBAL? privilegeType qualifiedName '(' typeList ')' FROM grantRevokeClause  #revokePrivWithFunc
    | REVOKE privilegeActionList ON ALL privilegeType (IN ALL privilegeType)* (IN privilegeType identifierOrString)? FROM grantRevokeClause  #revokeOnAll
    | REVOKE privilegeActionList ON ALL GLOBAL FUNCTIONS FROM grantRevokeClause   #revokeOnAllGlobalFunctions
    ;

grantRoleStatement
    : GRANT identifierOrStringList TO USER? user                                                        #grantRoleToUser
    | GRANT identifierOrStringList TO ROLE identifierOrString                                           #grantRoleToRole
    ;

revokeRoleStatement
    : REVOKE identifierOrStringList FROM USER? user                                                     #revokeRoleFromUser
    | REVOKE identifierOrStringList FROM ROLE identifierOrString                                        #revokeRoleFromRole
    ;

executeAsStatement
    : EXECUTE AS user (WITH NO REVERT)?
    ;

createUserStatement
    : CREATE USER (IF NOT EXISTS)? user authOption? (DEFAULT ROLE string)?
    ;

dropUserStatement
    : DROP USER user
    ;

alterUserStatement
    : ALTER USER user authOption
    ;

showUserStatement
    : SHOW (USER | USERS)
    ;

showAuthenticationStatement
    : SHOW ALL AUTHENTICATION                                                                #showAllAuthentication
    | SHOW AUTHENTICATION (FOR user)?                                                        #showAuthenticationForUser
    ;

createRoleStatement
    : CREATE ROLE identifierOrString
    ;

dropRoleStatement
    : DROP ROLE identifierOrString
    ;

showRolesStatement
    : SHOW ROLES
    ;

showGrantsStatement
    : SHOW GRANTS
    | SHOW GRANTS FOR USER? user
    | SHOW GRANTS FOR ROLE identifierOrString
    ;

// ---------------------------------------- Backup Restore Statement ---------------------------------------------------

backupStatement
    : BACKUP SNAPSHOT qualifiedName
    TO identifier
    (ON '(' tableDesc (',' tableDesc) * ')')?
    (PROPERTIES propertyList)?
    ;

cancelBackupStatement
    : CANCEL BACKUP ((FROM | IN) identifier)?
    ;

showBackupStatement
    : SHOW BACKUP ((FROM | IN) identifier)?
    ;

restoreStatement
    : RESTORE SNAPSHOT qualifiedName
    FROM identifier
    (ON '(' restoreTableDesc (',' restoreTableDesc) * ')')?
    (PROPERTIES propertyList)?
    ;

cancelRestoreStatement
    : CANCEL RESTORE ((FROM | IN) identifier)?
    ;

showRestoreStatement
    : SHOW RESTORE ((FROM | IN) identifier)? (WHERE where=expression)?
    ;

showSnapshotStatement
    : SHOW SNAPSHOT ON identifier
    (WHERE expression)?
    ;

createRepositoryStatement
    : CREATE (READ ONLY)? REPOSITORY identifier
    WITH BROKER identifier?
    ON LOCATION string
    PROPERTIES propertyList
    ;

dropRepositoryStatement
    : DROP REPOSITORY identifier
    ;

// ------------------------------------ Sql BlackList And WhiteList Statement ------------------------------------------

addSqlBlackListStatement
    : ADD SQLBLACKLIST string
    ;

delSqlBlackListStatement
    : DELETE SQLBLACKLIST INTEGER_VALUE (',' INTEGER_VALUE)*
    ;

showSqlBlackListStatement
    : SHOW SQLBLACKLIST
    ;

showWhiteListStatement
    : SHOW WHITELIST
    ;

// ------------------------------------------- Export Statement --------------------------------------------------------

exportStatement
    : EXPORT TABLE tableDesc columnAliases? TO string properties? brokerDesc?
    ;

cancelExportStatement
    : CANCEL EXPORT ((FROM | IN) catalog=qualifiedName)? ((LIKE pattern=string) | (WHERE expression))?
    ;

showExportStatement
    : SHOW EXPORT ((FROM | IN) catalog=qualifiedName)?
        ((LIKE pattern=string) | (WHERE expression))?
        (ORDER BY sortItem (',' sortItem)*)? (limitElement)?
    ;

// ------------------------------------------- Plugin Statement --------------------------------------------------------

installPluginStatement
    : INSTALL PLUGIN FROM identifierOrString properties?
    ;

uninstallPluginStatement
    : UNINSTALL PLUGIN identifierOrString
    ;

// ------------------------------------------- File Statement ----------------------------------------------------------

createFileStatement
    : CREATE FILE string ((FROM | IN) catalog=qualifiedName)? properties
    ;

dropFileStatement
    : DROP FILE string ((FROM | IN) catalog=qualifiedName)? properties
    ;

showSmallFilesStatement
    : SHOW FILE ((FROM | IN) catalog=qualifiedName)?
    ;

// ------------------------------------------- Set Statement -----------------------------------------------------------

setStatement
    : SET setVar (',' setVar)*
    ;

setVar
    : (CHAR SET | CHARSET) (identifierOrString | DEFAULT)                                       #setNames
    | NAMES (charset = identifierOrString | DEFAULT)
        (COLLATE (collate = identifierOrString | DEFAULT))?                                     #setNames
    | PASSWORD '=' (string | PASSWORD '(' string ')')                                           #setPassword
    | PASSWORD FOR user '=' (string | PASSWORD '(' string ')')                                  #setPassword
    | varType? identifier '=' setExprOrDefault                                                  #setVariable
    | userVariable '=' expression                                                               #setVariable
    | systemVariable '=' setExprOrDefault                                                       #setVariable
    | varType? TRANSACTION transaction_characteristics                                          #setTransaction
    ;

transaction_characteristics
    : transaction_access_mode
    | isolation_level
    | transaction_access_mode ',' isolation_level
    | isolation_level ',' transaction_access_mode
    ;

transaction_access_mode
    : READ ONLY
    | READ WRITE
    ;

isolation_level
    : ISOLATION LEVEL isolation_types
    ;

isolation_types
    : READ UNCOMMITTED
    | READ COMMITTED
    | REPEATABLE READ
    | SERIALIZABLE
    ;

setExprOrDefault
    : DEFAULT
    | ON
    | ALL
    | expression
    ;

setUserPropertyStatement
    : SET PROPERTY (FOR string)? userPropertyList
    ;

roleList
    : string (',' string)*
    ;

setRoleStatement
    : SET ROLE roleList                #setRole
    | SET ROLE ALL (EXCEPT roleList)?  #setRoleAll
    ;

unsupportedStatement
    : START TRANSACTION (WITH CONSISTENT SNAPSHOT)?
    | BEGIN WORK?
    | COMMIT WORK? (AND NO? CHAIN)? (NO? RELEASE)?
    | ROLLBACK WORK? (AND NO? CHAIN)? (NO? RELEASE)?
    ;

// ------------------------------------------- Query Statement ---------------------------------------------------------

queryStatement
    : (explainDesc | optimizerTrace) ? queryRelation outfile?;

queryRelation
    : withClause? queryNoWith
    ;

withClause
    : WITH commonTableExpression (',' commonTableExpression)*
    ;

queryNoWith
    : queryPrimary (ORDER BY sortItem (',' sortItem)*)? (limitElement)?
    ;

temporalClause
    : AS OF expression
    | FOR SYSTEM_TIME AS OF TIMESTAMP string
    | FOR SYSTEM_TIME BETWEEN expression AND expression
    | FOR SYSTEM_TIME FROM expression TO expression
    | FOR SYSTEM_TIME ALL
    ;

queryPrimary
    : querySpecification                                                                    #queryPrimaryDefault
    | subquery                                                                              #queryWithParentheses
    | left=queryPrimary operator=INTERSECT setQuantifier? right=queryPrimary                #setOperation
    | left=queryPrimary operator=(UNION | EXCEPT | MINUS)
        setQuantifier? right=queryPrimary                                                   #setOperation
    ;

subquery
    : '(' queryRelation ')'
    ;

rowConstructor
     :'(' expressionList ')'
     ;

sortItem
    : expression ordering = (ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

limitElement
    : LIMIT limit =INTEGER_VALUE (OFFSET offset=INTEGER_VALUE)?
    | LIMIT offset =INTEGER_VALUE ',' limit=INTEGER_VALUE
    ;

querySpecification
    : SELECT setVarHint* setQuantifier? selectItem (',' selectItem)*
      fromClause
      ((QUALIFY qualifyFunction=selectItem comparisonOperator limit=INTEGER_VALUE)?
      | (WHERE where=expression)? (GROUP BY groupingElement)? (HAVING having=expression)?)
    ;

fromClause
    : (FROM relations)?                                                                 #from
    | FROM DUAL                                                                         #dual
    ;

groupingElement
    : ROLLUP '(' (expressionList)? ')'                                                  #rollup
    | CUBE '(' (expressionList)? ')'                                                    #cube
    | GROUPING SETS '(' groupingSet (',' groupingSet)* ')'                              #multipleGroupingSets
    | expressionList                                                                    #singleGroupingSet
    ;

groupingSet
    : '(' expression? (',' expression)* ')'
    ;

commonTableExpression
    : name=identifier (columnAliases)? AS '(' queryRelation ')'
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? (identifier | string))?                                            #selectSingle
    | qualifiedName '.' ASTERISK_SYMBOL                                                  #selectAll
    | ASTERISK_SYMBOL                                                                    #selectAll
    ;

relations
    : relation (',' LATERAL? relation)*
    ;

relation
    : relationPrimary joinRelation*
    | '(' relationPrimary joinRelation* ')'
    ;

relationPrimary
    : qualifiedName temporalClause? partitionNames? tabletList? (
        AS? alias=identifier)? bracketHint?                                             #tableAtom
    | '(' VALUES rowConstructor (',' rowConstructor)* ')'
        (AS? alias=identifier columnAliases?)?                                          #inlineTable
    | subquery (AS? alias=identifier columnAliases?)?                                   #subqueryWithAlias
    | qualifiedName '(' expressionList ')'
        (AS? alias=identifier columnAliases?)?                                          #tableFunction
    | '(' relations ')'                                                                 #parenthesizedRelation
    ;

joinRelation
    : crossOrInnerJoinType bracketHint?
            LATERAL? rightRelation=relationPrimary joinCriteria?
    | outerAndSemiJoinType bracketHint?
            LATERAL? rightRelation=relationPrimary joinCriteria
    ;

crossOrInnerJoinType
    : JOIN | INNER JOIN
    | CROSS | CROSS JOIN
    ;

outerAndSemiJoinType
    : LEFT JOIN | RIGHT JOIN | FULL JOIN
    | LEFT OUTER JOIN | RIGHT OUTER JOIN
    | FULL OUTER JOIN
    | LEFT SEMI JOIN | RIGHT SEMI JOIN
    | LEFT ANTI JOIN | RIGHT ANTI JOIN
    ;

bracketHint
    : '[' identifier (',' identifier)* ']'
    ;

setVarHint
    : '/*+' SET_VAR '(' hintMap (',' hintMap)* ')' '*/'
    ;

hintMap
    : k=identifierOrString '=' v=literalExpression
    ;

joinCriteria
    : ON expression
    | USING '(' identifier (',' identifier)* ')'
    ;

columnAliases
    : '(' identifier (',' identifier)* ')'
    ;

partitionNames
    : TEMPORARY? (PARTITION | PARTITIONS) '(' identifier (',' identifier)* ')'
    | TEMPORARY? (PARTITION | PARTITIONS) identifier
    ;

tabletList
    : TABLET '(' INTEGER_VALUE (',' INTEGER_VALUE)* ')'
    ;

// ------------------------------------------- Expression --------------------------------------------------------------

/**
 * Operator precedences are shown in the following list, from highest precedence to the lowest.
 *
 * !
 * - (unary minus), ~ (unary bit inversion)
 * ^
 * *, /, DIV, %, MOD
 * -, +
 * &
 * |
 * = (comparison), <=>, >=, >, <=, <, <>, !=, IS, LIKE, REGEXP
 * BETWEEN, CASE WHEN
 * NOT
 * AND, &&
 * XOR
 * OR, ||
 * = (assignment)
 */

expressionsWithDefault
    : '(' expressionOrDefault (',' expressionOrDefault)* ')'
    ;

expressionOrDefault
    : expression | DEFAULT
    ;

expressionSingleton
    : expression EOF
    ;

expression
    : booleanExpression                                                                   #expressionDefault
    | NOT expression                                                                      #logicalNot
    | left=expression operator=(AND|LOGICAL_AND) right=expression                         #logicalBinary
    | left=expression operator=(OR|LOGICAL_OR) right=expression                           #logicalBinary
    ;

expressionList
    : expression (',' expression)*
    ;

booleanExpression
    : predicate                                                                           #booleanExpressionDefault
    | booleanExpression IS NOT? NULL                                                      #isNull
    | left = booleanExpression comparisonOperator right = predicate                       #comparison
    | booleanExpression comparisonOperator '(' queryRelation ')'                          #scalarSubquery
    ;

predicate
    : valueExpression (predicateOperations[$valueExpression.ctx])?
    | tupleInSubquery
    ;

tupleInSubquery
    : '(' expression (',' expression)+ ')' NOT? IN '(' queryRelation ')'
    ;

predicateOperations [ParserRuleContext value]
    : NOT? IN '(' expressionList ')'                                                      #inList
    | NOT? IN '(' queryRelation ')'                                                       #inSubquery
    | NOT? BETWEEN lower = valueExpression AND upper = predicate                          #between
    | NOT? (LIKE | RLIKE | REGEXP) pattern=valueExpression                                #like
    ;

valueExpression
    : primaryExpression                                                                   #valueExpressionDefault
    | left = valueExpression operator = BITXOR right = valueExpression                    #arithmeticBinary
    | left = valueExpression operator = (
              ASTERISK_SYMBOL
            | SLASH_SYMBOL
            | PERCENT_SYMBOL
            | INT_DIV
            | MOD)
      right = valueExpression                                                             #arithmeticBinary
    | left = valueExpression operator = (PLUS_SYMBOL | MINUS_SYMBOL)
        right = valueExpression                                                           #arithmeticBinary
    | left = valueExpression operator = BITAND right = valueExpression                    #arithmeticBinary
    | left = valueExpression operator = BITOR right = valueExpression                     #arithmeticBinary
    | left = valueExpression operator = BIT_SHIFT_LEFT right = valueExpression              #arithmeticBinary
    | left = valueExpression operator = BIT_SHIFT_RIGHT right = valueExpression             #arithmeticBinary
    | left = valueExpression operator = BIT_SHIFT_RIGHT_LOGICAL right = valueExpression     #arithmeticBinary
    ;

primaryExpression
    : userVariable                                                                        #userVariableExpression
    | systemVariable                                                                      #systemVariableExpression
    | functionCall                                                                        #functionCallExpression
    | '{' FN functionCall '}'                                                             #odbcFunctionCallExpression
    | primaryExpression COLLATE (identifier | string)                                     #collate
    | literalExpression                                                                   #literal
    | columnReference                                                                     #columnRef
    | base = primaryExpression (DOT_IDENTIFIER | '.' fieldName = identifier )             #dereference
    | left = primaryExpression CONCAT right = primaryExpression                           #concat
    | operator = (MINUS_SYMBOL | PLUS_SYMBOL | BITNOT) primaryExpression                  #arithmeticUnary
    | operator = LOGICAL_NOT primaryExpression                                            #arithmeticUnary
    | '(' expression ')'                                                                  #parenthesizedExpression
    | EXISTS '(' queryRelation ')'                                                        #exists
    | subquery                                                                            #subqueryExpression
    | CAST '(' expression AS type ')'                                                     #cast
    | CONVERT '(' expression ',' type ')'                                                 #convert
    | CASE caseExpr=expression whenClause+ (ELSE elseExpression=expression)? END          #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                              #searchedCase
    | arrayType? '[' (expressionList)? ']'                                                #arrayConstructor
    | value=primaryExpression '[' index=valueExpression ']'                               #collectionSubscript
    | primaryExpression '[' start=INTEGER_VALUE? ':' end=INTEGER_VALUE? ']'               #arraySlice
    | primaryExpression ARROW string                                                      #arrowExpression
    | (identifier | identifierList) '->' expression                                       #lambdaFunctionExpr
    ;

literalExpression
    : NULL                                                                                #nullLiteral
    | booleanValue                                                                        #booleanLiteral
    | number                                                                              #numericLiteral
    | (DATE | DATETIME) string                                                            #dateLiteral
    | string                                                                              #stringLiteral
    | interval                                                                            #intervalLiteral
    | unitBoundary                                                                        #unitBoundaryLiteral
    | binary                                                                              #binaryLiteral
    ;

functionCall
    : EXTRACT '(' identifier FROM valueExpression ')'                                     #extract
    | GROUPING '(' (expression (',' expression)*)? ')'                                    #groupingOperation
    | GROUPING_ID '(' (expression (',' expression)*)? ')'                                 #groupingOperation
    | informationFunctionExpression                                                       #informationFunction
    | specialDateTimeExpression                                                           #specialDateTime
    | specialFunctionExpression                                                           #specialFunction
    | aggregationFunction over?                                                           #aggregationFunctionCall
    | windowFunction over                                                                 #windowFunctionCall
    | qualifiedName '(' (expression (',' expression)*)? ')'  over?                        #simpleFunctionCall
    ;

aggregationFunction
    : AVG '(' DISTINCT? expression ')'
    | COUNT '(' ASTERISK_SYMBOL? ')'
    | COUNT '(' DISTINCT? (expression (',' expression)*)? ')'
    | MAX '(' DISTINCT? expression ')'
    | MIN '(' DISTINCT? expression ')'
    | SUM '(' DISTINCT? expression ')'
    ;

userVariable
    : AT identifierOrString
    ;

systemVariable
    : AT AT (varType '.')? identifier
    ;

columnReference
    : identifier
    ;

informationFunctionExpression
    : name = DATABASE '(' ')'
    | name = SCHEMA '(' ')'
    | name = USER '(' ')'
    | name = CONNECTION_ID '(' ')'
    | name = CURRENT_USER ('(' ')')?
    ;

specialDateTimeExpression
    : name = CURRENT_DATE ('(' ')')?
    | name = CURRENT_TIME ('(' ')')?
    | name = CURRENT_TIMESTAMP ('(' ')')?
    | name = LOCALTIME ('(' ')')?
    | name = LOCALTIMESTAMP ('(' ')')?
    ;

specialFunctionExpression
    : CHAR '(' expression ')'
    | DAY '(' expression ')'
    | HOUR '(' expression ')'
    | IF '(' (expression (',' expression)*)? ')'
    | LEFT '(' expression ',' expression ')'
    | LIKE '(' expression ',' expression ')'
    | MINUTE '(' expression ')'
    | MOD '(' expression ',' expression ')'
    | MONTH '(' expression ')'
    | QUARTER '(' expression ')'
    | REGEXP '(' expression ',' expression ')'
    | REPLACE '(' (expression (',' expression)*)? ')'
    | RIGHT '(' expression ',' expression ')'
    | RLIKE '(' expression ',' expression ')'
    | SECOND '(' expression ')'
    | TIMESTAMPADD '(' unitIdentifier ',' expression ',' expression ')'
    | TIMESTAMPDIFF '(' unitIdentifier ',' expression ',' expression ')'
    //| WEEK '(' expression ')' TODO: Support week(expr) function
    | YEAR '(' expression ')'
    | PASSWORD '(' string ')'
    | FLOOR '(' expression ')'
    | CEIL '(' expression ')'
    ;

windowFunction
    : name = ROW_NUMBER '(' ')'
    | name = RANK '(' ')'
    | name = DENSE_RANK '(' ')'
    | name = NTILE  '(' expression? ')'
    | name = LEAD  '(' (expression ignoreNulls? (',' expression)*)? ')'
    | name = LAG '(' (expression ignoreNulls? (',' expression)*)? ')'
    | name = FIRST_VALUE '(' (expression ignoreNulls? (',' expression)*)? ')'
    | name = LAST_VALUE '(' (expression ignoreNulls? (',' expression)*)? ')'
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

over
    : OVER '('
        (PARTITION BY partition+=expression (',' partition+=expression)*)?
        (ORDER BY sortItem (',' sortItem)*)?
        windowFrame?
      ')'
    ;

ignoreNulls
    : IGNORE NULLS
    ;

windowFrame
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED boundType=PRECEDING                 #unboundedFrame
    | UNBOUNDED boundType=FOLLOWING                 #unboundedFrame
    | CURRENT ROW                                   #currentRowBound
    | expression boundType=(PRECEDING | FOLLOWING)  #boundedFrame
    ;

// ------------------------------------------- COMMON AST --------------------------------------------------------------

tableDesc
    : qualifiedName partitionNames?
    ;

restoreTableDesc
    : qualifiedName partitionNames? (AS identifier)?
    ;

explainDesc
    : (DESC | DESCRIBE | EXPLAIN) (LOGICAL | VERBOSE | COSTS)?
    ;

optimizerTrace
    : TRACE OPTIMIZER
    ;

partitionDesc
    : PARTITION BY RANGE identifierList '(' (rangePartitionDesc (',' rangePartitionDesc)*)? ')'
    | PARTITION BY LIST identifierList '(' (listPartitionDesc (',' listPartitionDesc)*)? ')'
    | PARTITION BY functionCall '(' (rangePartitionDesc (',' rangePartitionDesc)*)? ')'
    ;

listPartitionDesc
    : singleItemListPartitionDesc
    | multiItemListPartitionDesc
    ;

singleItemListPartitionDesc
    : PARTITION (IF NOT EXISTS)? identifier VALUES IN stringList propertyList?
    ;

multiItemListPartitionDesc
    : PARTITION (IF NOT EXISTS)? identifier VALUES IN '(' stringList (',' stringList)* ')' propertyList?
    ;

stringList
    : '(' string (',' string)* ')'
    ;

rangePartitionDesc
    : singleRangePartition
    | multiRangePartition
    ;

singleRangePartition
    : PARTITION (IF NOT EXISTS)? identifier VALUES partitionKeyDesc propertyList?
    ;

multiRangePartition
    : START '(' string ')' END '(' string ')' EVERY '(' interval ')'
    | START '(' string ')' END '(' string ')' EVERY '(' INTEGER_VALUE ')'
    ;

partitionRangeDesc
    : START '(' string ')' END '(' string ')'
    ;

partitionKeyDesc
    : LESS THAN (MAXVALUE | partitionValueList)
    | '[' partitionValueList ',' partitionValueList ')'
    ;

partitionValueList
    : '(' partitionValue (',' partitionValue)* ')'
    ;

partitionValue
    : MAXVALUE | string
    ;

distributionClause
    : DISTRIBUTED BY HASH identifierList (BUCKETS INTEGER_VALUE)?
    | DISTRIBUTED BY HASH identifierList
    ;

distributionDesc
    : DISTRIBUTED BY HASH identifierList (BUCKETS INTEGER_VALUE)?
    | DISTRIBUTED BY HASH identifierList
    ;

refreshSchemeDesc
    : REFRESH (ASYNC
    | ASYNC (START '(' string ')')? EVERY '(' interval ')'
    | INCREMENTAL
    | MANUAL)
    ;

properties
    : PROPERTIES '(' property (',' property)* ')'
    ;

extProperties
    : BROKER properties
    ;

propertyList
    : '(' property (',' property)* ')'
    ;

userPropertyList
    : property (',' property)*
    ;

property
    : key=string '=' value=string
    ;

varType
    : GLOBAL
    | LOCAL
    | SESSION
    | VERBOSE
    ;

comment
    : COMMENT string
    ;

columnNameWithComment
    : identifier comment?
    ;

outfile
    : INTO OUTFILE file=string fileFormat? properties?
    ;

fileFormat
    : FORMAT AS (identifier | string)
    ;

string
    : SINGLE_QUOTED_TEXT
    | DOUBLE_QUOTED_TEXT
    ;

binary
    : BINARY_SINGLE_QUOTED_TEXT
    | BINARY_DOUBLE_QUOTED_TEXT
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE | EQ_FOR_NULL
    ;

booleanValue
    : TRUE | FALSE
    ;

interval
    : INTERVAL value=expression from=unitIdentifier
    ;

unitIdentifier
    : YEAR | MONTH | WEEK | DAY | HOUR | MINUTE | SECOND | QUARTER
    ;

unitBoundary
    : FLOOR | CEIL
    ;

type
    : baseType
    | decimalType
    | arrayType
    | structType
    | mapType
    ;

arrayType
    : ARRAY '<' type '>'
    ;

mapType
    : MAP '<' type ',' type '>'
    ;

subfieldDesc
    : identifier type
    ;

subfieldDescs
    : subfieldDesc (',' subfieldDesc)*
    ;

structType
    : STRUCT '<' subfieldDescs '>'
    ;

typeParameter
    : '(' INTEGER_VALUE ')'
    ;

baseType
    : BOOLEAN
    | TINYINT typeParameter?
    | SMALLINT typeParameter?
    | SIGNED INT?
    | SIGNED INTEGER?
    | UNSIGNED INT?
    | UNSIGNED INTEGER?
    | INT typeParameter?
    | INTEGER typeParameter?
    | BIGINT typeParameter?
    | LARGEINT typeParameter?
    | FLOAT
    | DOUBLE
    | DATE
    | DATETIME
    | TIME
    | CHAR typeParameter?
    | VARCHAR typeParameter?
    | STRING
    | BITMAP
    | HLL
    | PERCENTILE
    | JSON
    | VARBINARY typeParameter?
    ;

decimalType
    : (DECIMAL | DECIMALV2 | DECIMAL32 | DECIMAL64 | DECIMAL128) ('(' precision=INTEGER_VALUE (',' scale=INTEGER_VALUE)? ')')?
    ;

qualifiedName
    : identifier (DOT_IDENTIFIER | '.' identifier)*
    ;

identifier
    : LETTER_IDENTIFIER      #unquotedIdentifier
    | nonReserved            #unquotedIdentifier
    | DIGIT_IDENTIFIER       #digitIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    ;

identifierList
    : '(' identifier (',' identifier)* ')'
    ;

identifierOrString
    : identifier
    | string
    ;

identifierOrStringList
    : identifierOrString (',' identifierOrString)*
    ;

user
    : identifierOrString                                     # userWithoutHost
    | identifierOrString '@' identifierOrString              # userWithHost
    | identifierOrString '@' '[' identifierOrString ']'      # userWithHostAndBlanket
    ;

assignment
    : identifier EQ expressionOrDefault
    ;

assignmentList
    : assignment (',' assignment)*
    ;

number
    : DECIMAL_VALUE  #decimalValue
    | DOUBLE_VALUE   #doubleValue
    | INTEGER_VALUE  #integerValue
    ;

authOption
    : IDENTIFIED BY PASSWORD? string                            # authWithoutPlugin
    | IDENTIFIED WITH identifierOrString ((BY | AS) string)?    # authWithPlugin
    ;

nonReserved
    : AFTER | AGGREGATE | ASYNC | AUTHORS | AVG | ADMIN
    | BACKEND | BACKENDS | BACKUP | BEGIN | BITMAP_UNION | BOOLEAN | BROKER | BUCKETS | BUILTIN
    | CAST | CATALOG | CATALOGS | CEIL | CHAIN | CHARSET | CLUSTER | CLUSTERS | CURRENT | COLLATION | COLUMNS
    | COMMENT | COMMIT | COMMITTED | COMPUTE | CONNECTION | CONNECTION_ID | CONSISTENT | COSTS | COUNT | CONFIG
    | DATA | DATE | DATETIME | DAY | DECOMMISSION | DISTRIBUTION | DUPLICATE | DYNAMIC
    | END | ENGINE | ENGINES | ERRORS | EVENTS | EXECUTE | EXTERNAL | EXTRACT | EVERY
    | FIELDS | FILE | FILTER | FIRST | FLOOR | FOLLOWING | FORMAT | FN | FRONTEND | FRONTENDS | FOLLOWER | FREE | FUNCTIONS
    | GLOBAL | GRANTS
    | HASH | HISTOGRAM | HELP | HLL_UNION | HOUR | HUB
    | IDENTIFIED | IMPERSONATE | INDEXES | INSTALL | INTERMEDIATE | INTERVAL | ISOLATION
    | JOB
    | LABEL | LAST | LESS | LEVEL | LIST | LOCAL | LOGICAL
    | MANUAL | MAP | MATERIALIZED | MAX | META | MIN | MINUTE | MODE | MODIFY | MONTH | MERGE
    | NAME | NAMES | NEGATIVE | NO | NODE | NULLS
    | OBSERVER | OF | OFFSET | ONLY | OPEN | OPTION | OVERWRITE
    | PARTITIONS | PASSWORD | PATH | PAUSE | PERCENTILE_UNION | PLUGIN | PLUGINS | PRECEDING | PROC | PROCESSLIST
    | PROPERTIES | PROPERTY
    | QUARTER | QUERY | QUOTA
    | RANDOM | RECOVER | REFRESH | REPAIR | REPEATABLE | REPLACE_IF_NOT_NULL | REPLICA | REPOSITORY | REPOSITORIES
    | RESOURCE | RESOURCES | RESTORE | RESUME | RETURNS | REVERT | ROLE | ROLES | ROLLUP | ROLLBACK | ROUTINE | ROW
    | SAMPLE | SECOND | SERIALIZABLE | SESSION | SETS | SIGNED | SNAPSHOT | SQLBLACKLIST | START | SUM | STATUS | STOP
    | STORAGE| STRING | STRUCT | STATS | SUBMIT | SUSPEND | SYNC | SYSTEM_TIME
    | TABLES | TABLET | TASK | TEMPORARY | TIMESTAMP | TIMESTAMPADD | TIMESTAMPDIFF | THAN | TIME | TRANSACTION
    | TRIGGERS | TRUNCATE | TYPE | TYPES
    | UNBOUNDED | UNCOMMITTED | UNINSTALL | USER | USERS
    | VALUE | VARIABLES | VIEW | VERBOSE
    | WARNINGS | WEEK | WHITELIST | WORK | WRITE  | WAREHOUSE | WAREHOUSES
    | YEAR
    | DOTDOTDOT
    ;
