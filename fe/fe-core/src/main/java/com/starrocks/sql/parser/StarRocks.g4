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
    : singleStatement+ EOF
    ;

singleStatement
    : (statement (SEMICOLON | EOF)) | emptyStatement
    ;
emptyStatement
    : SEMICOLON
    ;

statement
    // Query Statement
    : queryStatement

    // Warehouse Statement
    | showWarehousesStatement
    | showClustersStatement

    // Database Statement
    | useDatabaseStatement
    | useCatalogStatement
    | setCatalogStatement
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
    | createTemporaryTableStatement
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
    | dropTaskStatement

    // Materialized View Statement
    | createMaterializedViewStatement
    | showMaterializedViewsStatement
    | dropMaterializedViewStatement
    | alterMaterializedViewStatement
    | refreshMaterializedViewStatement
    | cancelRefreshMaterializedViewStatement

    // Catalog Statement
    | createExternalCatalogStatement
    | dropExternalCatalogStatement
    | showCatalogsStatement
    | showCreateExternalCatalogStatement
    | alterCatalogStatement

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
    | showCreateRoutineLoadStatement

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
    | executeScriptStatement

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

    // Profile Statement
    | analyzeProfileStatement

    // Resource Group Statement
    | createResourceGroupStatement
    | dropResourceGroupStatement
    | alterResourceGroupStatement
    | showResourceGroupStatement
    | showResourceGroupUsageStatement

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
    | showPrivilegesStatement
    | showProcedureStatement
    | showProcStatement
    | showProcesslistStatement
    | showProfilelistStatement
    | showRunningQueriesStatement
    | showStatusStatement
    | showTabletStatement
    | showTransactionStatement
    | showTriggersStatement
    | showUserPropertyStatement
    | showVariablesStatement
    | showWarningStatement
    | helpStatement

    // authz Statement
    | createUserStatement
    | dropUserStatement
    | alterUserStatement
    | showUserStatement
    | showAuthenticationStatement
    | executeAsStatement
    | createRoleStatement
    | alterRoleStatement
    | dropRoleStatement
    | showRolesStatement
    | grantRoleStatement
    | revokeRoleStatement
    | setRoleStatement
    | setDefaultRoleStatement
    | grantPrivilegeStatement
    | revokePrivilegeStatement
    | showGrantsStatement
    | createSecurityIntegrationStatement
    | alterSecurityIntegrationStatement
    | dropSecurityIntegrationStatement
    | showSecurityIntegrationStatement
    | showCreateSecurityIntegrationStatement
    | createRoleMappingStatement
    | alterRoleMappingStatement
    | dropRoleMappingStatement
    | showRoleMappingStatement
    | refreshRoleMappingStatement

    // Security Policy
    | createMaskingPolicyStatement
    | dropMaskingPolicyStatement
    | alterMaskingPolicyStatement
    | showMaskingPolicyStatement
    | showCreateMaskingPolicyStatement

    | createRowAccessPolicyStatement
    | dropRowAccessPolicyStatement
    | alterRowAccessPolicyStatement
    | showRowAccessPolicyStatement
    | showCreateRowAccessPolicyStatement

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

    // Backend BlackList
    | addBackendBlackListStatement
    | delBackendBlackListStatement
    | showBackendBlackListStatement

    // Data Cache management statement
    | createDataCacheRuleStatement
    | showDataCacheRulesStatement
    | dropDataCacheRuleStatement
    | clearDataCacheRulesStatement

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

    // Storage Volume Statement
    | createStorageVolumeStatement
    | alterStorageVolumeStatement
    | dropStorageVolumeStatement
    | showStorageVolumesStatement
    | descStorageVolumeStatement
    | setDefaultStorageVolumeStatement

    // Pipe Statement
    | createPipeStatement
    | dropPipeStatement
    | alterPipeStatement
    | showPipeStatement
    | descPipeStatement

    // Compaction Statement
    | cancelCompactionStatement

    // FailPoint Statement
    | updateFailPointStatusStatement
    | showFailPointStatement

    // prepare_stmt
    | prepareStatement
    | executeStatement
    | deallocateStatement

    // Dictionary Statement
    | createDictionaryStatement
    | dropDictionaryStatement
    | refreshDictionaryStatement
    | showDictionaryStatement
    | cancelRefreshDictionaryStatement

    // Unsupported Statement
    | unsupportedStatement
    ;

// ---------------------------------------- DataBase Statement ---------------------------------------------------------

useDatabaseStatement
    : USE qualifiedName
    ;

useCatalogStatement
    : USE string
    ;

setCatalogStatement
    : SET CATALOG identifierOrString
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
    : CREATE (DATABASE | SCHEMA) (IF NOT EXISTS)? (catalog=identifier '.')? database=identifier charsetDesc? collateDesc? properties?
    ;

dropDbStatement
    : DROP (DATABASE | SCHEMA) (IF EXISTS)? (catalog=identifier '.')? database=identifier FORCE?
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
          withRowAccessPolicy*
          comment?
          partitionDesc?
          distributionDesc?
          orderByDesc?
          rollupDesc?
          properties?
          extProperties?
     ;

columnDesc
    : identifier type charsetName? KEY? aggDesc? (NULL | NOT NULL)?
    (defaultDesc | AUTO_INCREMENT | generatedColumnDesc)?
    (withMaskingPolicy)?
    comment?
    ;

charsetName
    : CHAR SET identifier
    | CHARSET identifier
    | CHARACTER SET identifier
    ;

defaultDesc
    : DEFAULT (string | NULL | CURRENT_TIMESTAMP | '(' qualifiedName '(' ')' ')')
    ;

generatedColumnDesc
    : AS expression
    ;

indexDesc
    : INDEX indexName=identifier identifierList (indexType propertyList?)? comment?
    ;

engineDesc
    : ENGINE EQ identifier
    ;

charsetDesc
    : DEFAULT? (CHAR SET | CHARSET | CHARACTER SET) EQ? identifierOrString
    ;

collateDesc
    : DEFAULT? COLLATE EQ? identifierOrString
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

withMaskingPolicy
    : WITH MASKING POLICY policyName=qualifiedName (USING identifierList)?
    ;

withRowAccessPolicy
    : WITH ROW ACCESS POLICY policyName=qualifiedName (ON identifierList)?
    ;

orReplace:
    (OR REPLACE)?
    ;
ifNotExists:
    (IF NOT EXISTS)?
    ;

createTemporaryTableStatement
    : CREATE TEMPORARY TABLE qualifiedName
        queryStatement
    ;

createTableAsSelectStatement
    : CREATE TABLE (IF NOT EXISTS)? qualifiedName
        ('(' (identifier (',' identifier)*  (',' indexDesc)* | indexDesc (',' indexDesc)*) ')')?
        keyDesc?
        comment?
        partitionDesc?
        distributionDesc?
        orderByDesc?
        properties?
        AS queryStatement
    ;

dropTableStatement
    : DROP TEMPORARY? TABLE (IF EXISTS)? qualifiedName FORCE?
    ;

alterTableStatement
    : ALTER TABLE qualifiedName alterClause (',' alterClause)*
    | ALTER TABLE qualifiedName ADD ROLLUP rollupItem (',' rollupItem)*
    | ALTER TABLE qualifiedName DROP ROLLUP identifier (',' identifier)*
    ;

createIndexStatement
    : CREATE INDEX indexName=identifier
        ON qualifiedName identifierList (indexType propertyList?)?
        comment?
    ;

dropIndexStatement
    : DROP INDEX indexName=identifier ON qualifiedName
    ;

indexType
    : USING (BITMAP | GIN)
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
    : SHOW ALTER TABLE (COLUMN | ROLLUP | OPTIMIZE) ((FROM | IN) db=qualifiedName)?
        (WHERE expression)? (ORDER BY sortItem (',' sortItem)*)? (limitElement)?
    | SHOW ALTER MATERIALIZED VIEW ((FROM | IN) db=qualifiedName)?
              (WHERE expression)? (ORDER BY sortItem (',' sortItem)*)? (limitElement)?
    ;

descTableStatement
    : (DESC | DESCRIBE) table=qualifiedName ALL?
    ;

createTableLikeStatement
    : CREATE (EXTERNAL)? TABLE (IF NOT EXISTS)? qualifiedName
        partitionDesc?
        distributionDesc?
        properties?
        LIKE qualifiedName
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
    : CANCEL ALTER TABLE (COLUMN | ROLLUP | OPTIMIZE)? FROM qualifiedName ('(' INTEGER_VALUE (',' INTEGER_VALUE)* ')')?
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
    : CREATE (OR REPLACE)? VIEW (IF NOT EXISTS)? qualifiedName
        ('(' columnNameWithComment (',' columnNameWithComment)* ')')?
        withRowAccessPolicy*
        comment? AS queryStatement
    ;

alterViewStatement
    : ALTER VIEW qualifiedName ('(' columnNameWithComment (',' columnNameWithComment)* ')')?  AS queryStatement
    | ALTER VIEW qualifiedName applyMaskingPolicyClause
    | ALTER VIEW qualifiedName applyRowAccessPolicyClause
    ;

dropViewStatement
    : DROP VIEW (IF EXISTS)? qualifiedName
    ;

columnNameWithComment
    : columnName=identifier withMaskingPolicy? comment?
    ;

// ------------------------------------------- Task Statement ----------------------------------------------------------

submitTaskStatement
    : SUBMIT TASK qualifiedName? properties?
    AS (createTableAsSelectStatement | insertStatement )
    ;

dropTaskStatement
    : DROP TASK qualifiedName
    ;

// ------------------------------------------- Materialized View Statement ---------------------------------------------

createMaterializedViewStatement
    : CREATE MATERIALIZED VIEW (IF NOT EXISTS)? mvName=qualifiedName
    ('(' columnNameWithComment (',' columnNameWithComment)* (',' indexDesc)* ')')?
    withRowAccessPolicy*
    comment?
    materializedViewDesc*
    AS queryStatement
    ;

materializedViewDesc
    : (PARTITION BY primaryExpression)
    | distributionDesc
    | orderByDesc
    | refreshSchemeDesc
    | properties
    ;

showMaterializedViewsStatement
    : SHOW MATERIALIZED VIEWS ((FROM | IN) db=qualifiedName)? ((LIKE pattern=string) | (WHERE expression))?
    ;

dropMaterializedViewStatement
    : DROP MATERIALIZED VIEW (IF EXISTS)? mvName=qualifiedName
    ;

alterMaterializedViewStatement
    : ALTER MATERIALIZED VIEW mvName=qualifiedName (
        refreshSchemeDesc |
        tableRenameClause |
        modifyPropertiesClause |
        swapTableClause )
    | ALTER MATERIALIZED VIEW mvName=qualifiedName statusDesc
    | ALTER MATERIALIZED VIEW qualifiedName applyMaskingPolicyClause
    | ALTER MATERIALIZED VIEW qualifiedName applyRowAccessPolicyClause
    ;

refreshMaterializedViewStatement
    : REFRESH MATERIALIZED VIEW mvName=qualifiedName (PARTITION partitionRangeDesc)? FORCE? (WITH (SYNC | ASYNC) MODE)?
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
    : ADMIN CHECK tabletList PROPERTIES '('property')'
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

alterCatalogStatement
    : ALTER CATALOG catalogName=identifierOrString modifyPropertiesClause
    ;

// ---------------------------------------- Warehouse Statement ---------------------------------------------------------

createWarehouseStatement
    : CREATE (WAREHOUSE) (IF NOT EXISTS)? warehouseName=identifierOrString
    properties?
    ;

showWarehousesStatement
    : SHOW WAREHOUSES ((LIKE pattern=string) | (WHERE expression))?
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

// ---------------------------------------- Storage Volume Statement ---------------------------------------------------

createStorageVolumeStatement
    : CREATE STORAGE VOLUME (IF NOT EXISTS)? storageVolumeName=identifierOrString typeDesc locationsDesc
          comment? properties?
    ;

typeDesc
    : TYPE EQ identifier
    ;

locationsDesc
    : LOCATIONS EQ stringList
    ;

showStorageVolumesStatement
    : SHOW STORAGE VOLUMES (LIKE pattern=string)?
    ;

dropStorageVolumeStatement
    : DROP STORAGE VOLUME (IF EXISTS)? storageVolumeName=identifierOrString
    ;

alterStorageVolumeStatement
    : ALTER STORAGE VOLUME identifierOrString alterStorageVolumeClause (',' alterStorageVolumeClause)*
    ;

alterStorageVolumeClause
    : modifyStorageVolumeCommentClause
    | modifyStorageVolumePropertiesClause
    ;

modifyStorageVolumePropertiesClause
    : SET propertyList
    ;

modifyStorageVolumeCommentClause
    : COMMENT '=' string
    ;

descStorageVolumeStatement
    : (DESC | DESCRIBE) STORAGE VOLUME identifierOrString
    ;

setDefaultStorageVolumeStatement
    : SET identifierOrString AS DEFAULT STORAGE VOLUME
    ;

// ------------------------------------------- FailPoint Statement -----------------------------------------------------

updateFailPointStatusStatement
    : ADMIN DISABLE FAILPOINT string (ON BACKEND string)?
    | ADMIN ENABLE FAILPOINT string (WITH INTEGER_VALUE TIMES)? (ON BACKEND string)?
    | ADMIN ENABLE FAILPOINT string (WITH DECIMAL_VALUE PROBABILITY)? (ON BACKEND string)?
    ;

showFailPointStatement
    : SHOW FAILPOINTS ((LIKE pattern=string))? (ON BACKEND string)?
    ;

// ------------------------------------------- Dictionary Statement -----------------------------------------------------

createDictionaryStatement
    : CREATE DICTIONARY dictionaryName USING qualifiedName
        '(' dictionaryColumnDesc (',' dictionaryColumnDesc)* ')'
        properties?
    ;

dropDictionaryStatement
    : DROP DICTIONARY qualifiedName CACHE?
    ;

refreshDictionaryStatement
    : REFRESH DICTIONARY qualifiedName
    ;

showDictionaryStatement
    : SHOW DICTIONARY qualifiedName?
    ;

cancelRefreshDictionaryStatement
    : CANCEL REFRESH DICTIONARY qualifiedName;

dictionaryColumnDesc
    : qualifiedName KEY
    | qualifiedName VALUE
    ;

dictionaryName
    : qualifiedName
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
    | createImageClause
    | cleanTabletSchedQClause
    | decommissionDiskClause
    | cancelDecommissionDiskClause
    | disableDiskClause
    | cancelDisableDiskClause

    //Alter table clause
    | createIndexClause
    | dropIndexClause
    | tableRenameClause
    | swapTableClause
    | modifyPropertiesClause
    | addColumnClause
    | addColumnsClause
    | dropColumnClause
    | modifyColumnClause
    | columnRenameClause
    | reorderColumnsClause
    | rollupRenameClause
    | compactionClause
    | modifyCommentClause
    | optimizeClause

    //Apply Policy clause
    | applyMaskingPolicyClause
    | applyRowAccessPolicyClause

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

createImageClause
    : CREATE IMAGE
    ;

cleanTabletSchedQClause
    : CLEAN TABLET SCHEDULER QUEUE
    ;

decommissionDiskClause
    : DECOMMISSION DISK string (',' string)* ON BACKEND string
    ;

cancelDecommissionDiskClause
    : CANCEL DECOMMISSION DISK string (',' string)* ON BACKEND string
    ;

disableDiskClause
    : DISABLE DISK string (',' string)* ON BACKEND string
    ;

cancelDisableDiskClause
    : CANCEL DISABLE DISK string (',' string)* ON BACKEND string
    ;

// ---------Alter table clause---------

createIndexClause
    : ADD INDEX indexName=identifier identifierList (indexType propertyList?)? comment?
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

modifyPropertiesClause
    : SET propertyList
    ;

modifyCommentClause
    : COMMENT '=' string
    ;

optimizeClause
    : partitionNames?
      keyDesc?
      partitionDesc?
      orderByDesc?
      distributionDesc?
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
    : RENAME COLUMN oldColumn=identifier TO newColumn=identifier
    ;

reorderColumnsClause
    : ORDER BY identifierList (FROM rollupName=identifier)? properties?
    ;

rollupRenameClause
    : RENAME ROLLUP rollupName=identifier newRollupName=identifier
    ;

compactionClause
    : (BASE | CUMULATIVE)? COMPACT (identifier | identifierList)?
    ;

applyMaskingPolicyClause
    : MODIFY COLUMN columnName=identifier SET MASKING POLICY policyName=qualifiedName (USING identifierList)?
    | MODIFY COLUMN columnName=identifier UNSET MASKING POLICY
    ;

applyRowAccessPolicyClause
    : ADD ROW ACCESS POLICY policyName=qualifiedName (ON identifierList)?
    | DROP ROW ACCESS POLICY policyName=qualifiedName
    | DROP ALL ROW ACCESS POLICIES
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
    : explainDesc? INSERT (INTO | OVERWRITE) (qualifiedName | (FILES propertyList) | (BLACKHOLE '(' ')')) partitionNames?
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

showCreateRoutineLoadStatement
    : SHOW CREATE ROUTINE LOAD (db=qualifiedName '.')? name=identifier
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

// ----------------------------------------- Analyze Profile Statement -------------------------------------------------

analyzeProfileStatement
    : ANALYZE PROFILE FROM string
    | ANALYZE PROFILE FROM string ',' INTEGER_VALUE (',' INTEGER_VALUE)*
    ;

// ------------------------------------------- Work Group Statement ----------------------------------------------------

createResourceGroupStatement
    : CREATE RESOURCE GROUP (IF NOT EXISTS)? (OR REPLACE)? identifier
        (TO classifier (',' classifier)*)?  WITH '(' property (',' property)* ')'
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

showResourceGroupUsageStatement
    : SHOW USAGE RESOURCE GROUP identifier
    | SHOW USAGE RESOURCE GROUPS
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

// ------------------------------------------- UDF Statement ----------------------------------------------------

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
        (ROWS TERMINATED BY rowSep=string)?
        format=fileFormat?
        (formatPropsField=formatProps)?
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

formatProps
    :  '('
            (SKIP_HEADER '=' INTEGER_VALUE)?
            (TRIM_SPACE '=' booleanValue)?
            (ENCLOSE '=' encloseCharacter=string)?
            (ESCAPE '=' escapeCharacter=string)?
        ')'
    ;

brokerDesc
    : WITH BROKER props=propertyList?
    | WITH BROKER name=identifierOrString props=propertyList?
    ;

resourceDesc
    : WITH RESOURCE name=identifierOrString props=propertyList?
    ;

showLoadStatement
    : SHOW LOAD (ALL)? (FROM identifier)? (WHERE expression)? (ORDER BY sortItem (',' sortItem)*)? limitElement?
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

// ------------------------------------------- Compaction Statement ----------------------------------------------------------

cancelCompactionStatement
    : CANCEL COMPACTION WHERE expression
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
    : SHOW (CHAR SET | CHARSET | CHARACTER SET) ((LIKE pattern=string) | (WHERE expression))?
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
showPrivilegesStatement
    : SHOW PRIVILEGES
    ;

showProcedureStatement
    : SHOW (PROCEDURE | FUNCTION) STATUS ((LIKE pattern=string) | (WHERE where=expression))?
    ;

showProcStatement
    : SHOW PROC path=string
    ;

showProcesslistStatement
    : SHOW FULL? PROCESSLIST
    ;

showProfilelistStatement
    : SHOW PROFILELIST (LIMIT limit =INTEGER_VALUE)?
    ;

showRunningQueriesStatement
    : SHOW RUNNING QUERIES (LIMIT limit =INTEGER_VALUE)?
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

// ------------------------------------------- Authz Statement -----------------------------------------------------

createUserStatement
    : CREATE USER (IF NOT EXISTS)? user authOption? (DEFAULT ROLE roleList)?
    ;

dropUserStatement
    : DROP USER (IF EXISTS)? user
    ;

alterUserStatement
    : ALTER USER (IF EXISTS)? user authOption
    | ALTER USER (IF EXISTS)? user DEFAULT ROLE (NONE| ALL | roleList)
    ;

showUserStatement
    : SHOW (USER | USERS)
    ;

showAuthenticationStatement
    : SHOW ALL AUTHENTICATION                                                                           #showAllAuthentication
    | SHOW AUTHENTICATION (FOR user)?                                                                   #showAuthenticationForUser
    ;

executeAsStatement
    : EXECUTE AS user (WITH NO REVERT)?
    ;

createRoleStatement
    : CREATE ROLE (IF NOT EXISTS)? roleList comment?
    ;

alterRoleStatement
    : ALTER ROLE (IF EXISTS)? roleList SET COMMENT '=' string
    ;

dropRoleStatement
    : DROP ROLE (IF EXISTS)? roleList
    ;

showRolesStatement
    : SHOW ROLES
    ;

grantRoleStatement
    : GRANT identifierOrStringList TO USER? user                                                        #grantRoleToUser
    | GRANT identifierOrStringList TO ROLE identifierOrString                                           #grantRoleToRole
    ;

revokeRoleStatement
    : REVOKE identifierOrStringList FROM USER? user                                                     #revokeRoleFromUser
    | REVOKE identifierOrStringList FROM ROLE identifierOrString                                        #revokeRoleFromRole
    ;

setRoleStatement
    : SET ROLE DEFAULT
    | SET ROLE NONE
    | SET ROLE ALL (EXCEPT roleList)?
    | SET ROLE roleList
    ;

setDefaultRoleStatement
    : SET DEFAULT ROLE (NONE | ALL | roleList) TO user;

grantRevokeClause
    : (USER? user | ROLE identifierOrString)
    ;

grantPrivilegeStatement
    : GRANT IMPERSONATE ON USER user (',' user)* TO grantRevokeClause (WITH GRANT OPTION)?              #grantOnUser
    | GRANT privilegeTypeList ON privObjectNameList TO grantRevokeClause (WITH GRANT OPTION)?           #grantOnTableBrief

    | GRANT privilegeTypeList ON GLOBAL? FUNCTION privFunctionObjectNameList
        TO grantRevokeClause (WITH GRANT OPTION)?                                                       #grantOnFunc
    | GRANT privilegeTypeList ON SYSTEM TO grantRevokeClause (WITH GRANT OPTION)?                       #grantOnSystem
    | GRANT privilegeTypeList ON privObjectType privObjectNameList
        TO grantRevokeClause (WITH GRANT OPTION)?                                                       #grantOnPrimaryObj
    | GRANT privilegeTypeList ON ALL privObjectTypePlural
        (IN isAll=ALL DATABASES| IN DATABASE identifierOrString)? TO grantRevokeClause
        (WITH GRANT OPTION)?                                                                            #grantOnAll
    ;

revokePrivilegeStatement
    : REVOKE IMPERSONATE ON USER user (',' user)* FROM grantRevokeClause                                #revokeOnUser
    | REVOKE privilegeTypeList ON privObjectNameList FROM grantRevokeClause                             #revokeOnTableBrief
    | REVOKE privilegeTypeList ON GLOBAL? FUNCTION privFunctionObjectNameList
        FROM grantRevokeClause                                                                          #revokeOnFunc
    | REVOKE privilegeTypeList ON SYSTEM FROM grantRevokeClause                                         #revokeOnSystem
    | REVOKE privilegeTypeList ON privObjectType privObjectNameList
        FROM grantRevokeClause                                                                          #revokeOnPrimaryObj
    | REVOKE privilegeTypeList ON ALL privObjectTypePlural
        (IN isAll=ALL DATABASES| IN DATABASE identifierOrString)? FROM grantRevokeClause                #revokeOnAll
    ;

showGrantsStatement
    : SHOW GRANTS
    | SHOW GRANTS FOR USER? user
    | SHOW GRANTS FOR ROLE identifierOrString
    ;

createSecurityIntegrationStatement
    : CREATE SECURITY INTEGRATION identifier properties
    ;

alterSecurityIntegrationStatement
    : ALTER SECURITY INTEGRATION identifier SET propertyList
    ;

dropSecurityIntegrationStatement
    : DROP SECURITY INTEGRATION identifier
    ;

showSecurityIntegrationStatement
    : SHOW SECURITY INTEGRATIONS
    ;

showCreateSecurityIntegrationStatement
    : SHOW CREATE SECURITY INTEGRATION identifier
    ;

createRoleMappingStatement
    : CREATE ROLE MAPPING identifier properties
    ;

alterRoleMappingStatement
    : ALTER ROLE MAPPING identifier SET propertyList
    ;

dropRoleMappingStatement
    : DROP ROLE MAPPING identifier
    ;

showRoleMappingStatement
    : SHOW ROLE MAPPINGS
    ;

refreshRoleMappingStatement
    : REFRESH ALL ROLE MAPPINGS
    ;

authOption
    : IDENTIFIED BY PASSWORD? string                                                                    #authWithoutPlugin
    | IDENTIFIED WITH identifierOrString ((BY | AS) string)?                                            #authWithPlugin
    ;

privObjectName
    : identifierOrStringOrStar ('.' identifierOrStringOrStar)?
    ;

privObjectNameList
    : privObjectName (',' privObjectName)*
    ;

privFunctionObjectNameList
    : qualifiedName '(' typeList ')' (',' qualifiedName '(' typeList ')')*
    ;

privilegeTypeList
    :  privilegeType (',' privilegeType)*
    ;

privilegeType
    : ALL PRIVILEGES?
    | ALTER | APPLY | BLACKLIST
    | CREATE (
        DATABASE| TABLE| VIEW| FUNCTION| GLOBAL FUNCTION| MATERIALIZED VIEW|
        RESOURCE| RESOURCE GROUP| EXTERNAL CATALOG | POLICY | STORAGE VOLUME
        | PIPE )
    | DELETE | DROP | EXPORT | FILE | IMPERSONATE | INSERT | GRANT | NODE | OPERATE
    | PLUGIN | REPOSITORY| REFRESH | SELECT | UPDATE | USAGE
    ;

privObjectType
    : CATALOG | DATABASE | MATERIALIZED VIEW | POLICY | RESOURCE | RESOURCE GROUP| STORAGE VOLUME | SYSTEM | TABLE| VIEW
    | PIPE
    ;

privObjectTypePlural
    : CATALOGS | DATABASES | FUNCTIONS | GLOBAL FUNCTIONS | MATERIALIZED VIEWS | POLICIES | RESOURCES | RESOURCE GROUPS
    | STORAGE VOLUMES | TABLES | USERS | VIEWS | PIPES
    ;

// ---------------------------------------- Security Policy Statement ---------------------------------------------------

createMaskingPolicyStatement
    : CREATE (OR REPLACE)? MASKING POLICY (IF NOT EXISTS)? policyName=qualifiedName
        AS '(' policySignature (',' policySignature)* ')' RETURNS type ARROW expression comment?
    ;

dropMaskingPolicyStatement
    : DROP MASKING POLICY (IF EXISTS)? policyName=qualifiedName FORCE?
    ;

alterMaskingPolicyStatement
    : ALTER MASKING POLICY (IF EXISTS)? policyName=qualifiedName SET BODY ARROW expression
    | ALTER MASKING POLICY (IF EXISTS)? policyName=qualifiedName SET COMMENT '=' string
    | ALTER MASKING POLICY (IF EXISTS)? policyName=qualifiedName RENAME TO newPolicyName=identifier
    ;

showMaskingPolicyStatement
    : SHOW MASKING POLICIES ((FROM | IN) db=qualifiedName)?
    ;

showCreateMaskingPolicyStatement
    : SHOW CREATE MASKING POLICY policyName=qualifiedName
    ;

createRowAccessPolicyStatement
    : CREATE (OR REPLACE)? ROW ACCESS POLICY (IF NOT EXISTS)? policyName=qualifiedName
      AS '(' policySignature (',' policySignature)* ')' RETURNS BOOLEAN ARROW expression comment?
    ;

dropRowAccessPolicyStatement
    : DROP ROW ACCESS POLICY (IF EXISTS)? policyName=qualifiedName FORCE?
    ;

alterRowAccessPolicyStatement
    : ALTER ROW ACCESS POLICY (IF EXISTS)? policyName=qualifiedName SET BODY ARROW expression
    | ALTER ROW ACCESS POLICY (IF EXISTS)? policyName=qualifiedName SET COMMENT '=' string
    | ALTER ROW ACCESS POLICY (IF EXISTS)? policyName=qualifiedName RENAME TO newPolicyName=identifier
    ;

showRowAccessPolicyStatement
    : SHOW ROW ACCESS POLICIES ((FROM | IN) db=qualifiedName)?
    ;

showCreateRowAccessPolicyStatement
    : SHOW CREATE ROW ACCESS POLICY policyName=qualifiedName
    ;

policySignature : identifier type;

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
    : CREATE (READ ONLY)? REPOSITORY repoName=identifier
    WITH BROKER brokerName=identifierOrString?
    ON LOCATION location=string
    (PROPERTIES propertyList)?
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

// ------------------------------------ backend BlackList Statement ---------------------------------------------------

addBackendBlackListStatement
    : ADD BACKEND BLACKLIST INTEGER_VALUE
    ;

delBackendBlackListStatement
    : DELETE BACKEND BLACKLIST INTEGER_VALUE (',' INTEGER_VALUE)*
    ;

showBackendBlackListStatement
    : SHOW BACKEND BLACKLIST
    ;

// -------------------------------------- DataCache Management Statement --------------------------------------------

dataCacheTarget
    : identifierOrStringOrStar '.' identifierOrStringOrStar '.' identifierOrStringOrStar
    ;

createDataCacheRuleStatement
    : CREATE DATACACHE RULE dataCacheTarget (WHERE expression)? PRIORITY '=' MINUS_SYMBOL? INTEGER_VALUE properties?
    ;

showDataCacheRulesStatement
    : SHOW DATACACHE RULES
    ;

dropDataCacheRuleStatement
    : DROP DATACACHE RULE INTEGER_VALUE
    ;

clearDataCacheRulesStatement
    : CLEAR DATACACHE RULES
    ;

// ------------------------------------------- Export Statement --------------------------------------------------------

exportStatement
    : EXPORT TABLE tableDesc columnAliases? TO string (WITH (SYNC | ASYNC) MODE)? properties? brokerDesc?
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

// -------------------------------------------- Pipe Statement ---------------------------------------------------------

createPipeStatement
    : CREATE orReplace PIPE ifNotExists qualifiedName
        properties?
        AS insertStatement
    ;

dropPipeStatement
    : DROP PIPE (IF EXISTS)? qualifiedName
    ;

alterPipeClause
    : SUSPEND |
        RESUME |
        RETRY ALL |
        RETRY FILE fileName=string |
        SET propertyList
    ;

alterPipeStatement
    : ALTER PIPE qualifiedName alterPipeClause
    ;

descPipeStatement
    : (DESC | DESCRIBE) PIPE qualifiedName
    ;

showPipeStatement
    : SHOW PIPES ((LIKE pattern=string) | (WHERE expression) | (FROM qualifiedName))?
        (ORDER BY sortItem (',' sortItem)*)? limitElement?
    ;


// ------------------------------------------- Set Statement -----------------------------------------------------------

setStatement
    : SET setVar (',' setVar)*
    ;

setVar
    : (CHAR SET | CHARSET | CHARACTER SET) (identifierOrString | DEFAULT)                       #setNames
    | NAMES (charset = identifierOrString | DEFAULT)
        (COLLATE (collate = identifierOrString | DEFAULT))?                                     #setNames
    | PASSWORD '=' (string | PASSWORD '(' string ')')                                           #setPassword
    | PASSWORD FOR user '=' (string | PASSWORD '(' string ')')                                  #setPassword
    | userVariable '=' expression                                                               #setUserVar
    | varType? identifier '=' setExprOrDefault                                                  #setSystemVar
    | systemVariable '=' setExprOrDefault                                                       #setSystemVar
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
    : identifierOrString (',' identifierOrString)*
    ;

executeScriptStatement
    : ADMIN EXECUTE ON (FRONTEND | INTEGER_VALUE) string
    ;

unsupportedStatement
    : START TRANSACTION (WITH CONSISTENT SNAPSHOT)?
    | BEGIN WORK?
    | COMMIT WORK? (AND NO? CHAIN)? (NO? RELEASE)?
    | ROLLBACK WORK? (AND NO? CHAIN)? (NO? RELEASE)?
    | LOCK TABLES lock_item (',' lock_item)*
    | UNLOCK TABLES
    ;

lock_item
    : identifier (AS? alias=identifier)? lock_type
    ;

lock_type
    : READ LOCAL?
    | LOW_PRIORITY? WRITE
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
    : LIMIT limit =(INTEGER_VALUE|PARAMETER) (OFFSET offset=(INTEGER_VALUE|PARAMETER))?
    | LIMIT offset =(INTEGER_VALUE|PARAMETER) ',' limit=(INTEGER_VALUE|PARAMETER)
    ;

querySpecification
    : SELECT setQuantifier? selectItem (',' selectItem)*
      fromClause
      ((WHERE where=expression)? (GROUP BY groupingElement)? (HAVING having=expression)?
       (QUALIFY qualifyFunction=selectItem comparisonOperator limit=INTEGER_VALUE)?)
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
    : qualifiedName temporalClause? partitionNames? tabletList? replicaList? (
        AS? alias=identifier)? bracketHint?                                             #tableAtom
    | '(' VALUES rowConstructor (',' rowConstructor)* ')'
        (AS? alias=identifier columnAliases?)?                                          #inlineTable
    | subquery (AS? alias=identifier columnAliases?)?                                   #subqueryWithAlias
    | qualifiedName '(' expressionList ')'
        (AS? alias=identifier columnAliases?)?                                          #tableFunction
    | TABLE '(' qualifiedName '(' argumentList ')' ')'
        (AS? alias=identifier columnAliases?)?                                          #normalizedTableFunction
    | FILES propertyList
        (AS? alias=identifier columnAliases?)?                                          #fileTableFunction
    | '(' relations ')'                                                                 #parenthesizedRelation
    ;

argumentList
    : expressionList
    | namedArgumentList
    ;

namedArgumentList
    : namedArgument (',' namedArgument)*
    ;

namedArgument
    : identifier '=>' expression                                                        #namedArguments
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
    | '[' identifier '|' primaryExpression literalExpressionList']'
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

// partitionNames should not support string, it should be identifier here only for compatibility with historical bugs
partitionNames
    : TEMPORARY? (PARTITION | PARTITIONS) '(' identifierOrString (',' identifierOrString)* ')'
    | TEMPORARY? (PARTITION | PARTITIONS) identifierOrString
    | keyPartitions
    ;

keyPartitions
    : PARTITION '(' keyPartition (',' keyPartition)* ')'                              #keyPartitionList
    ;

tabletList
    : TABLET '(' INTEGER_VALUE (',' INTEGER_VALUE)* ')'
    ;

prepareStatement
    : PREPARE identifier FROM prepareSql
    ;

prepareSql
    : statement
    | SINGLE_QUOTED_TEXT
    ;

executeStatement
    : EXECUTE identifier (USING  '@'identifierOrString (',' '@'identifierOrString)*)?
    ;

deallocateStatement
    : (DEALLOCATE | DROP) PREPARE identifier
    ;

replicaList
    : REPLICA '(' INTEGER_VALUE (',' INTEGER_VALUE)* ')'
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

mapExpressionList
    : mapExpression (',' mapExpression)*
    ;

mapExpression
    : key=expression ':' value=expression
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
    : NOT? IN '(' queryRelation ')'                                                       #inSubquery
    | NOT? IN '(' expressionList ')'                                                      #inList
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
    | DICTIONARY_GET '(' expressionList ')'                                               #dictionaryGetExpr
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
    | mapType '{' (mapExpressionList)? '}'                                                #mapConstructor
    | MAP '{' (mapExpressionList)? '}'                                                    #mapConstructor
    | value=primaryExpression '[' index=valueExpression ']'                               #collectionSubscript
    | primaryExpression '[' start=INTEGER_VALUE? ':' end=INTEGER_VALUE? ']'               #arraySlice
    | primaryExpression ARROW string                                                      #arrowExpression
    | (identifier | identifierList) '->' expression                                       #lambdaFunctionExpr
    | identifierList '->' '('(expressionList)?')'                                         #lambdaFunctionExpr
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
    | PARAMETER                                                                           #Parameter
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
    : AVG '(' setQuantifier? expression ')'
    | COUNT '(' ASTERISK_SYMBOL? ')'
    | COUNT '(' (setQuantifier bracketHint?)? (expression (',' expression)*)? ')'
    | MAX '(' setQuantifier? expression ')'
    | MIN '(' setQuantifier? expression ')'
    | SUM '(' setQuantifier? expression ')'
    | ARRAY_AGG '(' setQuantifier? expression (ORDER BY sortItem (',' sortItem)*)? ')'
    | GROUP_CONCAT '(' setQuantifier? expression (',' expression)* (ORDER BY sortItem (',' sortItem)*)? (SEPARATOR expression)? ')'
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
    : name = CATALOG '(' ')'
    | name = DATABASE '(' ')'
    | name = SCHEMA '(' ')'
    | name = USER '(' ')'
    | name = CURRENT_USER ('(' ')')?
    | name = CURRENT_ROLE ('(' ')')?
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
    | name = CUME_DIST '(' ')'
    | name = PERCENT_RANK '(' ')'
    | name = NTILE  '(' expression? ')'
    | name = LEAD  '(' (expression ignoreNulls? (',' expression)*)? ')' ignoreNulls?
    | name = LAG '(' (expression ignoreNulls? (',' expression)*)? ')' ignoreNulls?
    | name = FIRST_VALUE '(' (expression ignoreNulls? (',' expression)*)? ')' ignoreNulls?
    | name = LAST_VALUE '(' (expression ignoreNulls? (',' expression)*)? ')' ignoreNulls?
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

over
    : OVER '('
        (bracketHint? PARTITION BY partition+=expression (',' partition+=expression)*)?
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
    : (DESC | DESCRIBE | EXPLAIN) (LOGICAL | ANALYZE | VERBOSE | COSTS | SCHEDULER)?
    ;

optimizerTrace
    : TRACE (ALL | LOGS | TIMES | VALUES) identifier?
    ;

partitionDesc
    : PARTITION BY RANGE identifierList '(' (rangePartitionDesc (',' rangePartitionDesc)*)? ')'
    | PARTITION BY RANGE primaryExpression '(' (rangePartitionDesc (',' rangePartitionDesc)*)? ')'
    | PARTITION BY LIST? identifierList '(' (listPartitionDesc (',' listPartitionDesc)*)? ')'
    | PARTITION BY LIST? identifierList
    | PARTITION BY functionCall '(' (rangePartitionDesc (',' rangePartitionDesc)*)? ')'
    | PARTITION BY functionCall
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

literalExpressionList
    : '(' literalExpression (',' literalExpression)* ')'
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

keyPartition
    : partitionColName=identifier '=' partitionColValue=literalExpression
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
    | DISTRIBUTED BY RANDOM (BUCKETS INTEGER_VALUE)?
    ;

refreshSchemeDesc
    : REFRESH (IMMEDIATE | DEFERRED)? (ASYNC
    | ASYNC (START '(' string ')')? EVERY '(' interval ')'
    | INCREMENTAL
    | MANUAL)
    ;

statusDesc
    : ACTIVE
    | INACTIVE
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
    : YEAR | MONTH | WEEK | DAY | HOUR | MINUTE | SECOND | QUARTER | MILLISECOND | MICROSECOND
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
    | TEXT
    | BITMAP
    | HLL
    | PERCENTILE
    | JSON
    | VARBINARY typeParameter?
    | BINARY typeParameter?
    ;

decimalType
    : (DECIMAL | DECIMALV2 | DECIMAL32 | DECIMAL64 | DECIMAL128 | NUMERIC | NUMBER )
        ('(' precision=INTEGER_VALUE (',' scale=INTEGER_VALUE)? ')')?
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

identifierOrStringOrStar
    : ASTERISK_SYMBOL
    | identifier
    | string
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

nonReserved
    : ACCESS | ACTIVE | AFTER | AGGREGATE | APPLY | ASYNC | AUTHORS | AVG | ADMIN | ANTI | AUTHENTICATION | AUTO_INCREMENT
    | ARRAY_AGG
    | BACKEND | BACKENDS | BACKUP | BEGIN | BITMAP_UNION | BLACKLIST | BLACKHOLE | BINARY | BODY | BOOLEAN | BROKER | BUCKETS
    | BUILTIN | BASE
    | CACHE | CAST | CANCEL | CATALOG | CATALOGS | CEIL | CHAIN | CHARSET | CLEAN | CLEAR | CLUSTER | CLUSTERS | CURRENT | COLLATION | COLUMNS
    | CUME_DIST | CUMULATIVE | COMMENT | COMMIT | COMMITTED | COMPUTE | CONNECTION | CONSISTENT | COSTS | COUNT
    | CONFIG | COMPACT
    | DATA | DATE | DATACACHE | DATETIME | DAY | DECOMMISSION | DISABLE | DISK | DISTRIBUTION | DUPLICATE | DYNAMIC | DISTRIBUTED | DICTIONARY | DICTIONARY_GET | DEALLOCATE
    | ENABLE | END | ENGINE | ENGINES | ERRORS | EVENTS | EXECUTE | EXTERNAL | EXTRACT | EVERY | ENCLOSE | ESCAPE | EXPORT
    | FAILPOINT | FAILPOINTS | FIELDS | FILE | FILTER | FIRST | FLOOR | FOLLOWING | FORMAT | FN | FRONTEND | FRONTENDS | FOLLOWER | FREE
    | FUNCTIONS
    | GLOBAL | GRANTS | GROUP_CONCAT
    | HASH | HISTOGRAM | HELP | HLL_UNION | HOST | HOUR | HUB
    | IDENTIFIED | IMAGE | IMPERSONATE | INACTIVE | INCREMENTAL | INDEXES | INSTALL | INTEGRATION | INTEGRATIONS | INTERMEDIATE
    | INTERVAL | ISOLATION
    | JOB
    | LABEL | LAST | LESS | LEVEL | LIST | LOCAL | LOCATION | LOGS | LOGICAL | LOW_PRIORITY | LOCK | LOCATIONS
    | MASKING | MANUAL | MAP | MAPPING | MAPPINGS | MATERIALIZED | MAX | META | MIN | MINUTE | MODE | MODIFY | MONTH | MERGE | MINUS
    | NAME | NAMES | NEGATIVE | NO | NODE | NODES | NONE | NULLS | NUMBER | NUMERIC
    | OBSERVER | OF | OFFSET | ONLY | OPTIMIZER | OPEN | OPERATE | OPTION | OVERWRITE
    | PARTITIONS | PASSWORD | PATH | PAUSE | PENDING | PERCENTILE_UNION | PLUGIN | PLUGINS | POLICY | POLICIES
    | PERCENT_RANK | PRECEDING | PRIORITY | PROC | PROCESSLIST | PROFILE | PROFILELIST | PRIVILEGES | PROBABILITY | PROPERTIES | PROPERTY | PIPE | PIPES
    | QUARTER | QUERY | QUERIES | QUEUE | QUOTA | QUALIFY
    | REMOVE | REWRITE | RANDOM | RANK | RECOVER | REFRESH | REPAIR | REPEATABLE | REPLACE_IF_NOT_NULL | REPLICA | REPOSITORY
    | REPOSITORIES
    | RESOURCE | RESOURCES | RESTORE | RESUME | RETURNS | RETRY | REVERT | ROLE | ROLES | ROLLUP | ROLLBACK | ROUTINE | ROW | RUNNING | RULE | RULES
    | SAMPLE | SCHEDULER | SECOND | SECURITY | SEPARATOR | SERIALIZABLE |SEMI | SESSION | SETS | SIGNED | SNAPSHOT | SQLBLACKLIST | START
    | STREAM | SUM | STATUS | STOP | SKIP_HEADER | SWAP
    | STORAGE| STRING | STRUCT | STATS | SUBMIT | SUSPEND | SYNC | SYSTEM_TIME
    | TABLES | TABLET | TASK | TEMPORARY | TIMESTAMP | TIMESTAMPADD | TIMESTAMPDIFF | THAN | TIME | TIMES | TRANSACTION | TRACE
    | TRIM_SPACE
    | TRIGGERS | TRUNCATE | TYPE | TYPES
    | UNBOUNDED | UNCOMMITTED | UNSET | UNINSTALL | USAGE | USER | USERS | UNLOCK
    | VALUE | VARBINARY | VARIABLES | VIEW | VIEWS | VERBOSE | VOLUME | VOLUMES
    | WARNINGS | WEEK | WHITELIST | WORK | WRITE  | WAREHOUSE | WAREHOUSES
    | YEAR
    | DOTDOTDOT
    ;
