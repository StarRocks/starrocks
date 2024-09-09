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

package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.elasticsearch.EsUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.TemporaryTableMgr;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateTemporaryTableStmt;
import com.starrocks.sql.ast.DictionaryGetExpr;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.ast.KeysDesc;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.RandomDistributionDesc;
import com.starrocks.sql.common.EngineType;
import com.starrocks.sql.common.MetaUtils;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

public class CreateTableAnalyzer {
    private static final Logger LOG = LoggerFactory.getLogger(CreateTableAnalyzer.class);

    public static void analyze(CreateTableStmt statement, ConnectContext context) {
        final TableName tableNameObject = statement.getDbTbl();
        tableNameObject.normalization(context);

        final String catalogName = tableNameObject.getCatalog();
        MetaUtils.checkCatalogExistAndReport(catalogName);

        final String tableName = tableNameObject.getTbl();
        FeNameFormat.checkTableName(tableName);

        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalogName, tableNameObject.getDb());
        if (db == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, tableNameObject.getDb());
        }
        if (statement instanceof CreateTemporaryTableStmt) {
            analyzeTemporaryTable(statement, context, catalogName, db, tableName);
        } else {
            if (GlobalStateMgr.getCurrentState().getMetadataMgr()
                        .tableExists(catalogName, tableNameObject.getDb(), tableName) && !statement.isSetIfNotExists()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }
        }

        analyzeEngineName(statement, catalogName);
        analyzeCharsetName(statement);

        preCheckColumnRef(statement);
        analyzeKeysDesc(statement);
        analyzeSortKeys(statement);
        analyzePartitionDesc(statement);
        analyzeDistributionDesc(statement);
        analyzeColumnRef(statement, catalogName);

        if (statement.isHasGeneratedColumn()) {
            analyzeGeneratedColumn(statement, context);
        }

        analyzeIndexDefs(statement);
    }

    private static void analyzeTemporaryTable(CreateTableStmt stmt, ConnectContext context,
                                              String catalogName, Database db, String tableName) {
        ((CreateTemporaryTableStmt) stmt).setSessionId(context.getSessionId());
        if (catalogName != null && !CatalogMgr.isInternalCatalog(catalogName)) {
            throw new SemanticException("temporary table must be created under internal catalog");
        }
        Map<String, String> properties = stmt.getProperties();
        if (properties != null) {
            // temporary table doesn't support colocate_with property, so ignore it
            properties.remove(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH);
        }

        UUID sessionId = context.getSessionId();
        TemporaryTableMgr temporaryTableMgr = GlobalStateMgr.getCurrentState().getTemporaryTableMgr();
        if (temporaryTableMgr.tableExists(sessionId, db.getId(), tableName) && !stmt.isSetIfNotExists()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
        }
    }

    protected static void analyzeEngineName(CreateTableStmt stmt, String catalogName) {
        String engineName = stmt.getEngineName();

        if (CatalogMgr.isInternalCatalog(catalogName)) {
            if (Strings.isNullOrEmpty(engineName)) {
                engineName = EngineType.defaultEngine().name();
            } else {
                try {
                    engineName = EngineType.valueOf(engineName.toUpperCase()).name();
                } catch (IllegalArgumentException e) {
                    throw new SemanticException("Unknown engine name: %s", engineName);
                }
            }
        } else {
            String catalogType = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogType(catalogName);
            if (catalogType.equalsIgnoreCase(ConnectorType.UNIFIED.getName())) {
                if (Strings.isNullOrEmpty(engineName)) {
                    throw new SemanticException("Create table in unified catalog requires engine clause (ENGINE = ENGINE_NAME)");
                }
            } else {
                if (Strings.isNullOrEmpty(engineName)) {
                    engineName = catalogType; // use catalog type if engine is not specified
                }

                if (!engineName.equalsIgnoreCase(catalogType)) {
                    throw new SemanticException("Can't create %s table in the %s catalog", engineName, catalogType);
                }
            }
        }

        if ((stmt instanceof CreateTemporaryTableStmt) && !engineName.equalsIgnoreCase("olap")) {
            throw new SemanticException("temporary table only support olap engine");
        }

        stmt.setEngineName(engineName.toLowerCase());
    }

    private static void analyzeCharsetName(CreateTableStmt stmt) {
        String charsetName = stmt.getCharsetName();

        if (Strings.isNullOrEmpty(charsetName)) {
            charsetName = "utf8";
        }
        // be is not supported yet,so Display unsupported information to the user
        if (!charsetName.equalsIgnoreCase("utf8")) {
            throw new SemanticException("charset name %s is not supported yet", charsetName);
        }

        stmt.setCharsetName(charsetName.toLowerCase());
    }

    private static void preCheckColumnRef(CreateTableStmt statement) {
        List<ColumnDef> columnDefs = statement.getColumnDefs();
        if (columnDefs == null || columnDefs.isEmpty()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_TABLE_MUST_HAVE_COLUMNS);
        }

        if (columnDefs.size() > Config.max_column_number_per_table) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_TOO_MANY_COLUMNS, Config.max_column_number_per_table);
        }

        Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (ColumnDef columnDef : columnDefs) {
            if (!columnSet.add(columnDef.getName())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, columnDef.getName());
            }

            if (columnDef.getAggregateType() != null && columnDef.getAggregateType().isReplaceFamily()) {
                statement.setHasReplace(true);
            }

            if (columnDef.isGeneratedColumn()) {
                statement.setHasGeneratedColumn(true);
            }
        }
    }

    private static void analyzeColumnRef(CreateTableStmt statement, String catalogName) {
        String engineName = statement.getEngineName();
        KeysDesc keysDesc = statement.getKeysDesc();
        List<ColumnDef> columnDefs = statement.getColumnDefs();

        if (columnDefs.stream().filter(ColumnDef::isAutoIncrement).count() > 1) {
            throw new SemanticException("More than one AUTO_INCREMENT column defined in CREATE TABLE Statement",
                    statement.getPos());
        }

        List<Column> columns = new ArrayList<>();
        for (ColumnDef columnDef : columnDefs) {
            try {
                columnDef.analyze(statement.isOlapEngine(), CatalogMgr.isInternalCatalog(catalogName), engineName);
            } catch (AnalysisException e) {
                LOG.error("Column definition analyze failed.", e);
                throw new SemanticException(e.getMessage());
            }

            if (columnDef.isAutoIncrement()) {
                if (columnDef.getType() != Type.BIGINT) {
                    throw new SemanticException("The AUTO_INCREMENT column must be BIGINT", columnDef.getPos());
                }
            }

            if (engineName.equalsIgnoreCase(Table.TableType.MYSQL.name()) && columnDef.getType().isComplexType()) {
                throw new SemanticException(engineName + " external table don't support complex type", columnDef.getPos());
            }

            if (!statement.isOlapEngine() && !engineName.equalsIgnoreCase(Table.TableType.HIVE.name())) {
                columnDef.setIsKey(true);
            }

            if (columnDef.getAggregateType() == AggregateType.HLL_UNION && keysDesc != null
                    && keysDesc.getKeysType() != KeysType.AGG_KEYS) {
                throw new SemanticException("HLL_UNION must be used in AGG_KEYS", keysDesc.getPos());
            }

            if (columnDef.getAggregateType() == AggregateType.BITMAP_UNION && columnDef.getType().isBitmapType()
                    && keysDesc != null && keysDesc.getKeysType() != KeysType.AGG_KEYS) {
                throw new SemanticException("BITMAP_UNION must be used in AGG_KEYS", keysDesc.getPos());
            }

            Column col = columnDef.toColumn(null);
            if (keysDesc != null && (keysDesc.getKeysType() == KeysType.UNIQUE_KEYS
                    || keysDesc.getKeysType() == KeysType.PRIMARY_KEYS ||
                    keysDesc.getKeysType() == KeysType.DUP_KEYS)) {
                if (!col.isKey()) {
                    col.setAggregationTypeImplicit(true);
                }
            }
            columns.add(col);
        }

        statement.setColumns(columns);
    }

    private static void analyzeKeysDesc(CreateTableStmt stmt) {
        KeysDesc keysDesc = stmt.getKeysDesc();
        if (!stmt.isOlapEngine()) {
            // mysql, broker, iceberg, hudi and hive do not need key desc
            if (keysDesc != null) {
                throw new SemanticException("Create " + stmt.getEngineName() + " table should not contain keys desc",
                        keysDesc.getPos());
            }
            return;
        }

        List<ColumnDef> columnDefs = stmt.getColumnDefs();

        if (keysDesc == null) {
            List<String> keysColumnNames = Lists.newArrayList();
            if (columnDefs.stream().anyMatch(c -> c.getAggregateType() != null)) {
                for (ColumnDef columnDef : columnDefs) {
                    if (columnDef.getAggregateType() == null) {
                        keysColumnNames.add(columnDef.getName());
                    }
                }
                keysDesc = new KeysDesc(KeysType.AGG_KEYS, keysColumnNames);
            } else {
                int keyLength = 0;
                for (ColumnDef columnDef : columnDefs) {
                    keyLength += columnDef.getType().getIndexSize();
                    if (keysColumnNames.size() >= FeConstants.SHORTKEY_MAX_COLUMN_COUNT
                            || keyLength > FeConstants.SHORTKEY_MAXSIZE_BYTES) {
                        if (keysColumnNames.size() == 0
                                && columnDef.getType().getPrimitiveType().isCharFamily()) {
                            keysColumnNames.add(columnDef.getName());
                        }
                        break;
                    }
                    if (!columnDef.getType().canDistributedBy()) {
                        break;
                    }
                    if (columnDef.getType().getPrimitiveType() == PrimitiveType.VARCHAR) {
                        keysColumnNames.add(columnDef.getName());
                        break;
                    }
                    keysColumnNames.add(columnDef.getName());
                }
                if (columnDefs.isEmpty()) {
                    throw new SemanticException("Empty schema");
                }
                // The OLAP table must has at least one short key and the float and double should not be short key.
                // So the float and double could not be the first column in OLAP table.
                if (keysColumnNames.isEmpty()) {
                    throw new SemanticException("Data type of first column cannot be %s", columnDefs.get(0).getType());
                }
                keysDesc = new KeysDesc(KeysType.DUP_KEYS, keysColumnNames);
            }
        }

        KeysType keysType = keysDesc.getKeysType();
        if (keysType == null) {
            throw new SemanticException("Keys type is null.");
        }

        List<String> keysColumnNames = keysDesc.getKeysColumnNames();
        if (keysColumnNames == null || keysColumnNames.size() == 0) {
            throw new SemanticException("The number of key columns is 0.");
        }

        if (keysColumnNames.size() > columnDefs.size()) {
            throw new SemanticException("The number of key columns should be less than the number of columns.");
        }

        for (int i = 0; i < keysColumnNames.size(); ++i) {
            String colName = columnDefs.get(i).getName();
            if (!keysColumnNames.get(i).equalsIgnoreCase(colName)) {
                String keyName = keysColumnNames.get(i);
                if (columnDefs.stream().noneMatch(col -> col.getName().equalsIgnoreCase(keyName))) {
                    throw new SemanticException("Key column(%s) doesn't exist.", keysColumnNames.get(i));
                } else {
                    throw new SemanticException("Key columns must be the first few columns of the schema and the order "
                            + " of the key columns must be consistent with the order of the schema");
                }
            }

            if (columnDefs.get(i).getAggregateType() != null) {
                throw new SemanticException("Key column[" + colName + "] should not specify aggregate type.");
            }

            if (keysType == KeysType.PRIMARY_KEYS) {
                ColumnDef cd = columnDefs.get(i);
                cd.setPrimaryKeyNonNullable();
                if (cd.isAllowNull()) {
                    throw new SemanticException("primary key column[" + colName + "] cannot be nullable");
                }
                Type t = cd.getType();
                if (!(t.isBoolean() || t.isIntegerType() || t.isLargeint() || t.isVarchar() || t.isDate() ||
                        t.isDatetime())) {
                    throw new SemanticException("primary key column[" + colName + "] type not supported: " + t.toSql());
                }
            }
        }

        // for olap table
        for (int i = keysColumnNames.size(); i < columnDefs.size(); ++i) {
            if (keysType == KeysType.AGG_KEYS) {
                if (columnDefs.get(i).getAggregateType() == null) {
                    throw new SemanticException(keysType.name() + " table should specify aggregate type for "
                            + "non-key column[" + columnDefs.get(i).getName() + "]");
                }
            } else {
                if (columnDefs.get(i).getAggregateType() != null
                        && columnDefs.get(i).getAggregateType() != AggregateType.REPLACE) {
                    throw new SemanticException(keysType.name() + " table should not specify aggregate type for "
                            + "non-key column[" + columnDefs.get(i).getName() + "]");
                }
            }
        }

        for (int i = 0; i < keysDesc.getKeysColumnNames().size(); ++i) {
            columnDefs.get(i).setIsKey(true);
        }

        if (keysDesc.getKeysType() != KeysType.AGG_KEYS) {
            // note: PRIMARY_KEYS uses REPLACE aggregate type for now
            AggregateType aggregateType = keysDesc.getKeysType() == KeysType.DUP_KEYS ?
                    AggregateType.NONE : AggregateType.REPLACE;
            for (int i = keysDesc.getKeysColumnNames().size(); i < columnDefs.size(); ++i) {
                columnDefs.get(i).setAggregateType(aggregateType);
            }
        }

        stmt.setKeysDesc(keysDesc);
    }

    private static void analyzeSortKeys(CreateTableStmt stmt) {
        if (!stmt.isOlapEngine()) {
            return;
        }

        KeysDesc keysDesc = stmt.getKeysDesc();
        KeysType keysType = keysDesc.getKeysType();

        List<ColumnDef> columnDefs = stmt.getColumnDefs();
        List<String> sortKeys = stmt.getSortKeys();
        List<String> columnNames = columnDefs.stream().map(ColumnDef::getName).collect(Collectors.toList());
        if (sortKeys != null) {
            // we should check sort key column type if table is primary key table
            if (keysType == KeysType.PRIMARY_KEYS) {
                for (String column : sortKeys) {
                    int idx = columnNames.indexOf(column);
                    if (idx == -1) {
                        throw new SemanticException("Unknown column '%s' does not exist", column);
                    }
                    ColumnDef cd = columnDefs.get(idx);
                    Type t = cd.getType();
                    if (!(t.isBoolean() || t.isIntegerType() || t.isLargeint() || t.isVarchar() || t.isDate() ||
                            t.isDatetime())) {
                        throw new SemanticException("sort key column[" + cd.getName() + "] type not supported: " + t.toSql());
                    }
                }
            } else if (keysType == KeysType.DUP_KEYS) {
                // sort key column of duplicate table has no limitation
            } else if (keysType == KeysType.AGG_KEYS || keysType == KeysType.UNIQUE_KEYS) {
                List<Integer> sortKeyIdxes = Lists.newArrayList();
                for (String column : sortKeys) {
                    int idx = columnNames.indexOf(column);
                    if (idx == -1) {
                        throw new SemanticException("Unknown column '%s' does not exist", column);
                    }
                    sortKeyIdxes.add(idx);
                }

                List<Integer> keyColIdxes = Lists.newArrayList();
                for (String column : keysDesc.getKeysColumnNames()) {
                    int idx = columnNames.indexOf(column);
                    if (idx == -1) {
                        throw new SemanticException("Unknown column '%s' does not exist", column);
                    }
                    keyColIdxes.add(idx);
                }

                // sort key column of AGG and UNIQUE table must include all key columns and cannot have any columns other than
                // the key columns
                boolean res = new HashSet<>(keyColIdxes).equals(new HashSet<>(sortKeyIdxes));
                if (!res) {
                    throw new SemanticException("The sort columns of " + keysType.toSql()
                            + " table must be same with key columns");
                }
            } else {
                throw new SemanticException("Table type:" + keysType.toSql() + " does not support sort key column");
            }
        }
    }

    public static void analyzePartitionDesc(CreateTableStmt stmt) {
        String engineName = stmt.getEngineName();
        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        if (stmt.isOlapEngine()) {
            if (partitionDesc != null) {
                if (partitionDesc.getType() == PartitionType.RANGE || partitionDesc.getType() == PartitionType.LIST) {
                    try {
                        PartitionDescAnalyzer.analyze(partitionDesc);
                        partitionDesc.analyze(stmt.getColumnDefs(), stmt.getProperties());
                    } catch (AnalysisException e) {
                        throw new SemanticException(e.getMessage());
                    }
                } else if (partitionDesc instanceof ExpressionPartitionDesc) {
                    ExpressionPartitionDesc expressionPartitionDesc = (ExpressionPartitionDesc) partitionDesc;
                    try {
                        PartitionDescAnalyzer.analyze(partitionDesc);
                        expressionPartitionDesc.analyze(stmt.getColumnDefs(), stmt.getProperties());
                    } catch (AnalysisException e) {
                        throw new SemanticException(e.getMessage());
                    }
                } else {
                    throw new SemanticException("Currently only support range and list partition with engine type olap",
                            partitionDesc.getPos());
                }
            }
        } else {
            if (engineName.equalsIgnoreCase(Table.TableType.ELASTICSEARCH.name())) {
                EsUtil.analyzePartitionDesc(partitionDesc);
            } else if (engineName.equalsIgnoreCase(Table.TableType.ICEBERG.name())
                    || engineName.equalsIgnoreCase(Table.TableType.HIVE.name())) {
                if (partitionDesc != null) {
                    ((ListPartitionDesc) partitionDesc).analyzeExternalPartitionColumns(stmt.getColumnDefs(), engineName);
                }
            } else {
                if (partitionDesc != null) {
                    throw new SemanticException("Create " + engineName + " table should not contain partition desc",
                            partitionDesc.getPos());
                }
            }
        }
    }

    public static void analyzeDistributionDesc(CreateTableStmt stmt) {
        List<ColumnDef> columnDefs = stmt.getColumnDefs();
        DistributionDesc distributionDesc = stmt.getDistributionDesc();
        if (stmt.isOlapEngine()) {
            Map<String, String> properties = stmt.getProperties();
            KeysDesc keysDesc = Preconditions.checkNotNull(stmt.getKeysDesc());

            // analyze distribution
            if (distributionDesc == null) {
                if (properties != null && properties.containsKey("colocate_with")) {
                    throw new SemanticException("Colocate table must specify distribution column");
                }

                if (keysDesc != null && keysDesc.getKeysType() == KeysType.PRIMARY_KEYS) {
                    distributionDesc = new HashDistributionDesc(0, keysDesc.getKeysColumnNames());
                } else if (keysDesc.getKeysType() == KeysType.DUP_KEYS) {
                    // no specified distribution, use random distribution
                    if (ConnectContext.get().getSessionVariable().isAllowDefaultPartition()) {
                        if (properties == null) {
                            properties = Maps.newHashMap();
                            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "1");
                        }
                        distributionDesc = new HashDistributionDesc(0, Lists.newArrayList(columnDefs.get(0).getName()));
                    } else {
                        distributionDesc = new RandomDistributionDesc();
                    }
                } else {
                    throw new SemanticException("Currently not support default distribution in " + keysDesc.getKeysType());
                }
            }
            if (distributionDesc instanceof RandomDistributionDesc && keysDesc.getKeysType() != KeysType.DUP_KEYS
                    && !(keysDesc.getKeysType() == KeysType.AGG_KEYS && !stmt.isHasReplace())) {
                throw new SemanticException(keysDesc.getKeysType().toSql() + (stmt.isHasReplace() ? " with replace " : "")
                        + " must use hash distribution", distributionDesc.getPos());
            }
            if (distributionDesc.getBuckets() > Config.max_bucket_number_per_partition && stmt.isOlapEngine()
                    && stmt.getPartitionDesc() != null && stmt.getPartitionDesc().getType() != PartitionType.UNPARTITIONED) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TOO_MANY_BUCKETS, Config.max_bucket_number_per_partition);
            }
            Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            columnSet.addAll(columnDefs.stream().map(ColumnDef::getName).collect(Collectors.toSet()));
            distributionDesc.analyze(columnSet);
            stmt.setDistributionDesc(distributionDesc);
            stmt.setProperties(properties);
        } else {
            if (stmt.getEngineName().equalsIgnoreCase(Table.TableType.ELASTICSEARCH.name())) {
                EsUtil.analyzeDistributionDesc(distributionDesc);
            } else if (stmt.getEngineName().equalsIgnoreCase(Table.TableType.ICEBERG.name())
                    || stmt.getEngineName().equalsIgnoreCase(Table.TableType.HIVE.name())) {
                // no special analyze
            } else {
                if (distributionDesc != null) {
                    throw new SemanticException("Create " + stmt.getEngineName() + " table should not contain distribution desc",
                            distributionDesc.getPos());
                }
            }
        }
    }

    public static void analyzeGeneratedColumn(CreateTableStmt stmt, ConnectContext context) {
        if (!stmt.isOlapEngine()) {
            throw new SemanticException("Generated Column only support olap table");
        }

        KeysDesc keysDesc = Preconditions.checkNotNull(stmt.getKeysDesc());
        if (keysDesc.getKeysType() == KeysType.AGG_KEYS) {
            throw new SemanticException("Generated Column does not support AGG table");
        }

        if (RunMode.isSharedDataMode()) {
            throw new SemanticException("Does not support generated column in shared data cluster yet");
        }

        final TableName tableNameObject = stmt.getDbTbl();

        List<Column> columns = stmt.getColumns();
        Map<String, Column> columnsMap = Maps.newHashMap();
        for (Column column : columns) {
            columnsMap.put(column.getName(), column);
        }

        boolean found = false;
        for (Column column : columns) {
            if (found && !column.isGeneratedColumn()) {
                throw new SemanticException("All generated columns must be defined after ordinary columns");
            }

            if (column.isGeneratedColumn()) {
                if (keysDesc.containsCol(column.getName())) {
                    throw new SemanticException("Generated Column can not be KEY");
                }

                Expr expr = column.getGeneratedColumnExpr(columns);

                List<DictionaryGetExpr> dictionaryGetExprs = Lists.newArrayList();
                expr.collect(DictionaryGetExpr.class, dictionaryGetExprs);
                if (dictionaryGetExprs.size() != 0) {
                    for (DictionaryGetExpr dictionaryGetExpr : dictionaryGetExprs) {
                        dictionaryGetExpr.setSkipStateCheck(true);
                    }
                }

                ExpressionAnalyzer.analyzeExpression(expr, new AnalyzeState(), new Scope(RelationId.anonymous(),
                        new RelationFields(columns.stream().map(col -> new Field(
                                        col.getName(), col.getType(), tableNameObject, null))
                                .collect(Collectors.toList()))), context);

                // check if contain aggregation
                List<FunctionCallExpr> funcs = Lists.newArrayList();
                expr.collect(FunctionCallExpr.class, funcs);
                for (FunctionCallExpr fn : funcs) {
                    if (fn.isAggregateFunction()) {
                        throw new SemanticException("Generated Column don't support aggregation function");
                    }
                }

                // check if the expression refers to other Generated columns
                List<SlotRef> slots = Lists.newArrayList();
                expr.collect(SlotRef.class, slots);
                if (slots.size() != 0) {
                    for (SlotRef slot : slots) {
                        Column refColumn = columnsMap.get(slot.getColumnName());
                        if (refColumn == null) {
                            throw new SemanticException("column:" + slot.getColumnName() + " does not exist");
                        }
                        if (refColumn.isGeneratedColumn()) {
                            throw new SemanticException("Expression can not refers to other generated columns");
                        }
                        if (refColumn.isAutoIncrement()) {
                            throw new SemanticException("Expression can not refers to AUTO_INCREMENT columns");
                        }
                    }
                }

                if (!column.getType().matchesType(expr.getType())) {
                    throw new SemanticException("Illegal expression type for Generated Column " +
                            "Column Type: " + column.getType().toString() +
                            ", Expression Type: " + expr.getType().toString());
                }

                found = true;
            }
        }
    }

    public static void analyzeIndexDefs(CreateTableStmt statement) {
        List<IndexDef> indexDefs = statement.getIndexDefs();

        List<Column> columns = statement.getColumns();
        KeysDesc keysDesc = statement.getKeysDesc();

        List<Index> indexes = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(indexDefs)) {
            Set<String> distinct = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            Set<List<String>> distinctCol = new HashSet<>();

            for (IndexDef indexDef : indexDefs) {
                indexDef.analyze();
                if (!statement.isOlapEngine()) {
                    throw new SemanticException("index only support in olap engine at current version", indexDef.getPos());
                }
                List<ColumnId> columnIds = new ArrayList<>(indexDef.getColumns().size());
                for (String indexColName : indexDef.getColumns()) {
                    boolean found = false;
                    for (Column column : columns) {
                        if (column.getName().equalsIgnoreCase(indexColName)) {
                            indexDef.checkColumn(column, keysDesc.getKeysType());
                            found = true;
                            columnIds.add(column.getColumnId());
                            break;
                        }
                    }
                    if (!found) {
                        throw new SemanticException(
                                indexDef.getIndexName() + " column does not exist in table. invalid column: " +
                                        indexColName,
                                indexDef.getPos());
                    }
                }
                indexes.add(new Index(indexDef.getIndexName(), columnIds, indexDef.getIndexType(),
                        indexDef.getComment(), indexDef.getProperties()));

                distinct.add(indexDef.getIndexName());
                distinctCol.add(indexDef.getColumns().stream().map(String::toUpperCase).collect(Collectors.toList()));
            }
            if (distinct.size() != indexes.size()) {
                throw new SemanticException("index name must be unique", indexDefs.get(0).getPos());
            }
            if (distinctCol.size() != indexes.size()) {
                throw new SemanticException("same index columns have multiple index name is not allowed",
                        indexDefs.get(0).getPos());
            }
        }

        statement.setIndexes(indexes);
    }
}
