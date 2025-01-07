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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IndexDef;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.connector.elasticsearch.EsUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.RandomDistributionDesc;
import com.starrocks.sql.common.EngineType;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.parser.NodePosition;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.starrocks.catalog.AggregateType.BITMAP_UNION;
import static com.starrocks.catalog.AggregateType.HLL_UNION;

public class CreateTableAnalyzer {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableAnalyzer.class);

    private static final String DEFAULT_CHARSET_NAME = "utf8";

    private static final String UNIFIED = "unified";
    private static final String ELASTICSEARCH = "elasticsearch";
    private static final String ICEBERG = "iceberg";
    private static final String HIVE = "hive";

    public enum CharsetType {
        UTF8,
        GBK,
    }

    protected static String analyzeEngineName(String engineName, String catalogName) {
        if (CatalogMgr.isInternalCatalog(catalogName)) {
            if (Strings.isNullOrEmpty(engineName)) {
                return EngineType.defaultEngine().name();
            } else {
                try {
                    return EngineType.valueOf(engineName.toUpperCase()).name();
                } catch (IllegalArgumentException e) {
                    throw new SemanticException("Unknown engine name: %s", engineName);
                }
            }
        }

        String catalogType = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogType(catalogName);
        if (catalogType.equalsIgnoreCase(UNIFIED)) {
            if (Strings.isNullOrEmpty(engineName)) {
                throw new SemanticException("Create table in unified catalog requires engine clause " +
                        "(ENGINE = ENGINE_NAME)");
            }
            return engineName;
        }

        if (Strings.isNullOrEmpty(engineName)) {
            return catalogType; // use catalog type if engine is not specified
        }

        if (!engineName.equalsIgnoreCase(catalogType)) {
            throw new SemanticException("Can't create %s table in the %s catalog", engineName, catalogType);
        }

        return engineName;
    }

    private static String analyzeCharsetName(String charsetName) {
        if (Strings.isNullOrEmpty(charsetName)) {
            return DEFAULT_CHARSET_NAME;
        }
        try {
            CharsetType.valueOf(charsetName.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new SemanticException("Unknown charset name: %s", charsetName);
        }
        // be is not supported yet,so Display unsupported information to the user
        if (!charsetName.equalsIgnoreCase(DEFAULT_CHARSET_NAME)) {
            throw new SemanticException("charset name %s is not supported yet", charsetName);
        }
        return charsetName;
    }

    public static void analyze(CreateTableStmt statement, ConnectContext context) {
        final TableName tableNameObject = statement.getDbTbl();
        MetaUtils.normalizationTableName(context, tableNameObject);

        final String tableName = tableNameObject.getTbl();
        FeNameFormat.checkTableName(tableName);

        final String catalogName = tableNameObject.getCatalog();
        try {
            MetaUtils.checkCatalogExistAndReport(catalogName);
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }

        Database db = MetaUtils.getDatabase(catalogName, tableNameObject.getDb());

        // check if table exists in db
        db.readLock();
        try {
            if (db.getTable(tableName) != null && !statement.isSetIfNotExists()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }
        } finally {
            db.readUnlock();
        }

        final String engineName = analyzeEngineName(statement.getEngineName(), catalogName).toLowerCase();
        statement.setEngineName(engineName);
        statement.setCharsetName(analyzeCharsetName(statement.getCharsetName()).toLowerCase());

        KeysDesc keysDesc = statement.getKeysDesc();
        List<Integer> sortKeyIdxes = Lists.newArrayList();
        if (statement.getSortKeys() != null) {
            if (keysDesc == null || keysDesc.getKeysType() != KeysType.PRIMARY_KEYS) {
                NodePosition keysPos = NodePosition.ZERO;
                if (keysDesc != null) {
                    keysPos = keysDesc.getPos();
                }
                throw new SemanticException("only primary key support sort key", keysPos);
            } else {
                List<String> columnNames =
                        statement.getColumnDefs().stream().map(ColumnDef::getName).collect(Collectors.toList());

                for (String column : statement.getSortKeys()) {
                    int idx = columnNames.indexOf(column);
                    if (idx == -1) {
                        throw new SemanticException("Invalid column '%s' not exists in all columns. '%s', '%s'", column);
                    }
                    sortKeyIdxes.add(idx);
                }
            }
        }
        List<ColumnDef> columnDefs = statement.getColumnDefs();
        int autoIncrementColumnCount = 0;
        for (ColumnDef colDef : columnDefs) {
            if (colDef.isAutoIncrement()) {
                autoIncrementColumnCount++;
                if (colDef.getType() != Type.BIGINT) {
                    throw new SemanticException("The AUTO_INCREMENT column must be BIGINT", colDef.getPos());
                }
            }

            if (autoIncrementColumnCount > 1) {
                throw new SemanticException("More than one AUTO_INCREMENT column defined in CREATE TABLE Statement",
                        colDef.getPos());
            }
        }
        PartitionDesc partitionDesc = statement.getPartitionDesc();
        // analyze key desc
        if (statement.isOlapEngine()) {
            // olap table or lake table
            if (keysDesc == null) {
                List<String> keysColumnNames = Lists.newArrayList();
                int keyLength = 0;
                boolean hasAggregate = false;
                for (ColumnDef columnDef : columnDefs) {
                    if (columnDef.getAggregateType() != null) {
                        hasAggregate = true;
                        break;
                    }
                }
                if (hasAggregate) {
                    for (ColumnDef columnDef : columnDefs) {
                        if (columnDef.getAggregateType() == null) {
                            keysColumnNames.add(columnDef.getName());
                        }
                    }
                    keysDesc = new KeysDesc(KeysType.AGG_KEYS, keysColumnNames);
                } else {
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
                        throw new SemanticException("Data type of first column cannot be %s",
                                columnDefs.get(0).getType());
                    }
                    keysDesc = new KeysDesc(KeysType.DUP_KEYS, keysColumnNames);
                }
            }

            keysDesc.analyze(columnDefs);
            keysDesc.checkColumnDefs(columnDefs, sortKeyIdxes);
            for (int i = 0; i < keysDesc.keysColumnSize(); ++i) {
                columnDefs.get(i).setIsKey(true);
            }
            if (keysDesc.getKeysType() != KeysType.AGG_KEYS) {
                AggregateType type = AggregateType.REPLACE;
                // note: PRIMARY_KEYS uses REPLACE aggregate type for now
                if (keysDesc.getKeysType() == KeysType.DUP_KEYS) {
                    type = AggregateType.NONE;
                }
                for (int i = keysDesc.keysColumnSize(); i < columnDefs.size(); ++i) {
                    columnDefs.get(i).setAggregateType(type);
                }
            }
            statement.setKeysDesc(keysDesc);
        } else {
            // mysql, broker, iceberg, hudi and hive do not need key desc
            if (keysDesc != null) {
                throw new SemanticException("Create " + engineName + " table should not contain keys desc", keysDesc.getPos());
            }

            for (ColumnDef columnDef : columnDefs) {
                if (engineName.equalsIgnoreCase("mysql") && columnDef.getType().isComplexType()) {
                    throw new SemanticException(engineName + " external table don't support complex type", columnDef.getPos());
                }

                if (!engineName.equalsIgnoreCase("hive")) {
                    columnDef.setIsKey(true);
                }
            }
        }

        // analyze column def
        if (columnDefs == null || columnDefs.isEmpty()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_TABLE_MUST_HAVE_COLUMNS);
        }

        boolean hasHll = false;
        boolean hasBitmap = false;
        boolean hasReplace = false;
        Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (ColumnDef columnDef : columnDefs) {
            try {
                columnDef.analyze(statement.isOlapEngine(), CatalogMgr.isInternalCatalog(catalogName), engineName);
            } catch (AnalysisException e) {
                LOGGER.error("Column definition analyze failed.", e);
                throw new SemanticException(e.getMessage());
            }

            if (columnDef.getAggregateType() == HLL_UNION) {
                hasHll = true;
            }

            if (columnDef.getAggregateType() == BITMAP_UNION) {
                hasBitmap = columnDef.getType().isBitmapType();
            }

            if (columnDef.getAggregateType() != null && columnDef.getAggregateType().isReplaceFamily()) {
                hasReplace = true;
            }

            if (!columnSet.add(columnDef.getName())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, columnDef.getName());
            }
        }

        if (hasHll && keysDesc.getKeysType() != KeysType.AGG_KEYS) {
            throw new SemanticException("HLL_UNION must be used in AGG_KEYS", keysDesc.getPos());
        }

        if (hasBitmap && keysDesc.getKeysType() != KeysType.AGG_KEYS) {
            throw new SemanticException("BITMAP_UNION must be used in AGG_KEYS", keysDesc.getPos());
        }

        DistributionDesc distributionDesc = statement.getDistributionDesc();
        if (statement.isOlapEngine()) {
            // analyze partition
            Map<String, String> properties = statement.getProperties();
            if (partitionDesc != null) {
                if (partitionDesc.getType() == PartitionType.RANGE || partitionDesc.getType() == PartitionType.LIST) {
                    try {
                        partitionDesc.analyze(columnDefs, properties);
                    } catch (AnalysisException e) {
                        throw new SemanticException(e.getMessage());
                    }
                } else if (partitionDesc instanceof ExpressionPartitionDesc) {
                    ExpressionPartitionDesc expressionPartitionDesc = (ExpressionPartitionDesc) partitionDesc;
                    try {
                        expressionPartitionDesc.analyze(columnDefs, properties);
                    } catch (AnalysisException e) {
                        throw new SemanticException(e.getMessage());
                    }
                } else {
                    throw new SemanticException("Currently only support range and list partition with engine type olap",
                            partitionDesc.getPos());
                }
            }

            // analyze distribution
            if (distributionDesc == null) {
                if (keysDesc.getKeysType() != KeysType.DUP_KEYS) {
                    throw new SemanticException("Currently only support default distribution in DUP_KEYS");
                }
                if (ConnectContext.get().getSessionVariable().isAllowDefaultPartition()) {
                    if (properties == null) {
                        properties = Maps.newHashMap();
                        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "1");
                    }
                    distributionDesc = new HashDistributionDesc(0, Lists.newArrayList(columnDefs.get(0).getName()));
                } else {
                    distributionDesc = new RandomDistributionDesc();
                }
            }
            if (distributionDesc instanceof RandomDistributionDesc && keysDesc.getKeysType() != KeysType.DUP_KEYS
                    && !(keysDesc.getKeysType() == KeysType.AGG_KEYS && !hasReplace)) {
                throw new SemanticException(keysDesc.getKeysType().toSql() + (hasReplace ? " with replace " : "")
                        + " must use hash distribution", distributionDesc.getPos());
            }
            distributionDesc.analyze(columnSet);
            statement.setDistributionDesc(distributionDesc);
            statement.setProperties(properties);
        } else {
            if (engineName.equalsIgnoreCase(ELASTICSEARCH)) {
                EsUtil.analyzePartitionAndDistributionDesc(partitionDesc, distributionDesc);
            } else if (engineName.equalsIgnoreCase(ICEBERG) || engineName.equalsIgnoreCase(HIVE)) {
                if (partitionDesc != null) {
                    ((ListPartitionDesc) partitionDesc).analyzeExternalPartitionColumns(columnDefs, engineName);
                }
            } else {
                if (partitionDesc != null || distributionDesc != null) {
                    NodePosition pos = NodePosition.ZERO;
                    if (partitionDesc != null) {
                        pos = partitionDesc.getPos();
                    }

                    if (distributionDesc != null) {
                        pos = distributionDesc.getPos();
                    }

                    throw new SemanticException("Create " + engineName + " table should not contain partition " +
                            "or distribution desc", pos);
                }
            }
        }
        List<Column> columns = statement.getColumns();
        List<Index> indexes = statement.getIndexes();
        for (ColumnDef columnDef : columnDefs) {
            Column col = columnDef.toColumn();
            if (keysDesc != null && (keysDesc.getKeysType() == KeysType.UNIQUE_KEYS
                    || keysDesc.getKeysType() == KeysType.PRIMARY_KEYS ||
                    keysDesc.getKeysType() == KeysType.DUP_KEYS)) {
                if (!col.isKey()) {
                    col.setAggregationTypeImplicit(true);
                }
            }
            columns.add(col);
        }
        boolean hasGeneratedColumn = false;
        for (Column column : columns) {
            if (column.isGeneratedColumn()) {
                hasGeneratedColumn = true;
                break;
            }
        }

<<<<<<< HEAD
        if (hasGeneratedColumn && !statement.isOlapEngine()) {
=======
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

    public static void analyzeMultiExprsPartition(CreateTableStmt stmt, TableName tableName) {
        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        if (partitionDesc == null || !(partitionDesc instanceof ListPartitionDesc)) {
            return;
        }
        ListPartitionDesc listPartitionDesc = (ListPartitionDesc) partitionDesc;
        if (!listPartitionDesc.isAutoPartitionTable()) {
            return;
        }
        List<ParseNode> multiDescList = listPartitionDesc.getMultiDescList();
        if (multiDescList == null || multiDescList.isEmpty()) {
            return;
        }
        List<ColumnDef> columnDefs = stmt.getColumnDefs();
        List<String> partitionColumnList = Lists.newArrayList();
        List<PartitionDesc> partitionDescList = Lists.newArrayList();
        List<Expr> partitionExprs = Lists.newArrayList();
        int placeHolderSlotId = 0;
        for (ParseNode partitionExpr : multiDescList) {
            if (partitionExpr instanceof Identifier) {
                Identifier identifier = (Identifier) partitionExpr;
                partitionColumnList.add(identifier.getValue());
            }
            if (partitionExpr instanceof FunctionCallExpr) {
                FunctionCallExpr expr = (FunctionCallExpr) (((Expr) partitionExpr).clone());
                ExpressionAnalyzer.analyzeExpression(expr, new AnalyzeState(), new Scope(RelationId.anonymous(),
                                new RelationFields(columnDefs.stream().map(col -> new Field(col.getName(),
                                        col.getType(), null, null)).collect(Collectors.toList()))),
                        ConnectContext.buildInner());
                String columnName = FeConstants.GENERATED_PARTITION_COLUMN_PREFIX + placeHolderSlotId++;
                partitionColumnList.add(columnName);
                Type type = expr.getType();
                if (type.isScalarType()) {
                    ScalarType scalarType = (ScalarType) type;
                    if (scalarType.isWildcardChar()) {
                        type = ScalarType.createCharType(ScalarType.getOlapMaxVarcharLength());
                    } else if (scalarType.isWildcardVarchar()) {
                        type = ScalarType.createVarcharType(ScalarType.getOlapMaxVarcharLength());
                    }
                }
                TypeDef typeDef = new TypeDef(type);
                try {
                    typeDef.analyze();
                } catch (Exception e) {
                    throw new SemanticException("Generate partition column " + columnName
                            + " for multi expression partition error: " + e.getMessage(), partitionDesc.getPos());
                }
                // generated column expression should be saved in unanalyzed way in meta
                ColumnDef generatedPartitionColumn = new ColumnDef(
                        columnName, typeDef, null, false, null, null, true,
                        ColumnDef.DefaultValueDef.NOT_SET, null, (FunctionCallExpr) partitionExpr, "");
                columnDefs.add(generatedPartitionColumn);
                partitionExprs.add((FunctionCallExpr) partitionExpr);
            }
        }
        for (ColumnDef columnDef : columnDefs) {
            if (partitionColumnList.contains(columnDef.getName())) {
                columnDef.setIsPartitionColumn(true);
            }
        }
        listPartitionDesc = new ListPartitionDesc(partitionColumnList, partitionDescList);
        listPartitionDesc.setAutoPartitionTable(true);
        listPartitionDesc.setPartitionExprs(partitionExprs);
        stmt.setPartitionDesc(listPartitionDesc);
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
                    if (partitionDesc instanceof ListPartitionDesc) {
                        ListPartitionDesc listPartitionDesc = (ListPartitionDesc) partitionDesc;
                        if (listPartitionDesc.getPartitionExprs() != null && !listPartitionDesc.getPartitionExprs().isEmpty()
                                && (stmt.getKeysDesc().getKeysType() == KeysType.AGG_KEYS
                                || stmt.getKeysDesc().getKeysType() == KeysType.UNIQUE_KEYS)) {
                            throw new SemanticException("expression partition base on generated column"
                                    + " doest not support AGG_KEYS or UNIQUE_KEYS", partitionDesc.getPos());
                        }
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
>>>>>>> 6a0fd5dd7b ([BugFix] Build a ConnectContext for inner query which is used for StarRocks internal query (#54737))
            throw new SemanticException("Generated Column only support olap table");
        }

        if (hasGeneratedColumn && keysDesc.getKeysType() == KeysType.AGG_KEYS) {
            throw new SemanticException("Generated Column does not support AGG table");
        }

        Map<String, Column> columnsMap = Maps.newHashMap();
        for (Column column : columns) {
            columnsMap.put(column.getName(), column);
            if (column.isGeneratedColumn() && keysDesc.containsCol(column.getName())) {
                throw new SemanticException("Generated Column can not be KEY");
            }
        }

        if (hasGeneratedColumn) {
            if (!statement.isOlapEngine()) {
                throw new SemanticException("Generated Column only support olap table");
            }

            if (RunMode.isSharedDataMode()) {
                throw new SemanticException("Does not support generated column in shared data cluster yet");
            }

            boolean found = false;
            for (Column column : columns) {
                if (found && !column.isGeneratedColumn()) {
                    throw new SemanticException("All generated columns must be defined after ordinary columns");
                }

                if (column.isGeneratedColumn()) {
                    Expr expr = column.generatedColumnExpr();

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
                        throw new SemanticException("Illege expression type for Generated Column " +
                                "Column Type: " + column.getType().toString() +
                                ", Expression Type: " + expr.getType().toString());
                    }

                    found = true;
                }
            }
        }
        List<IndexDef> indexDefs = statement.getIndexDefs();
        if (CollectionUtils.isNotEmpty(indexDefs)) {
            Set<String> distinct = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            Set<List<String>> distinctCol = new HashSet<>();

            for (IndexDef indexDef : indexDefs) {
                indexDef.analyze();
                if (!statement.isOlapEngine()) {
                    throw new SemanticException("index only support in olap engine at current version", indexDef.getPos());
                }
                for (String indexColName : indexDef.getColumns()) {
                    boolean found = false;
                    for (Column column : columns) {
                        if (column.getName().equalsIgnoreCase(indexColName)) {
                            indexDef.checkColumn(column, keysDesc.getKeysType());
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw new SemanticException("BITMAP column does not exist in table. invalid column: " + indexColName,
                                indexDef.getPos());
                    }
                }
                indexes.add(new Index(indexDef.getIndexName(), indexDef.getColumns(), indexDef.getIndexType(),
                        indexDef.getComment()));
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
    }
}
