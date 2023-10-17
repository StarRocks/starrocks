// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.IndexDef;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.external.elasticsearch.EsUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.common.MetaUtils;
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

    private static final String DEFAULT_ENGINE_NAME = "olap";

    private static final String ELASTICSEARCH = "elasticsearch";

    public enum EngineType {
        OLAP,
        MYSQL,
        BROKER,
        ELASTICSEARCH,
        HIVE,
        ICEBERG,
        HUDI,
        JDBC,
        STARROCKS,
        FILE;

        public static Set<EngineType> SUPPORT_NOT_NULL_SET = ImmutableSet.of(
                OLAP,
                MYSQL,
                BROKER,
                STARROCKS
        );

        public static boolean supportNotNullColumn(String engineName) {
            return SUPPORT_NOT_NULL_SET.contains(EngineType.valueOf(engineName.toUpperCase()));
        }
    }

    public enum CharsetType {
        UTF8,
        GBK,
    }

    private static String analyzeEngineName(String engineName) {
        if (Strings.isNullOrEmpty(engineName)) {
            return DEFAULT_ENGINE_NAME;
        }

        if (engineName.equalsIgnoreCase(CreateTableStmt.LAKE_ENGINE_NAME) && !Config.use_staros) {
            throw new SemanticException("Engine %s needs 'use_staros = true' config in fe.conf", engineName);
        }

        try {
            return EngineType.valueOf(engineName.toUpperCase()).name();
        } catch (IllegalArgumentException e) {
            throw new SemanticException("Unknown engine name: %s", engineName);
        }
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
        try {
            FeNameFormat.checkTableName(tableName);
        } catch (AnalysisException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_TABLE_NAME, tableName);
        }

        final String engineName = analyzeEngineName(statement.getEngineName()).toLowerCase();
        statement.setEngineName(engineName);
        statement.setCharsetName(analyzeCharsetName(statement.getCharsetName()).toLowerCase());

        KeysDesc keysDesc = statement.getKeysDesc();
        List<Integer> sortKeyIdxes = Lists.newArrayList();
        if (statement.getSortKeys() != null) {
            if (keysDesc == null || keysDesc.getKeysType() != KeysType.PRIMARY_KEYS) {
                throw new IllegalArgumentException("only primary key support sort key");
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
        PartitionDesc partitionDesc = statement.getPartitionDesc();
        // analyze key desc
        if (statement.isOlapOrLakeEngine()) {
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
                        if (keysColumnNames.size() >= FeConstants.shortkey_max_column_count
                                || keyLength > FeConstants.shortkey_maxsize_bytes) {
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

            if (keysDesc.getKeysType() == KeysType.PRIMARY_KEYS && statement.isLakeEngine()) {
                throw new SemanticException("Lake table does not support primary key type");
            }
        } else {
            // mysql, broker, iceberg, hudi and hive do not need key desc
            if (keysDesc != null) {
                throw new SemanticException("Create %s table should not contain keys desc", engineName);
            }

            for (ColumnDef columnDef : columnDefs) {
                if (engineName.equals("mysql") && columnDef.getType().isComplexType()) {
                    throw new SemanticException("%s external table don't support complex type", engineName);
                }

                if (!engineName.equals("hive")) {
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
        boolean hasJson = false;
        Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (ColumnDef columnDef : columnDefs) {
            try {
                columnDef.analyze(statement.isOlapOrLakeEngine(), EngineType.supportNotNullColumn(engineName));
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

            if (columnDef.getType().isJsonType()) {
                hasJson = true;
            }

            if (!columnSet.add(columnDef.getName())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, columnDef.getName());
            }
        }

        if (hasHll && keysDesc.getKeysType() != KeysType.AGG_KEYS) {
            throw new SemanticException("HLL_UNION must be used in AGG_KEYS");
        }

        if (hasBitmap && keysDesc.getKeysType() != KeysType.AGG_KEYS) {
            throw new SemanticException("BITMAP_UNION must be used in AGG_KEYS");
        }

        DistributionDesc distributionDesc = statement.getDistributionDesc();
        if (statement.isOlapOrLakeEngine()) {
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
                    throw new SemanticException(
                            "Currently only support range and list partition with engine type olap");
                }
            }

            // analyze distribution
            if (distributionDesc == null) {
                if (ConnectContext.get().getSessionVariable().isAllowDefaultPartition()) {
                    if (properties == null) {
                        properties = Maps.newHashMap();
                        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "1");
                    }
                    distributionDesc = new HashDistributionDesc(0, Lists.newArrayList(columnDefs.get(0).getName()));
                } else {
                    throw new SemanticException("Create olap table should contain distribution desc");
                }
            }
            distributionDesc.analyze(columnSet);
            statement.setDistributionDesc(distributionDesc);
            statement.setProperties(properties);
        } else {
            if (engineName.equals(ELASTICSEARCH)) {
                EsUtil.analyzePartitionAndDistributionDesc(partitionDesc, distributionDesc);
            } else {
                if (partitionDesc != null || distributionDesc != null) {
                    throw new SemanticException("Create %s table should not contain partition or distribution desc",
                            engineName);
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
        List<IndexDef> indexDefs = statement.getIndexDefs();
        if (CollectionUtils.isNotEmpty(indexDefs)) {
            Set<String> distinct = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            Set<List<String>> distinctCol = new HashSet<>();

            for (IndexDef indexDef : indexDefs) {
                indexDef.analyze();
                if (!statement.isOlapOrLakeEngine()) {
                    throw new SemanticException("index only support in olap engine at current version.");
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
                        throw new SemanticException("BITMAP column does not exist in table. invalid column: %s",
                                indexColName);
                    }
                }
                indexes.add(new Index(indexDef.getIndexName(), indexDef.getColumns(), indexDef.getIndexType(),
                        indexDef.getComment()));
                distinct.add(indexDef.getIndexName());
                distinctCol.add(indexDef.getColumns().stream().map(String::toUpperCase).collect(Collectors.toList()));
            }
            if (distinct.size() != indexes.size()) {
                throw new SemanticException("index name must be unique.");
            }
            if (distinctCol.size() != indexes.size()) {
                throw new SemanticException("same index columns have multiple index name is not allowed.");
            }
        }
    }

}
