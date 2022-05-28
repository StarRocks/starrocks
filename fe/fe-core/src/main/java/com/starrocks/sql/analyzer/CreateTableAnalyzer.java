// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.DistributionDesc;
import com.starrocks.analysis.HashDistributionDesc;
import com.starrocks.analysis.IndexDef;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.analysis.PartitionDesc;
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
import com.starrocks.sql.common.MetaUtils;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.starrocks.catalog.AggregateType.BITMAP_UNION;
import static com.starrocks.catalog.AggregateType.HLL_UNION;

public class CreateTableAnalyzer {
    private static final Set<String> engineNames;
    private static Set<String> charsetNames;
    private static final String DEFAULT_CHARSET_NAME = "utf8";

    static {
        engineNames = Sets.newHashSet();
        engineNames.add("olap");
        engineNames.add("mysql");
        engineNames.add("broker");
        engineNames.add("elasticsearch");
        engineNames.add("hive");
        engineNames.add("iceberg");
        engineNames.add("hudi");
        engineNames.add("jdbc");
    }

    static {
        charsetNames = Sets.newHashSet();
        charsetNames.add("utf8");
        charsetNames.add("gbk");
    }

    private static void analyzeEngineName(String engineName) {
        if (Strings.isNullOrEmpty(engineName)) {
            engineName = "olap";
        }
        engineName = engineName.toLowerCase();

        if (!engineNames.contains(engineName)) {
            throw new SemanticException("Unknown engine name: %s", engineName);
        }
    }

    private static void analyzeCharsetName(String charsetName) {
        if (Strings.isNullOrEmpty(charsetName)) {
            charsetName = "utf8";
        }
        charsetName = charsetName.toLowerCase();

        if (!charsetNames.contains(charsetName)) {
            throw new SemanticException("Unknown charset name: " + charsetName);
        }
        // be is not supported yet,so Display unsupported information to the user
        if (!charsetName.equals(DEFAULT_CHARSET_NAME)) {
            throw new SemanticException("charset name " + charsetName + " is not supported yet");
        }
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

        final String engineName = statement.getEngineName().toLowerCase();
        analyzeEngineName(engineName);
        analyzeCharsetName(statement.getCharsetName());

        KeysDesc keysDesc = statement.getKeysDesc();
        List<ColumnDef> columnDefs = statement.getColumnDefs();
        PartitionDesc partitionDesc = statement.getPartitionDesc();
        // analyze key desc
        if (!(engineName.equals("mysql") || engineName.equals("broker") ||
                engineName.equals("hive") || engineName.equals("iceberg") ||
                engineName.equals("hudi") || engineName.equals("jdbc"))) {
            // olap table
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
                        if (!columnDef.getType().isKeyType()) {
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
                        throw new SemanticException(
                                "Data type of first column cannot be " + columnDefs.get(0).getType());
                    }
                    keysDesc = new KeysDesc(KeysType.DUP_KEYS, keysColumnNames);
                }
            }

            keysDesc.analyze(columnDefs);
            keysDesc.checkColumnDefs(columnDefs);
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
                throw new SemanticException("Create " + engineName + " table should not contain keys desc");
            }

            for (ColumnDef columnDef : columnDefs) {
                if (engineName.equals("mysql") && columnDef.getType().isComplexType()) {
                    throw new SemanticException(engineName + " external table don't support complex type");
                }
                columnDef.setIsKey(true);
            }
        }

        // analyze column def
        if (columnDefs == null || columnDefs.isEmpty()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_TABLE_MUST_HAVE_COLUMNS);
        }

        int rowLengthBytes = 0;
        boolean hasHll = false;
        boolean hasBitmap = false;
        boolean hasJson = false;
        Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (ColumnDef columnDef : columnDefs) {
            try {
                columnDef.analyze(engineName.equals("olap"));
            } catch (AnalysisException e) {
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

            rowLengthBytes += columnDef.getType().getStorageLayoutBytes();
        }

        if (rowLengthBytes > Config.max_layout_length_per_row && engineName.equals("olap")) {
            throw new SemanticException("The size of a row (" + rowLengthBytes + ") exceed the maximal row size: "
                    + Config.max_layout_length_per_row);
        }

        if (hasHll && keysDesc.getKeysType() != KeysType.AGG_KEYS) {
            throw new SemanticException("HLL_UNION must be used in AGG_KEYS");
        }

        if (hasBitmap && keysDesc.getKeysType() != KeysType.AGG_KEYS) {
            throw new SemanticException("BITMAP_UNION must be used in AGG_KEYS");
        }

        DistributionDesc distributionDesc = statement.getDistributionDesc();
        if (engineName.equals("olap")) {
            // analyze partition
            Map<String, String> properties = statement.getProperties();
            if (partitionDesc != null) {
                if (partitionDesc.getType() == PartitionType.RANGE || partitionDesc.getType() == PartitionType.LIST) {
                    try {
                        partitionDesc.analyze(columnDefs, properties);
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
                    distributionDesc = new HashDistributionDesc(Config.default_bucket_num,
                            Lists.newArrayList(columnDefs.get(0).getName()));
                } else {
                    throw new SemanticException("Create olap table should contain distribution desc");
                }
            }
            distributionDesc.analyze(columnSet);
            statement.setDistributionDesc(distributionDesc);
            statement.setProperties(properties);
        } else if (engineName.equalsIgnoreCase("elasticsearch")) {
            EsUtil.analyzePartitionAndDistributionDesc(partitionDesc, distributionDesc);
        } else {
            if (partitionDesc != null || distributionDesc != null) {
                throw new SemanticException("Create " + engineName
                        + " table should not contain partition or distribution desc");
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
                if (!engineName.equalsIgnoreCase("olap")) {
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
                        throw new SemanticException("BITMAP column does not exist in table. invalid column: "
                                + indexColName);
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
