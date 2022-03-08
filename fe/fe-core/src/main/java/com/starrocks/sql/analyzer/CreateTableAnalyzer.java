// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.DistributionDesc;
import com.starrocks.analysis.IndexDef;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.analysis.PartitionDesc;
import com.starrocks.analysis.RangePartitionDesc;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeNameFormat;
import com.starrocks.external.elasticsearch.EsUtil;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.relation.Relation;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.starrocks.catalog.AggregateType.BITMAP_UNION;

public class CreateTableAnalyzer {

    private final Catalog catalog;
    private final ConnectContext session;
    private static final Set<String> engineNames;

    static {
        engineNames = Sets.newHashSet();
        engineNames.add("olap");
        engineNames.add("mysql");
        engineNames.add("elasticsearch");
        engineNames.add("hive");
    }

    public CreateTableAnalyzer(Catalog catalog, ConnectContext session) {
        this.catalog = catalog;
        this.session = session;
    }

    public Relation transformCreateTableStmt(CreateTableStmt createTableStmt) {
        createTableStmt.setClusterName(session.getClusterName());
        TableName tableName = createTableStmt.getDbTbl();
        analyzeTableName(tableName);

        FeNameFormat.verifyTableName(tableName.getTbl());

        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), tableName.getDb(),
                tableName.getTbl(), PrivPredicate.CREATE)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
        }

        String engineName = createTableStmt.getEngineName().toLowerCase();
        analyzeEngineName(engineName);

        List<ColumnDef> columnDefs = createTableStmt.getColumnDefs();
        KeysDesc keysDesc = createTableStmt.getKeysDesc();
        // analyze key desc
        if (!(engineName.equals("mysql") || engineName.equals("broker") || engineName.equals("hive"))) {
            // olap table
            if (createTableStmt.getKeysDesc() == null) {
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
                        if (columnDef.getType().isFloatingPointType() || columnDef.getType().isComplexType()) {
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
                                "Data type of first column cannot be %s", columnDefs.get(0).getType());
                    }
                    keysDesc = new KeysDesc(KeysType.DUP_KEYS, keysColumnNames);
                }
            }
            createTableStmt.setKeysDesc(keysDesc);

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
        } else {
            // mysql, broker and hive do not need key desc
            if (keysDesc != null) {
                throw new SemanticException("Create %s table should not contain keys desc", engineName);
            }

            for (ColumnDef columnDef : columnDefs) {
                if (engineName.equals("mysql") && columnDef.getType().isComplexType()) {
                    throw new SemanticException("%s external table don't support complex type", engineName);
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
        Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

        boolean isOlap = engineName.equals("olap");
        for (ColumnDef columnDef : columnDefs) {

            try {
                // TODO: convert to new analyzer
                columnDef.analyze(isOlap);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }

            if (columnDef.getType().isHllType()) {
                hasHll = true;
            }

            if (columnDef.getAggregateType() == BITMAP_UNION) {
                hasBitmap = columnDef.getType().isBitmapType();
            }

            if (!columnSet.add(columnDef.getName())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, columnDef.getName());
            }

            rowLengthBytes += columnDef.getType().getStorageLayoutBytes();
        }

        if (rowLengthBytes > Config.max_layout_length_per_row && engineName.equals("olap")) {
            throw new SemanticException("The size of a row (%d) exceed the maximal row size: %d", rowLengthBytes,
                    Config.max_layout_length_per_row);
        }

        if (keysDesc == null) {
            throw new SemanticException("keysDesc should not be null");
        }

        if (hasHll && keysDesc.getKeysType() != KeysType.AGG_KEYS) {
            throw new SemanticException("HLL must be used in AGG_KEYS");
        }

        if (hasBitmap && keysDesc.getKeysType() != KeysType.AGG_KEYS) {
            throw new SemanticException("BITMAP_UNION must be used in AGG_KEYS");
        }

        PartitionDesc partitionDesc = createTableStmt.getPartitionDesc();
        DistributionDesc distributionDesc = createTableStmt.getDistributionDesc();

        if (engineName.equals("olap")) {
            // analyze partition
            if (partitionDesc != null) {
                if (partitionDesc.getType() != PartitionType.RANGE) {
                    throw new SemanticException("Currently only support range partition with engine type olap");
                }

                RangePartitionDesc rangePartitionDesc = (RangePartitionDesc) partitionDesc;
                try {
                    // TODO: convert to new analyzer
                    rangePartitionDesc.analyze(columnDefs, createTableStmt.getProperties());
                } catch (AnalysisException e) {
                    throw new SemanticException(e.getMessage());
                }
            }

            // analyze distribution
            if (distributionDesc == null) {
                throw new SemanticException("Create olap table should contain distribution desc");
            }
            try {
                // TODO: convert to new analyzer
                distributionDesc.analyze(columnSet);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
        } else if (engineName.equalsIgnoreCase("elasticsearch")) {
            try {
                // TODO: convert to new analyzer
                EsUtil.analyzePartitionAndDistributionDesc(partitionDesc, distributionDesc);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
        } else {
            if (partitionDesc != null || distributionDesc != null) {
                throw new SemanticException("Create %s table should not contain partition or distribution desc",
                        engineName);
            }
        }

        for (ColumnDef columnDef : columnDefs) {
            Column col = columnDef.toColumn();
            if (keysDesc != null && (keysDesc.getKeysType() == KeysType.UNIQUE_KEYS
                    || keysDesc.getKeysType() == KeysType.PRIMARY_KEYS || keysDesc.getKeysType() == KeysType.DUP_KEYS)) {
                if (!col.isKey()) {
                    col.setAggregationTypeImplicit(true);
                }
            }
            createTableStmt.getColumns().add(col);
        }

        List<Index> indexes = createTableStmt.getIndexes();
        List<IndexDef> indexDefs = createTableStmt.getIndexDefs();

        if (CollectionUtils.isNotEmpty(indexDefs)) {
            Set<String> distinct = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            Set<List<String>> distinctCol = new HashSet<>();

            for (IndexDef indexDef : indexDefs) {
                analyzeIndexDef(indexDef);
                if (!engineName.equalsIgnoreCase("olap")) {
                    throw new SemanticException("index only support in olap engine at current version.");
                }
                for (String indexColName : indexDef.getColumns()) {
                    boolean found = false;
                    for (Column column : createTableStmt.getColumns()) {
                        if (column.getName().equalsIgnoreCase(indexColName)) {
                            indexDef.verifyColumn(column, createTableStmt.getKeysDesc().getKeysType());
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
        return null;
    }

    private void analyzeIndexDef(IndexDef indexDef) {
        IndexDef.IndexType indexType = indexDef.getIndexType();
        List<String> columns = indexDef.getColumns();
        String indexName = indexDef.getIndexName();
        if (indexType == IndexDef.IndexType.BITMAP) {
            if (columns == null || columns.size() != 1) {
                throw new SemanticException("bitmap index can only apply to a single column.");
            }
            if (Strings.isNullOrEmpty(indexName)) {
                throw new SemanticException("index name cannot be blank.");
            }
            if (indexName.length() > 64) {
                throw new SemanticException("index name too long, the index name length at most is 64.");
            }
            TreeSet<String> distinct = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            distinct.addAll(columns);
            if (columns.size() != distinct.size()) {
                throw new SemanticException("columns of index has duplicated.");
            }
        }
    }


    private Relation analyzeTableName(TableName tableName) {
        String db = tableName.getDb();
        if (Strings.isNullOrEmpty(db)) {
            db = session.getDatabase();
            if (Strings.isNullOrEmpty(db)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            if (Strings.isNullOrEmpty(session.getClusterName())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_CLUSTER_NAME_NULL);
            }
            db = ClusterNamespace.getFullName(session.getClusterName(), db);
        }
        tableName.setDb(db);
        if (Strings.isNullOrEmpty(tableName.getTbl())) {
            throw new SemanticException("Table name is null");
        }
        return null;
    }

    private void analyzeEngineName(String engineName) {
        if (Strings.isNullOrEmpty(engineName)) {
            engineName = "olap";
        }
        engineName = engineName.toLowerCase();

        if (!engineNames.contains(engineName)) {
            throw new SemanticException("Unknown engine name: %s", engineName);
        }
    }
}
