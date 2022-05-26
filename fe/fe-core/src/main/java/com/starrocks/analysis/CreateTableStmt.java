// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/CreateTableStmt.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Catalog;
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
import com.starrocks.common.UserException;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.external.elasticsearch.EsUtil;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import org.apache.commons.collections.CollectionUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import static com.starrocks.catalog.AggregateType.BITMAP_UNION;

public class CreateTableStmt extends DdlStmt {

    private static final String DEFAULT_ENGINE_NAME = "olap";

    private boolean ifNotExists;
    private boolean isExternal;
    private TableName tableName;
    private List<ColumnDef> columnDefs;
    private List<IndexDef> indexDefs;
    private KeysDesc keysDesc;
    private PartitionDesc partitionDesc;
    private DistributionDesc distributionDesc;
    private Map<String, String> properties;
    private Map<String, String> extProperties;
    private String engineName;
    private String comment;
    private List<AlterClause> rollupAlterClauseList;

    private static Set<String> engineNames;

    // set in analyze
    private List<Column> columns = Lists.newArrayList();

    private List<Index> indexes = Lists.newArrayList();

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

    // for backup. set to -1 for normal use
    private int tableSignature;

    public CreateTableStmt() {
        // for persist
        tableName = new TableName();
        columnDefs = Lists.newArrayList();
    }

    public CreateTableStmt(boolean ifNotExists,
                           boolean isExternal,
                           TableName tableName,
                           List<ColumnDef> columnDefinitions,
                           String engineName,
                           KeysDesc keysDesc,
                           PartitionDesc partitionDesc,
                           DistributionDesc distributionDesc,
                           Map<String, String> properties,
                           Map<String, String> extProperties,
                           String comment) {
        this(ifNotExists, isExternal, tableName, columnDefinitions, null, engineName, keysDesc, partitionDesc,
                distributionDesc, properties, extProperties, comment, null);
    }

    public CreateTableStmt(boolean ifNotExists,
                           boolean isExternal,
                           TableName tableName,
                           List<ColumnDef> columnDefinitions,
                           String engineName,
                           KeysDesc keysDesc,
                           PartitionDesc partitionDesc,
                           DistributionDesc distributionDesc,
                           Map<String, String> properties,
                           Map<String, String> extProperties,
                           String comment, List<AlterClause> ops) {
        this(ifNotExists, isExternal, tableName, columnDefinitions, null, engineName, keysDesc, partitionDesc,
                distributionDesc, properties, extProperties, comment, ops);
    }

    public CreateTableStmt(boolean ifNotExists,
                           boolean isExternal,
                           TableName tableName,
                           List<ColumnDef> columnDefinitions,
                           List<IndexDef> indexDefs,
                           String engineName,
                           KeysDesc keysDesc,
                           PartitionDesc partitionDesc,
                           DistributionDesc distributionDesc,
                           Map<String, String> properties,
                           Map<String, String> extProperties,
                           String comment, List<AlterClause> rollupAlterClauseList) {
        this.tableName = tableName;
        if (columnDefinitions == null) {
            this.columnDefs = Lists.newArrayList();
        } else {
            this.columnDefs = columnDefinitions;
        }
        this.indexDefs = indexDefs;
        if (Strings.isNullOrEmpty(engineName)) {
            this.engineName = DEFAULT_ENGINE_NAME;
        } else {
            this.engineName = engineName;
        }

        this.keysDesc = keysDesc;
        this.partitionDesc = partitionDesc;
        this.distributionDesc = distributionDesc;
        this.properties = properties;
        this.extProperties = extProperties;
        this.isExternal = isExternal;
        this.ifNotExists = ifNotExists;
        this.comment = Strings.nullToEmpty(comment);

        this.tableSignature = -1;
        this.rollupAlterClauseList = rollupAlterClauseList == null ? new ArrayList<>() : rollupAlterClauseList;
    }

    public void addColumnDef(ColumnDef columnDef) {
        columnDefs.add(columnDef);
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists() {
        this.ifNotExists = true;
    }

    public boolean isExternal() {
        return isExternal;
    }

    public TableName getDbTbl() {
        return tableName;
    }

    public String getTableName() {
        return tableName.getTbl();
    }

    public List<Column> getColumns() {
        return this.columns;
    }

    public KeysDesc getKeysDesc() {
        return this.keysDesc;
    }

    public PartitionDesc getPartitionDesc() {
        return this.partitionDesc;
    }

    public DistributionDesc getDistributionDesc() {
        return this.distributionDesc;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    public Map<String, String> getExtProperties() {
        return this.extProperties;
    }

    public String getEngineName() {
        return engineName;
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public void setTableSignature(int tableSignature) {
        this.tableSignature = tableSignature;
    }

    public int getTableSignature() {
        return tableSignature;
    }

    public void setTableName(String newTableName) {
        tableName = new TableName(tableName.getDb(), newTableName);
    }

    public String getComment() {
        return comment;
    }

    public List<AlterClause> getRollupAlterClauseList() {
        return rollupAlterClauseList;
    }

    public List<Index> getIndexes() {
        return indexes;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        tableName.analyze(analyzer);
        FeNameFormat.checkTableName(tableName.getTbl());

        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), tableName.getDb(),
                tableName.getTbl(), PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
        }

        analyzeEngineName();

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
                        throw new AnalysisException("Empty schema");
                    }
                    // The OLAP table must has at least one short key and the float and double should not be short key.
                    // So the float and double could not be the first column in OLAP table.
                    if (keysColumnNames.isEmpty()) {
                        throw new AnalysisException(
                                "Data type of first column cannot be " + columnDefs.get(0).getType());
                    }
                    keysDesc = new KeysDesc(KeysType.DUP_KEYS, keysColumnNames);
                }
            }

            keysDesc.analyze(columnDefs);
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
            // mysql, broker, iceberg, hudi and hive do not need key desc
            if (keysDesc != null) {
                throw new AnalysisException("Create " + engineName + " table should not contain keys desc");
            }

            for (ColumnDef columnDef : columnDefs) {
                if (engineName.equals("mysql") && columnDef.getType().isComplexType()) {
                    throw new AnalysisException(engineName + " external table don't support complex type");
                }
                columnDef.setIsKey(true);
            }
        }

        // analyze column def
        if (columnDefs == null || columnDefs.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_MUST_HAVE_COLUMNS);
        }

        int rowLengthBytes = 0;
        boolean hasHll = false;
        boolean hasBitmap = false;
        boolean hasJson = false;
        Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (ColumnDef columnDef : columnDefs) {
            columnDef.analyze(engineName.equals("olap"));

            if (columnDef.getType().isHllType()) {
                hasHll = true;
            }

            if (columnDef.getAggregateType() == BITMAP_UNION) {
                hasBitmap = columnDef.getType().isBitmapType();
            }

            if (columnDef.getType().isJsonType()) {
                hasJson = true;
            }

            if (!columnSet.add(columnDef.getName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, columnDef.getName());
            }

            rowLengthBytes += columnDef.getType().getStorageLayoutBytes();
        }

        if (rowLengthBytes > Config.max_layout_length_per_row && engineName.equals("olap")) {
            throw new AnalysisException("The size of a row (" + rowLengthBytes + ") exceed the maximal row size: "
                    + Config.max_layout_length_per_row);
        }

        if (hasHll && keysDesc.getKeysType() != KeysType.AGG_KEYS) {
            throw new AnalysisException("HLL must be used in AGG_KEYS");
        }

        if (hasBitmap && keysDesc.getKeysType() != KeysType.AGG_KEYS) {
            throw new AnalysisException("BITMAP_UNION must be used in AGG_KEYS");
        }

        if (engineName.equals("olap")) {
            // analyze partition
            if (partitionDesc != null) {
                if (partitionDesc.getType() != PartitionType.RANGE) {
                    throw new AnalysisException("Currently only support range partition with engine type olap");
                }

                RangePartitionDesc rangePartitionDesc = (RangePartitionDesc) partitionDesc;
                rangePartitionDesc.analyze(columnDefs, properties);
            }

            // analyze distribution
            if (distributionDesc == null) {
                throw new AnalysisException("Create olap table should contain distribution desc");
            }
            distributionDesc.analyze(columnSet);
        } else if (engineName.equalsIgnoreCase("elasticsearch")) {
            EsUtil.analyzePartitionAndDistributionDesc(partitionDesc, distributionDesc);
        } else {
            if (partitionDesc != null || distributionDesc != null) {
                throw new AnalysisException("Create " + engineName
                        + " table should not contain partition or distribution desc");
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
            columns.add(col);
        }

        if (CollectionUtils.isNotEmpty(indexDefs)) {
            Set<String> distinct = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            Set<List<String>> distinctCol = new HashSet<>();

            for (IndexDef indexDef : indexDefs) {
                indexDef.analyze();
                if (!engineName.equalsIgnoreCase("olap")) {
                    throw new AnalysisException("index only support in olap engine at current version.");
                }
                for (String indexColName : indexDef.getColumns()) {
                    boolean found = false;
                    for (Column column : columns) {
                        if (column.getName().equalsIgnoreCase(indexColName)) {
                            indexDef.checkColumn(column, getKeysDesc().getKeysType());
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw new AnalysisException("BITMAP column does not exist in table. invalid column: "
                                + indexColName);
                    }
                }
                indexes.add(new Index(indexDef.getIndexName(), indexDef.getColumns(), indexDef.getIndexType(),
                        indexDef.getComment()));
                distinct.add(indexDef.getIndexName());
                distinctCol.add(indexDef.getColumns().stream().map(String::toUpperCase).collect(Collectors.toList()));
            }
            if (distinct.size() != indexes.size()) {
                throw new AnalysisException("index name must be unique.");
            }
            if (distinctCol.size() != indexes.size()) {
                throw new AnalysisException("same index columns have multiple index name is not allowed.");
            }
        }
    }

    public List<ColumnDef> getColumnDefs() {
        return columnDefs;
    }

    public List<IndexDef> getIndexDefs() {
        return indexDefs;
    }

    public void setKeysDesc(KeysDesc keysDesc) {
        this.keysDesc = keysDesc;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void setDistributionDesc(DistributionDesc distributionDesc) {
        this.distributionDesc = distributionDesc;
    }

    private void analyzeEngineName() throws AnalysisException {
        if (Strings.isNullOrEmpty(engineName)) {
            engineName = "olap";
        }
        engineName = engineName.toLowerCase();

        if (!engineNames.contains(engineName)) {
            throw new AnalysisException("Unknown engine name: " + engineName);
        }
    }

    public static CreateTableStmt read(DataInput in) throws IOException {
        throw new RuntimeException("CreateTableStmt serialization is not supported anymore.");
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE ");
        if (isExternal) {
            sb.append("EXTERNAL ");
        }
        sb.append("TABLE ");
        sb.append(tableName.toSql()).append(" (\n");
        int idx = 0;
        for (ColumnDef columnDef : columnDefs) {
            if (idx != 0) {
                sb.append(",\n");
            }
            sb.append("  ").append(columnDef.toSql());
            idx++;
        }
        if (CollectionUtils.isNotEmpty(indexDefs)) {
            sb.append(",\n");
            for (IndexDef indexDef : indexDefs) {
                sb.append("  ").append(indexDef.toSql());
            }
        }
        sb.append("\n)");
        if (engineName != null) {
            sb.append(" ENGINE = ").append(engineName);
        }

        if (keysDesc != null) {
            sb.append("\n").append(keysDesc.toSql());
        }

        if (partitionDesc != null) {
            sb.append("\n").append(partitionDesc.toSql());
        }

        if (distributionDesc != null) {
            sb.append("\n").append(distributionDesc.toSql());
        }

        if (rollupAlterClauseList != null && rollupAlterClauseList.size() != 0) {
            sb.append("\n rollup(");
            StringBuilder opsSb = new StringBuilder();
            for (int i = 0; i < rollupAlterClauseList.size(); i++) {
                opsSb.append(rollupAlterClauseList.get(i).toSql());
                if (i != rollupAlterClauseList.size() - 1) {
                    opsSb.append(",");
                }
            }
            sb.append(opsSb.toString().replace("ADD ROLLUP", "")).append(")");
        }

        // properties may contains password and other sensitive information,
        // so do not print properties.
        // This toSql() method is only used for log, user can see detail info by using show create table stmt,
        // which is implemented in Catalog.getDdlStmt()
        if (properties != null && !properties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<String, String>(properties, " = ", true, true, true));
            sb.append(")");
        }

        if (extProperties != null && !extProperties.isEmpty()) {
            sb.append("\n").append(engineName.toUpperCase()).append(" PROPERTIES (");
            sb.append(new PrintableMap<String, String>(extProperties, " = ", true, true, true));
            sb.append(")");
        }

        if (!Strings.isNullOrEmpty(comment)) {
            sb.append("\nCOMMENT \"").append(comment).append("\"");
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public boolean needAuditEncryption() {
        return !engineName.equals("olap");
    }
}
