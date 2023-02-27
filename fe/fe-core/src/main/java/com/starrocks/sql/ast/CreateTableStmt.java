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


package com.starrocks.sql.ast;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.IndexDef;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Index;
import com.starrocks.sql.common.EngineType;
import com.starrocks.sql.parser.NodePosition;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateTableStmt extends DdlStmt {
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
    private String charsetName;
    private String comment;
    private List<AlterClause> rollupAlterClauseList;

    // set in analyze
    private List<Column> columns = Lists.newArrayList();
    private List<String> sortKeys = Lists.newArrayList();

    private List<Index> indexes = Lists.newArrayList();

    // for backup. set to -1 for normal use
    private int tableSignature;

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
        this(ifNotExists, isExternal, tableName, columnDefinitions, null, engineName, null, keysDesc, partitionDesc,
                distributionDesc, properties, extProperties, comment, null, null);
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
        this(ifNotExists, isExternal, tableName, columnDefinitions, engineName, null, keysDesc, partitionDesc,
                distributionDesc, properties, extProperties, comment, ops, null);
    }

    public CreateTableStmt(boolean ifNotExists,
                           boolean isExternal,
                           TableName tableName,
                           List<ColumnDef> columnDefinitions,
                           String engineName,
                           String charsetName,
                           KeysDesc keysDesc,
                           PartitionDesc partitionDesc,
                           DistributionDesc distributionDesc,
                           Map<String, String> properties,
                           Map<String, String> extProperties,
                           String comment, List<AlterClause> ops, List<String> sortKeys) {
        this(ifNotExists, isExternal, tableName, columnDefinitions, null, engineName, charsetName, keysDesc, partitionDesc,
                distributionDesc, properties, extProperties, comment, ops, sortKeys);
    }

    public CreateTableStmt(boolean ifNotExists,
                           boolean isExternal,
                           TableName tableName,
                           List<ColumnDef> columnDefinitions,
                           List<IndexDef> indexDefs,
                           String engineName,
                           String charsetName,
                           KeysDesc keysDesc,
                           PartitionDesc partitionDesc,
                           DistributionDesc distributionDesc,
                           Map<String, String> properties,
                           Map<String, String> extProperties,
                           String comment, List<AlterClause> rollupAlterClauseList, List<String> sortKeys) {
        this(ifNotExists, isExternal, tableName, columnDefinitions, indexDefs, engineName, charsetName, keysDesc,
                partitionDesc, distributionDesc, properties, extProperties, comment, rollupAlterClauseList,
                sortKeys, NodePosition.ZERO);
    }

    public CreateTableStmt(boolean ifNotExists,
                           boolean isExternal,
                           TableName tableName,
                           List<ColumnDef> columnDefinitions,
                           List<IndexDef> indexDefs,
                           String engineName,
                           String charsetName,
                           KeysDesc keysDesc,
                           PartitionDesc partitionDesc,
                           DistributionDesc distributionDesc,
                           Map<String, String> properties,
                           Map<String, String> extProperties,
                           String comment, List<AlterClause> rollupAlterClauseList, List<String> sortKeys,
                           NodePosition pos) {
        super(pos);
        this.tableName = tableName;
        if (columnDefinitions == null) {
            this.columnDefs = Lists.newArrayList();
        } else {
            this.columnDefs = columnDefinitions;
        }
        this.indexDefs = indexDefs;
        this.engineName = engineName;
        this.charsetName = charsetName;
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
        this.sortKeys = sortKeys;
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

    public List<String> getSortKeys() {
        return sortKeys;
    }

    public void setEngineName(String engineName) {
        this.engineName = engineName;
    }

    public boolean isOlapEngine() {
        return engineName.equalsIgnoreCase(EngineType.OLAP.name());
    }

    public String getCharsetName() {
        return charsetName;
    }

    public void setCharsetName(String charsetName) {
        this.charsetName = charsetName;
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

    public static CreateTableStmt read(DataInput in) throws IOException {
        throw new RuntimeException("CreateTableStmt serialization is not supported anymore.");
    }

    @Override
    public boolean needAuditEncryption() {
        return !isOlapEngine();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateTableStatement(this, context);
    }
}
