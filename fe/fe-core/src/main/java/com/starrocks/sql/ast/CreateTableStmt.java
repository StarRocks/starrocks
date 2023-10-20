// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.IndexDef;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Index;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CreateTableStmt extends DdlStmt {

    private static final String DEFAULT_ENGINE_NAME = "olap";
    public static final String LAKE_ENGINE_NAME = "starrocks";
    private static final String DEFAULT_CHARSET_NAME = "utf8";

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

    private static Set<String> engineNames;

    private static Set<String> charsetNames;

    // set in analyze
    private List<Column> columns = Lists.newArrayList();
    private List<String> sortKeys = Lists.newArrayList();

    private List<Index> indexes = Lists.newArrayList();

    static {
        engineNames = Sets.newHashSet();
        engineNames.add("olap");
        engineNames.add("mysql");
        engineNames.add("broker");
        engineNames.add("elasticsearch");
        engineNames.add("hive");
        engineNames.add("file");
        engineNames.add("iceberg");
        engineNames.add("hudi");
        engineNames.add("jdbc");
        engineNames.add(LAKE_ENGINE_NAME);
    }

    static {
        charsetNames = Sets.newHashSet();
        charsetNames.add("utf8");
        charsetNames.add("gbk");
    }

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

        if (Strings.isNullOrEmpty(charsetName)) {
            this.charsetName = DEFAULT_CHARSET_NAME;
        } else {
            this.charsetName = charsetName;
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
        return engineName.equalsIgnoreCase("olap");
    }

    public boolean isLakeEngine() {
        return engineName.equals(LAKE_ENGINE_NAME);
    }

    public boolean isOlapOrLakeEngine() {
        return isOlapEngine() || isLakeEngine();
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
<<<<<<< HEAD
        return !isOlapOrLakeEngine();
=======
        return !Strings.isNullOrEmpty(engineName) && !isOlapEngine();
>>>>>>> 444cc3aeb1 ([BugFix] Fix sql is lost in audit log when creating OLAP table (#33176))
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateTableStatement(this, context);
    }
}
