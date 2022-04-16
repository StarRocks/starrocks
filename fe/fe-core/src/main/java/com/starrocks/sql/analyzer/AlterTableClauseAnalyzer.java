// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.AddColumnClause;
import com.starrocks.analysis.AddColumnsClause;
import com.starrocks.analysis.AddPartitionClause;
import com.starrocks.analysis.AddRollupClause;
import com.starrocks.analysis.AlterClause;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.ColumnPosition;
import com.starrocks.analysis.ColumnRenameClause;
import com.starrocks.analysis.CreateIndexClause;
import com.starrocks.analysis.DropColumnClause;
import com.starrocks.analysis.DropIndexClause;
import com.starrocks.analysis.DropPartitionClause;
import com.starrocks.analysis.DropRollupClause;
import com.starrocks.analysis.IndexDef;
import com.starrocks.analysis.ModifyColumnClause;
import com.starrocks.analysis.ModifyPartitionClause;
import com.starrocks.analysis.ModifyTablePropertiesClause;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.analysis.PartitionRenameClause;
import com.starrocks.analysis.ReorderColumnsClause;
import com.starrocks.analysis.ReplacePartitionClause;
import com.starrocks.analysis.RollupRenameClause;
import com.starrocks.analysis.SwapTableClause;
import com.starrocks.analysis.TableRenameClause;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlterTableClauseAnalyzer {
    public static void analyze(AlterClause alterTableClause, ConnectContext session) {
        new AlterTableClauseAnalyzerVisitor().analyze(alterTableClause, session);
    }
    static class AlterTableClauseAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(AlterClause statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitAddColumnClause(AddColumnClause statement, ConnectContext context) {
            ColumnDef columnDef = statement.getColumnDef();
            ColumnPosition columnPosition = statement.getColPos();
            String rollupName = statement.getRollupName();
            if (columnDef == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERROR_NO_COLUMN_DEFINITION);
            }
            try {
                columnDef.analyze(true);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
            if (columnPosition != null) {
                columnPosition.analyze();
            }

            if (!columnDef.isAllowNull() && columnDef.defaultValueIsNull()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DEFAULT_FOR_FIELD, columnDef.getName());
            }

            if (columnDef.getAggregateType() != null && columnPosition != null && columnPosition.isFirst()) {
                throw new SemanticException("Cannot add value column[" + columnDef.getName() + "] at first");
            }

            if (Strings.isNullOrEmpty(rollupName)) {
                statement.setRollupName(null);
            }

            statement.setColumn(columnDef.toColumn());
            return null;
        }

        @Override
        public Void visitAddColumnsClause(AddColumnsClause statement, ConnectContext context) {
            List<ColumnDef> columnDefs = statement.getColumnDefs();
            if (columnDefs == null || columnDefs.isEmpty()) {
                throw new SemanticException("Columns is empty in add columns clause.");
            }
            for (ColumnDef colDef : columnDefs) {
                try {
                    colDef.analyze(true);
                } catch (AnalysisException e) {
                    throw new SemanticException(e.getMessage());
                }

                if (!colDef.isAllowNull() && colDef.defaultValueIsNull()) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DEFAULT_FOR_FIELD, colDef.getName());
                }
            }

            // Make sure return null if rollup name is empty.
            statement.setRollupName(Strings.emptyToNull(statement.getRollupName()));

            List<Column> columns = Lists.newArrayList();
            for (ColumnDef columnDef : columnDefs) {
                Column column = columnDef.toColumn();
                columns.add(column);
            }
            statement.setColumns(columns);
            return null;
        }

        @Override
        public Void visitAddPartitionClause(AddPartitionClause statement, ConnectContext context) {
            return super.visitAddPartitionClause(statement, context);
        }

        @Override
        public Void visitAddRollupClause(AddRollupClause statement, ConnectContext context) {
            String rollupName = statement.getRollupName();
            try {
                FeNameFormat.checkTableName(rollupName);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
            List<String> columnNames = statement.getColumnNames();
            if (columnNames == null || columnNames.isEmpty()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLE_MUST_HAVE_COLUMNS);
            }
            Set<String> colSet = Sets.newHashSet();
            for (String col : columnNames) {
                if (Strings.isNullOrEmpty(col)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_COLUMN_NAME, col);
                }
                if (!colSet.add(col)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, col);
                }
            }
            statement.setBaseRollupName(Strings.emptyToNull(statement.getBaseRollupName()));
            return null;
        }

        @Override
        public Void visitColumnRenameClause(ColumnRenameClause statement, ConnectContext context) {
            String colName = statement.getColName();
            if (Strings.isNullOrEmpty(colName)) {
                throw new SemanticException("Column name is not set");
            }

            String newColName = statement.getNewColName();
            if (Strings.isNullOrEmpty(newColName)) {
                throw new SemanticException("New column name is not set");
            }

            try {
                FeNameFormat.checkColumnName(newColName);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
            return null;
        }

        @Override
        public Void visitCreateIndexClause(CreateIndexClause statement, ConnectContext context) {
            IndexDef indexDef = statement.getIndexDef();
            if (indexDef == null) {
                throw new SemanticException("index definition expected.");
            }
            try {
                indexDef.analyze();
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
            Index index = new Index(indexDef.getIndexName(), indexDef.getColumns(), indexDef.getIndexType(),
                    indexDef.getComment());
            statement.setIndex(index);
            return null;
        }

        @Override
        public Void visitDropColumnClause(DropColumnClause statement, ConnectContext context) {
            String colName = statement.getColName();
            if (Strings.isNullOrEmpty(colName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_COLUMN_NAME, colName);
            }
            statement.setRollupName(Strings.emptyToNull(statement.getRollupName()));
            return null;
        }

        @Override
        public Void visitDropIndexClause(DropIndexClause statement, ConnectContext context) {
            String indexName = statement.getIndexName();
            if (StringUtils.isEmpty(indexName)) {
                throw new SemanticException("index name is excepted");
            }
            return null;
        }

        @Override
        public Void visitDropPartitionClause(DropPartitionClause statement, ConnectContext context) {
            String partitionName = statement.getPartitionName();
            if (Strings.isNullOrEmpty(partitionName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_PARTITION_NAME, partitionName);
            }
            return null;
        }

        @Override
        public Void visitDropRollupClause(DropRollupClause statement, ConnectContext context) {
            String rollupName = statement.getRollupName();
            if (Strings.isNullOrEmpty(rollupName)) {
                throw new SemanticException("No rollup in delete rollup.");
            }
            return null;
        }

        @Override
        public Void visitModifyColumnClause(ModifyColumnClause statement, ConnectContext context) {
            ColumnDef columnDef = statement.getColumnDef();
            if (columnDef == null) {
                throw new SemanticException("No column definition in modify column clause.");
            }
            try {
                columnDef.analyze(true);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
            ColumnPosition columnPosition = statement.getColPos();
            if (columnPosition != null) {
                columnPosition.analyze();
            }
            statement.setRollupName(Strings.emptyToNull(statement.getRollupName()));
            statement.setColumn(columnDef.toColumn());
            return null;
        }

        @Override
        public Void visitModifyPartitionClause(ModifyPartitionClause statement, ConnectContext context) {
            List<String> partitionNames = statement.getPartitionNames();
            boolean needExpand = statement.isNeedExpand();
            Map<String, String> properties = statement.getProperties();
            if (partitionNames == null || (!needExpand && partitionNames.isEmpty())) {
                throw new SemanticException("Partition names is not set or empty");
            }

            if (partitionNames.stream().anyMatch(entity -> Strings.isNullOrEmpty(entity))) {
                throw new SemanticException("there are empty partition name");
            }

            if (properties == null || properties.isEmpty()) {
                throw new SemanticException("Properties is not set");
            }

            // check properties here
            try {
                checkProperties(Maps.newHashMap(properties));
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
            return null;
        }

        @Override
        public Void visitModifyTablePropertiesClause(ModifyTablePropertiesClause statement, ConnectContext context) {
            Map<String, String> properties = statement.getProperties();
            if (properties == null || properties.isEmpty()) {
                throw new SemanticException("Properties is not set");
            }

            if (properties.size() != 1
                    && !TableProperty
                    .isSamePrefixProperties(properties, TableProperty.DYNAMIC_PARTITION_PROPERTY_PREFIX)) {
                throw new SemanticException("Can only set one table property at a time");
            }

            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)) {
                statement.setNeedTableStable(false);
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE)) {
                if (!properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE).equalsIgnoreCase("column")) {
                    throw new SemanticException("Can only change storage type to COLUMN");
                }
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_TYPE)) {
                if (!properties.get(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_TYPE).equalsIgnoreCase("hash")) {
                    throw new SemanticException("Can only change distribution type to HASH");
                }
                statement.setNeedTableStable(false);
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK)) {
                if (!properties.get(PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK).equalsIgnoreCase("true")) {
                    throw new SemanticException(
                            "Property " + PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK + " should be set to true");
                }
                statement.setNeedTableStable(false);
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BF_COLUMNS)
                    || properties.containsKey(PropertyAnalyzer.PROPERTIES_BF_FPP)) {
                // do nothing, these 2 properties will be analyzed when creating alter job
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT)) {
                if (!properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT).equalsIgnoreCase("v2")) {
                    throw new SemanticException(
                            "Property " + PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT + " should be v2");
                }
            } else if (DynamicPartitionUtil.checkDynamicPartitionPropertiesExist(properties)) {
                // do nothing, dynamic properties will be analyzed in SchemaChangeHandler.process
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
                PropertyAnalyzer.analyzeReplicationNum(properties, false);
            } else if (properties.containsKey("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
                short defaultReplicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, true);
                properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Short.toString(defaultReplicationNum));
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
                statement.setNeedTableStable(false);
                statement.setOpType(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC);
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TABLET_TYPE)) {
                throw new SemanticException("Alter tablet type not supported");
            } else {
                throw new SemanticException("Unknown table property: " + properties.keySet());
            }
            return null;
        }

        @Override
        public Void visitPartitionRenameClause(PartitionRenameClause statement, ConnectContext context) {
            String partitionName = statement.getPartitionName();
            String newPartitionName = statement.getNewPartitionName();
            if (Strings.isNullOrEmpty(partitionName)) {
                throw new SemanticException("Partition name is not set");
            }

            if (Strings.isNullOrEmpty(newPartitionName)) {
                throw new SemanticException("New partition name is not set");
            }

            try {
                FeNameFormat.checkPartitionName(newPartitionName);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
            return null;
        }

        @Override
        public Void visitReorderColumnsClause(ReorderColumnsClause statement, ConnectContext context) {
            List<String> columnsByPos = statement.getColumnsByPos();
            if (columnsByPos == null || columnsByPos.isEmpty()) {
                throw new SemanticException("No column in reorder columns clause.");
            }
            for (String col : columnsByPos) {
                if (Strings.isNullOrEmpty(col)) {
                    throw new SemanticException("Empty column in reorder columns.");
                }
            }
            statement.setRollupName(Strings.emptyToNull(statement.getRollupName()));
            return super.visitReorderColumnsClause(statement, context);
        }

        @Override
        public Void visitReplacePartitionClause(ReplacePartitionClause statement, ConnectContext context) {
            PartitionNames partitionNames = statement.getPartitionNamesObject();
            PartitionNames tempPartitionNames = statement.getTempPartitionNamesObject();
            Map<String, String> properties = statement.getProperties();
            if (partitionNames == null || tempPartitionNames == null) {
                throw new SemanticException("No partition specified");
            }

            PartitionNamesAnalyzer.analyze(partitionNames, context);
            PartitionNamesAnalyzer.analyze(tempPartitionNames, context);

            if (partitionNames.isTemp() || !tempPartitionNames.isTemp()) {
                throw new SemanticException("Only support replace partitions with temp partitions");
            }

            statement.setStrictRange(
                    PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_STRICT_RANGE, true));
            statement.setUseTempPartitionName(PropertyAnalyzer.analyzeBooleanProp(properties,
                    PropertyAnalyzer.PROPERTIES_USE_TEMP_PARTITION_NAME, false));

            if (properties != null && !properties.isEmpty()) {
                throw new SemanticException("Unknown properties: " + properties.keySet());
            }
            return null;
        }

        @Override
        public Void visitRollupRenameClause(RollupRenameClause statement, ConnectContext context) {
            String rollupName = statement.getRollupName();
            String newRollupName = statement.getNewRollupName();
            if (Strings.isNullOrEmpty(rollupName)) {
                throw new SemanticException("Rollup name is not set");
            }

            if (Strings.isNullOrEmpty(newRollupName)) {
                throw new SemanticException("New rollup name is not set");
            }

            try {
                FeNameFormat.checkTableName(newRollupName);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
            return null;
        }

        @Override
        public Void visitSwapTableClause(SwapTableClause statement, ConnectContext context) {
            String tblName = statement.getTblName();
            if (org.apache.parquet.Strings.isNullOrEmpty(tblName)) {
                throw new SemanticException("No table specified");
            }
            return null;
        }

        @Override
        public Void visitTableRenameClause(TableRenameClause statement, ConnectContext context) {
            String newTableName = statement.getNewTableName();
            if (Strings.isNullOrEmpty(newTableName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERROR_NO_NEW_TABLE_NAME);
            }
            try {
                FeNameFormat.checkTableName(newTableName);
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_TABLE_NAME);
            }
            return null;
        }

        // Check the following properties' legality before modifying partition.
        // 1. replication_num
        // 2. storage_medium && storage_cooldown_time
        // 3. in_memory
        // 4. tablet type
        private void checkProperties(Map<String, String> properties) throws AnalysisException {
            // 1. data property
            DataProperty newDataProperty = null;
            newDataProperty = PropertyAnalyzer.analyzeDataProperty(properties, DataProperty.DEFAULT_DATA_PROPERTY);
            Preconditions.checkNotNull(newDataProperty);

            // 2. replication num
            short newReplicationNum = (short) -1;
            newReplicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, FeConstants.default_replication_num);
            Preconditions.checkState(newReplicationNum != (short) -1);

            // 3. in memory
            PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);

            // 4. tablet type
            PropertyAnalyzer.analyzeTabletType(properties);
        }
    }
}
