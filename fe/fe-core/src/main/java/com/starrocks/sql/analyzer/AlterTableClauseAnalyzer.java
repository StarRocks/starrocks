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
import com.starrocks.analysis.ColumnPosition;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.NotImplementedException;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.WriteQuorum;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AddFieldClause;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AddRollupClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterMaterializedViewStatusClause;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.CompactionClause;
import com.starrocks.sql.ast.CreateIndexClause;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.DropFieldClause;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.DropRollupClause;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.ast.IndexDef.IndexType;
import com.starrocks.sql.ast.IntervalLiteral;
import com.starrocks.sql.ast.KeysDesc;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.ModifyPartitionClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.MultiRangePartitionDesc;
import com.starrocks.sql.ast.OptimizeClause;
import com.starrocks.sql.ast.PartitionConvertContext;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.PartitionRenameClause;
import com.starrocks.sql.ast.RandomDistributionDesc;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.RefreshSchemeClause;
import com.starrocks.sql.ast.ReorderColumnsClause;
import com.starrocks.sql.ast.ReplacePartitionClause;
import com.starrocks.sql.ast.RollupRenameClause;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.sql.ast.SinglePartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.ast.StructFieldDesc;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import org.apache.commons.collections4.CollectionUtils;

import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

public class AlterTableClauseAnalyzer implements AstVisitor<Void, ConnectContext> {
    private final Table table;

    public AlterTableClauseAnalyzer(Table table) {
        this.table = table;
    }

    public void analyze(ConnectContext session, AlterClause statement) {
        visit(statement, session);
    }

    @Override
    public Void visitCreateIndexClause(CreateIndexClause clause, ConnectContext context) {
        IndexDef indexDef = clause.getIndexDef();
        indexDef.analyze();
        Index index;
        // Only assign meaningful indexId for OlapTable
        if (table.isOlapTableOrMaterializedView()) {
            long indexId = IndexType.isCompatibleIndex(indexDef.getIndexType()) ? ((OlapTable) table).incAndGetMaxIndexId() : -1;
            index = new Index(indexId, indexDef.getIndexName(),
                    MetaUtils.getColumnIdsByColumnNames(table, indexDef.getColumns()),
                    indexDef.getIndexType(), indexDef.getComment(), indexDef.getProperties());
        } else {
            index = new Index(indexDef.getIndexName(),
                    MetaUtils.getColumnIdsByColumnNames(table, indexDef.getColumns()),
                    indexDef.getIndexType(), indexDef.getComment(), indexDef.getProperties());
        }
        clause.setIndex(index);
        return null;
    }

    @Override
    public Void visitTableRenameClause(TableRenameClause clause, ConnectContext context) {
        String newTableName = clause.getNewTableName();
        if (Strings.isNullOrEmpty(newTableName)) {
            throw new SemanticException("New Table name is not set");
        }
        if (table.getName().equals(newTableName)) {
            throw new SemanticException("Same table name " + newTableName, clause.getPos());
        }
        FeNameFormat.checkTableName(newTableName);
        return null;
    }

    @Override
    public Void visitModifyTablePropertiesClause(ModifyTablePropertiesClause clause, ConnectContext context) {
        Map<String, String> properties = clause.getProperties();
        if (properties.isEmpty()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Properties is not set");
        }

        if (table.isExternalTableWithFileSystem()) {
            return null;
        }

        if (properties.size() != 1
                && !(TableProperty.isSamePrefixProperties(properties, TableProperty.DYNAMIC_PARTITION_PROPERTY_PREFIX)
                || TableProperty.isSamePrefixProperties(properties, TableProperty.BINLOG_PROPERTY_PREFIX))) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Can only set one table property at a time");
        }

        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)) {
            // do nothing
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Can't change storage type");
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_TYPE)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_TYPE).equalsIgnoreCase("hash")) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Can only change distribution type to HASH");
            }
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK).equalsIgnoreCase("true")) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Property " + PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK + " should be set to true");
            }
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BF_COLUMNS)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_BF_FPP)) {
            // do nothing, these 2 properties will be analyzed when creating alter job
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM)) {
            if (WriteQuorum.findTWriteQuorumByName(properties.get(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM)) == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Property " + PropertyAnalyzer.PROPERTIES_WRITE_QUORUM + " not valid");
            }
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION)) {
            try {
                PropertyAnalyzer.analyzeLocation(properties, false);
            } catch (SemanticException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Property " + PropertyAnalyzer.PROPERTIES_LABELS_LOCATION + " not valid");
            }
        } else if (DynamicPartitionUtil.checkDynamicPartitionPropertiesExist(properties)) {
            // do nothing, dynamic properties will be analyzed in SchemaChangeHandler.process
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER)) {
            PropertyAnalyzer.analyzePartitionLiveNumber(properties, false);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
            PropertyAnalyzer.analyzeReplicationNum(properties, false);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL)) {
            String storageCoolDownTTL = properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL);
            try {
                PropertyAnalyzer.analyzeStorageCoolDownTTL(properties, true);
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
            }
            properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL, storageCoolDownTTL);
        } else if (properties.containsKey("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
            short defaultReplicationNum = 0;
            defaultReplicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, true);
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Short.toString(defaultReplicationNum));
        } else if (properties.containsKey("default." + PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)) {
            String storageMedium = properties.remove("default." + PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM);
            properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, storageMedium);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
            // do nothing
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PRIMARY_INDEX_CACHE_EXPIRE_SEC)) {
            String valStr = properties.get(PropertyAnalyzer.PROPERTIES_PRIMARY_INDEX_CACHE_EXPIRE_SEC);
            try {
                int val = Integer.parseInt(valStr);
                if (val < 0) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Property "
                            + PropertyAnalyzer.PROPERTIES_PRIMARY_INDEX_CACHE_EXPIRE_SEC + " must not be less than 0");
                }
            } catch (NumberFormatException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Property "
                        + PropertyAnalyzer.PROPERTIES_PRIMARY_INDEX_CACHE_EXPIRE_SEC + " must be integer: " + valStr);
            }
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX).equalsIgnoreCase("true") &&
                    !properties.get(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX).equalsIgnoreCase("false")) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Property " + PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX +
                                " must be bool type(false/true)");
            }
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE).equalsIgnoreCase("true") &&
                    !properties.get(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE).equalsIgnoreCase("false")) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Property " + PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE +
                                " must be bool type(false/true)");
            }
            if (table instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) table;
                boolean hasNewIndex = false;
                for (Index index : olapTable.getIndexes()) {
                    if (IndexType.isCompatibleIndex(index.getIndexType())) {
                        hasNewIndex = true;
                        break;
                    }
                }
                if (properties.get(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE).equalsIgnoreCase("true") && hasNewIndex) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_GIN_REPLICATED_STORAGE_NOT_SUPPORTED);
                }
            }
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BUCKET_SIZE)) {
            PropertyAnalyzer.analyzeBucketSize(properties);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_MUTABLE_BUCKET_NUM)) {
            PropertyAnalyzer.analyzeMutableBucketNum(properties);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_LOAD_PROFILE)) {
            PropertyAnalyzer.analyzeEnableLoadProfile(properties);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE) ||
                properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_TTL) ||
                properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE)) {

            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE)) {
                String binlogEnable = properties.get(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE);
                if (!(binlogEnable.equals("false") || binlogEnable.equals("true"))) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Property " + PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE +
                                    " must be true of false");
                }
                if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_TTL)) {
                    try {
                        Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_BINLOG_TTL));
                    } catch (NumberFormatException e) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                                "Property " + PropertyAnalyzer.PROPERTIES_BINLOG_TTL +
                                        " must be long");
                    }
                }
                if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE)) {
                    try {
                        Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE));
                    } catch (NumberFormatException e) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                                "Property " + PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE +
                                        " must be long");
                    }
                }
            }
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TABLET_TYPE)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Alter tablet type not supported");
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)) {
            // do nothing
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION)) {
            try {
                TimeUtils.parseHumanReadablePeriodOrDuration(
                        properties.get(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION));
            } catch (DateTimeParseException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
            }
        } else {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Unknown properties: " + properties);
        }
        return null;
    }

    @Override
    public Void visitModifyPartitionClause(ModifyPartitionClause clause, ConnectContext context) {
        final List<String> partitionNames = clause.getPartitionNames();
        final boolean needExpand = clause.isNeedExpand();
        final Map<String, String> properties = clause.getProperties();
        if (partitionNames == null || (!needExpand && partitionNames.isEmpty())) {
            throw new SemanticException("Partition names is not set or empty");
        }

        if (partitionNames.stream().anyMatch(Strings::isNullOrEmpty)) {
            throw new SemanticException("there are empty partition name");
        }

        if (properties == null || properties.isEmpty()) {
            throw new SemanticException("Properties is not set");
        }

        // check properties here
        try {
            checkProperties(Maps.newHashMap(properties));
        } catch (AnalysisException e) {
            throw new SemanticException("check properties error: %s", e.getMessage());
        }
        return null;
    }

    @Override
    public Void visitOptimizeClause(OptimizeClause clause, ConnectContext context) {
        if (!(table instanceof OlapTable)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_NOT_OLAP_TABLE, table.getName());
        }
        OlapTable olapTable = (OlapTable) table;
        if (olapTable.getColocateGroup() != null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Optimize table in colocate group is not supported");
        }

        if (olapTable.isMaterializedView()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Optimize materialized view is not supported");
        }

        List<Integer> sortKeyIdxes = Lists.newArrayList();
        List<ColumnDef> columnDefs = olapTable.getColumns()
                .stream().map(column -> column.toColumnDef(olapTable)).collect(Collectors.toList());
        if (clause.getSortKeys() != null) {
            List<String> columnNames = columnDefs.stream().map(ColumnDef::getName).collect(Collectors.toList());

            for (String column : clause.getSortKeys()) {
                int idx = columnNames.indexOf(column);
                if (idx == -1) {
                    throw new SemanticException("Unknown column '%s' does not exist", column);
                }
                sortKeyIdxes.add(idx);
            }
        }
        boolean hasReplace = false;
        Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (ColumnDef columnDef : columnDefs) {
            if (columnDef.getAggregateType() != null && columnDef.getAggregateType().isReplaceFamily()) {
                hasReplace = true;
            }
            if (!columnSet.add(columnDef.getName())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, columnDef.getName());
            }
        }

        // analyze key desc
        KeysType originalKeysType = olapTable.getKeysType();
        KeysDesc keysDesc = clause.getKeysDesc();
        if (keysDesc != null) {
            throw new SemanticException("not support change keys type when optimize table");
        }
        KeysType targetKeysType = keysDesc == null ? originalKeysType : keysDesc.getKeysType();

        // analyze distribution
        DistributionDesc distributionDesc = clause.getDistributionDesc();
        if (distributionDesc != null) {
            if (distributionDesc instanceof RandomDistributionDesc && targetKeysType != KeysType.DUP_KEYS
                    && !(targetKeysType == KeysType.AGG_KEYS && !hasReplace)) {
                throw new SemanticException(targetKeysType.toSql() + (hasReplace ? " with replace " : "")
                        + " must use hash distribution", distributionDesc.getPos());
            }
            distributionDesc.analyze(columnSet);
            clause.setDistributionDesc(distributionDesc);

            if (distributionDesc.getType() != olapTable.getDefaultDistributionInfo().getType()
                    && clause.getPartitionNames() != null) {
                throw new SemanticException("not support change distribution type when specify partitions");
            }
        }

        // analyze partitions
        PartitionNames partitionNames = clause.getPartitionNames();
        if (partitionNames != null) {
            if (clause.getSortKeys() != null || clause.getKeysDesc() != null) {
                throw new SemanticException("not support change sort keys or keys type when specify partitions");
            }
            if (partitionNames.isTemp()) {
                throw new SemanticException("not support optimize temp partition");
            }
            List<String> partitionNameList = partitionNames.getPartitionNames();
            if (partitionNameList == null || partitionNameList.isEmpty()) {
                throw new SemanticException("partition names is empty");
            }

            if (distributionDesc instanceof HashDistributionDesc
                    && olapTable.getDefaultDistributionInfo() instanceof HashDistributionInfo) {
                HashDistributionDesc hashDistributionDesc = (HashDistributionDesc) distributionDesc;
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) olapTable.getDefaultDistributionInfo();
                List<String> orginalPartitionColumn = MetaUtils.getColumnNamesByColumnIds(
                        table.getIdToColumn(), hashDistributionInfo.getDistributionColumns());
                List<String> newPartitionColumn = hashDistributionDesc.getDistributionColumnNames();
                if (!orginalPartitionColumn.equals(newPartitionColumn)) {
                    throw new SemanticException("not support change distribution column when specify partitions");
                }
            }

            List<Long> partitionIds = Lists.newArrayList();
            for (String partitionName : partitionNameList) {
                Partition partition = olapTable.getPartition(partitionName);
                if (partition == null) {
                    throw new SemanticException("partition %s does not exist", partitionName);
                }
                partitionIds.add(partition.getId());
            }
            clause.setSourcePartitionIds(partitionIds);
        } else {
            clause.setSourcePartitionIds(olapTable.getPartitions().stream().map(Partition::getId).collect(Collectors.toList()));
            clause.setTableOptimize(true);
        }

        return null;
    }

    @Override
    public Void visitAddColumnClause(AddColumnClause clause, ConnectContext context) {
        ColumnDef columnDef = clause.getColumnDef();
        if (columnDef == null) {
            throw new SemanticException("No column definition in add column clause.");
        }
        try {
            if (table.isOlapTable() && ((OlapTable) table).getKeysType() == KeysType.PRIMARY_KEYS) {
                columnDef.setAggregateType(AggregateType.REPLACE);
            }
            columnDef.analyze(true);
        } catch (AnalysisException e) {
            throw new SemanticException(PARSER_ERROR_MSG.invalidColumnDef(e.getMessage()), columnDef.getPos());
        }

        if (columnDef.getType().isTime()) {
            throw new SemanticException("Unsupported data type: TIME");
        }

        if (columnDef.isGeneratedColumn()) {
            if (!table.isOlapTable()) {
                throw new SemanticException("Generated Column only support olap table");
            }

            if (table.isCloudNativeTable()) {
                throw new SemanticException("Lake table does not support generated column");
            }

            if (((OlapTable) table).getKeysType() == KeysType.AGG_KEYS) {
                throw new SemanticException("Generated Column does not support AGG table");
            }

            clause.setRollupName(Strings.emptyToNull(clause.getRollupName()));

            Expr expr = columnDef.generatedColumnExpr();
            TableName tableName = new TableName(context.getDatabase(), table.getName());

            ExpressionAnalyzer.analyzeExpression(expr, new AnalyzeState(), new Scope(RelationId.anonymous(),
                    new RelationFields(table.getBaseSchema().stream().map(col -> new Field(col.getName(), col.getType(),
                                    tableName, null))
                            .collect(Collectors.toList()))), context);

            // check if contain aggregation
            List<FunctionCallExpr> funcs = Lists.newArrayList();
            expr.collect(FunctionCallExpr.class, funcs);
            for (FunctionCallExpr fn : funcs) {
                if (fn.isAggregateFunction()) {
                    throw new SemanticException("Generated Column don't support aggregation function");
                }
            }

            // check if the expression refers to other generated columns
            List<SlotRef> slots = Lists.newArrayList();
            expr.collect(SlotRef.class, slots);
            if (slots.size() != 0) {
                for (SlotRef slot : slots) {
                    Column refColumn = table.getColumn(slot.getColumnName());
                    if (refColumn.isGeneratedColumn()) {
                        throw new SemanticException("Expression can not refers to other generated columns: " +
                                refColumn.getName());
                    }
                    if (refColumn.isAutoIncrement()) {
                        throw new SemanticException("Expression can not refers to AUTO_INCREMENT columns: " +
                                refColumn.getName());
                    }
                }
            }

            if (!columnDef.getType().matchesType(expr.getType())) {
                throw new SemanticException("Illegal expression type for Generated Column " +
                        "Column Type: " + columnDef.getType().toString() +
                        ", Expression Type: " + expr.getType().toString());
            }
            clause.setColumn(columnDef.toColumn(table));
            return null;
        }

        ColumnPosition colPos = clause.getColPos();
        if (colPos == null && table instanceof OlapTable && ((OlapTable) table).hasGeneratedColumn()) {
            List<Column> baseSchema = ((OlapTable) table).getBaseSchema();
            if (baseSchema.size() > 1) {
                for (int columnIdx = 0; columnIdx < baseSchema.size() - 1; ++columnIdx) {
                    if (!baseSchema.get(columnIdx).isGeneratedColumn() &&
                            baseSchema.get(columnIdx + 1).isGeneratedColumn()) {
                        clause.setColPos(new ColumnPosition(baseSchema.get(columnIdx).getName()));
                        break;
                    }
                }
            }
        }
        if (colPos != null) {
            try {
                colPos.analyze();
            } catch (AnalysisException e) {
                throw new SemanticException(PARSER_ERROR_MSG.invalidColumnPos(e.getMessage()), colPos.getPos());
            }
        }

        if (!columnDef.isAllowNull() && columnDef.defaultValueIsNull()) {
            throw new SemanticException(PARSER_ERROR_MSG.withOutDefaultVal(columnDef.getName()), columnDef.getPos());
        }

        if (columnDef.getAggregateType() != null && colPos != null && colPos.isFirst()) {
            throw new SemanticException("Cannot add value column[" + columnDef.getName() + "] at first",
                    columnDef.getPos());
        }

        // Make sure return null if rollup name is empty.
        clause.setRollupName(Strings.emptyToNull(clause.getRollupName()));

        clause.setColumn(columnDef.toColumn(table));
        return null;
    }

    @Override
    public Void visitAddColumnsClause(AddColumnsClause clause, ConnectContext context) {
        List<ColumnDef> columnDefs = clause.getColumnDefs();
        if (columnDefs == null || columnDefs.isEmpty()) {
            throw new SemanticException("Columns is empty in add columns clause.");
        }
        boolean hasGeneratedColumn = false;
        boolean hasNormalColumn = false;
        for (ColumnDef colDef : columnDefs) {
            try {
                if (table.isOlapTable() && ((OlapTable) table).getKeysType() == KeysType.PRIMARY_KEYS) {
                    colDef.setAggregateType(AggregateType.REPLACE);
                }
                colDef.analyze(true);
            } catch (AnalysisException e) {
                throw new SemanticException(PARSER_ERROR_MSG.invalidColumnDef(e.getMessage()), colDef.getPos());
            }
            if (!colDef.isAllowNull() && colDef.defaultValueIsNull()) {
                throw new SemanticException(PARSER_ERROR_MSG.withOutDefaultVal(colDef.getName()), colDef.getPos());
            }

            if (colDef.isGeneratedColumn()) {
                hasGeneratedColumn = true;

                if (!table.isOlapTable()) {
                    throw new SemanticException("Generated Column only support olap table");
                }

                if (table.isCloudNativeTable()) {
                    throw new SemanticException("Lake table does not support generated column");
                }

                if (((OlapTable) table).getKeysType() == KeysType.AGG_KEYS) {
                    throw new SemanticException("Generated Column does not support AGG table");
                }

                Expr expr = colDef.generatedColumnExpr();
                TableName tableName = new TableName(context.getDatabase(), table.getName());

                ExpressionAnalyzer.analyzeExpression(expr, new AnalyzeState(), new Scope(RelationId.anonymous(),
                        new RelationFields(table.getBaseSchema().stream().map(col -> new Field(col.getName(), col.getType(),
                                        tableName, null))
                                .collect(Collectors.toList()))), context);

                // check if contain aggregation
                List<FunctionCallExpr> funcs = Lists.newArrayList();
                expr.collect(FunctionCallExpr.class, funcs);
                for (FunctionCallExpr fn : funcs) {
                    if (fn.isAggregateFunction()) {
                        throw new SemanticException("Generated Column don't support aggregation function");
                    }
                }

                // check if the expression refers to other generated columns
                List<SlotRef> slots = Lists.newArrayList();
                expr.collect(SlotRef.class, slots);
                if (slots.size() != 0) {
                    for (SlotRef slot : slots) {
                        Column refColumn = table.getColumn(slot.getColumnName());
                        if (refColumn.isGeneratedColumn()) {
                            throw new SemanticException("Expression can not refers to other generated columns");
                        }
                        if (refColumn.isAutoIncrement()) {
                            throw new SemanticException("Expression can not refers to AUTO_INCREMENT columns");
                        }
                    }
                }

                if (!colDef.getType().matchesType(expr.getType())) {
                    throw new SemanticException("Illegal expression type for Generated Column " +
                            "Column Type: " + colDef.getType().toString() +
                            ", Expression Type: " + expr.getType().toString());
                }
            } else {
                hasNormalColumn = true;
            }
        }

        if (hasGeneratedColumn && hasNormalColumn) {
            throw new SemanticException("Can not add normal column and Generated Column in the same time");
        }

        if (hasNormalColumn && table instanceof OlapTable && ((OlapTable) table).hasGeneratedColumn()) {
            List<Column> baseSchema = ((OlapTable) table).getBaseSchema();
            if (baseSchema.size() > 1) {
                for (int columnIdx = 0; columnIdx < baseSchema.size() - 1; ++columnIdx) {
                    if (!baseSchema.get(columnIdx).isGeneratedColumn() &&
                            baseSchema.get(columnIdx + 1).isGeneratedColumn()) {
                        ColumnPosition pos = new ColumnPosition(baseSchema.get(columnIdx).getName());
                        clause.setGeneratedColumnPos(pos);
                        break;
                    }
                }
            }
        }

        // Make sure return null if rollup name is empty.
        clause.setRollupName(Strings.emptyToNull(clause.getRollupName()));

        columnDefs.forEach(columnDef -> clause.addColumn(columnDef.toColumn(table)));
        return null;
    }

    @Override
    public Void visitDropColumnClause(DropColumnClause clause, ConnectContext context) {
        if (Strings.isNullOrEmpty(clause.getColName())) {
            throw new SemanticException(PARSER_ERROR_MSG.invalidColFormat(clause.getColName()));
        }
        clause.setRollupName(Strings.emptyToNull(clause.getRollupName()));

        for (Column column : table.getFullSchema()) {
            if (column.isGeneratedColumn()) {
                List<SlotRef> slots = column.getGeneratedColumnRef(table.getIdToColumn());
                for (SlotRef slot : slots) {
                    if (slot.getColumnName().equals(clause.getColName())) {
                        throw new SemanticException("Column: " + clause.getColName() + " can not be dropped" +
                                ", because expression of Generated Column: " +
                                column.getName() + " will refer to it");
                    }
                }
            }
        }
        return null;
    }

    @Override
    public Void visitAddFieldClause(AddFieldClause clause, ConnectContext context) {
        String columnName = clause.getColName();
        if (Strings.isNullOrEmpty(columnName)) {
            throw new SemanticException(PARSER_ERROR_MSG.invalidColFormat(columnName));
        }

        if (!table.isOlapTable() && !table.isCloudNativeTable()) {
            throw new SemanticException("Add field only support olap table");
        }

        Column baseColumn = ((OlapTable) table).getBaseColumn(columnName);
        StructFieldDesc fieldDesc = clause.getFieldDesc();
        try {
            fieldDesc.analyze(baseColumn, false);
        } catch (AnalysisException e) {
            throw new SemanticException("Analyze add field definition failed: %s", e.getMessage());
        }
        return null;
    }

    @Override
    public Void visitDropFieldClause(DropFieldClause clause, ConnectContext context) {
        String columnName = clause.getColName();
        if (Strings.isNullOrEmpty(columnName)) {
            throw new SemanticException(PARSER_ERROR_MSG.invalidColFormat(columnName));
        }

        if (!table.isOlapTable() && !table.isCloudNativeTable()) {
            throw new SemanticException("Drop field only support olap table");
        }

        Column baseColumn = ((OlapTable) table).getBaseColumn(columnName);
        StructFieldDesc fieldDesc = new StructFieldDesc(clause.getFieldName(), clause.getNestedParentFieldNames(), null, null);
        try {
            fieldDesc.analyze(baseColumn, true);
        } catch (AnalysisException e) {
            throw new SemanticException("Analyze drop field definition failed: %s", e.getMessage());
        }
        return null;
    }

    @Override
    public Void visitModifyColumnClause(ModifyColumnClause clause, ConnectContext context) {
        ColumnDef columnDef = clause.getColumnDef();
        if (columnDef == null) {
            throw new SemanticException("No column definition in modify column clause.");
        }
        try {
            if (table.isOlapTable() && ((OlapTable) table).getKeysType() == KeysType.PRIMARY_KEYS) {
                columnDef.setAggregateType(AggregateType.REPLACE);
            }
            columnDef.analyze(true);
        } catch (AnalysisException e) {
            throw new SemanticException(PARSER_ERROR_MSG.invalidColumnDef(e.getMessage()), columnDef.getPos());
        }

        if (columnDef.getType().isTime()) {
            throw new SemanticException("Unsupported data type: TIME");
        }

        if (columnDef.isGeneratedColumn()) {
            if (!(table instanceof OlapTable)) {
                throw new SemanticException("Generated Column only support olap table");
            }

            if (table.isCloudNativeTable()) {
                throw new SemanticException("Lake table does not support generated column");
            }

            if (((OlapTable) table).getKeysType() == KeysType.AGG_KEYS) {
                throw new SemanticException("Generated Column does not support AGG table");
            }

            clause.setRollupName(Strings.emptyToNull(clause.getRollupName()));

            Expr expr = columnDef.generatedColumnExpr();
            TableName tableName = new TableName(context.getDatabase(), table.getName());

            ExpressionAnalyzer.analyzeExpression(expr, new AnalyzeState(), new Scope(RelationId.anonymous(),
                    new RelationFields(table.getBaseSchema().stream().map(col -> new Field(col.getName(), col.getType(),
                                    tableName, null))
                            .collect(Collectors.toList()))), context);

            // check if contain aggregation
            List<FunctionCallExpr> funcs = Lists.newArrayList();
            expr.collect(FunctionCallExpr.class, funcs);
            for (FunctionCallExpr fn : funcs) {
                if (fn.isAggregateFunction()) {
                    throw new SemanticException("Generated Column don't support aggregation function");
                }
            }

            // check if the expression refers to other generated columns
            List<SlotRef> slots = Lists.newArrayList();
            expr.collect(SlotRef.class, slots);
            if (slots.size() != 0) {
                for (SlotRef slot : slots) {
                    Column refColumn = table.getColumn(slot.getColumnName());
                    if (refColumn.isGeneratedColumn()) {
                        throw new SemanticException("Expression can not refers to other generated columns: " +
                                refColumn.getName());
                    }
                    if (refColumn.isAutoIncrement()) {
                        throw new SemanticException("Expression can not refers to AUTO_INCREMENT columns: " +
                                refColumn.getName());
                    }
                }
            }

            if (!columnDef.getType().matchesType(expr.getType())) {
                throw new SemanticException("Illegal expression type for Generated Column " +
                        "Column Type: " + columnDef.getType().toString() +
                        ", Expression Type: " + expr.getType().toString());
            }
            clause.setColumn(columnDef.toColumn(table));
            return null;
        }

        ColumnPosition colPos = clause.getColPos();
        if (colPos != null) {
            try {
                colPos.analyze();
            } catch (AnalysisException e) {
                throw new SemanticException(PARSER_ERROR_MSG.invalidColumnPos(e.getMessage()), colPos.getPos());
            }
        }
        if (colPos != null && table instanceof OlapTable && colPos.getLastCol() != null) {
            Column afterColumn = table.getColumn(colPos.getLastCol());
            if (afterColumn.isGeneratedColumn()) {
                throw new SemanticException("Can not modify column after Generated Column");
            }
        }

        clause.setRollupName(Strings.emptyToNull(clause.getRollupName()));

        clause.setColumn(columnDef.toColumn(table));
        return null;
    }

    @Override
    public Void visitColumnRenameClause(ColumnRenameClause clause, ConnectContext context) {
        if (Strings.isNullOrEmpty(clause.getColName())) {
            throw new SemanticException("Column name is not set");
        }

        if (Strings.isNullOrEmpty(clause.getNewColName())) {
            throw new SemanticException("New column name is not set");
        }

        FeNameFormat.checkColumnName(clause.getNewColName());
        return null;
    }

    @Override
    public Void visitReorderColumnsClause(ReorderColumnsClause clause, ConnectContext context) {
        List<String> columnsByPos = clause.getColumnsByPos();
        if (columnsByPos == null || columnsByPos.isEmpty()) {
            throw new SemanticException("No column in reorder columns clause.");
        }
        for (String col : columnsByPos) {
            if (Strings.isNullOrEmpty(col)) {
                throw new SemanticException("Empty column in reorder columns.");
            }
        }
        clause.setRollupName(Strings.emptyToNull(clause.getRollupName()));
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
        newDataProperty = PropertyAnalyzer.analyzeDataProperty(properties,
                DataProperty.getInferredDefaultDataProperty(), false);
        Preconditions.checkNotNull(newDataProperty);

        // 2. replication num
        short newReplicationNum;
        newReplicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, RunMode.defaultReplicationNum());
        Preconditions.checkState(newReplicationNum != (short) -1);

        // 3. in memory
        PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);

        // 4. tablet type
        PropertyAnalyzer.analyzeTabletType(properties);
    }

    @Override
    public Void visitReplacePartitionClause(ReplacePartitionClause clause, ConnectContext context) {
        if (clause.getPartitionNames().stream().anyMatch(String::isEmpty)) {
            throw new SemanticException("there are empty partition name", clause.getPartition().getPos());
        }

        if (clause.getTempPartitionNames().stream().anyMatch(String::isEmpty)) {
            throw new SemanticException("there are empty partition name", clause.getTempPartition().getPos());
        }

        if (clause.getPartition().isTemp()) {
            throw new SemanticException("Only support replace partitions with temp partitions",
                    clause.getPartition().getPos());
        }

        if (!clause.getTempPartition().isTemp()) {
            throw new SemanticException("Only support replace partitions with temp partitions",
                    clause.getTempPartition().getPos());
        }

        clause.setStrictRange(PropertyAnalyzer.analyzeBooleanProp(
                clause.getProperties(), PropertyAnalyzer.PROPERTIES_STRICT_RANGE, true));
        clause.setUseTempPartitionName(PropertyAnalyzer.analyzeBooleanProp(
                clause.getProperties(), PropertyAnalyzer.PROPERTIES_USE_TEMP_PARTITION_NAME, false));

        if (clause.getProperties() != null && !clause.getProperties().isEmpty()) {
            throw new SemanticException("Unknown properties: " + clause.getProperties());
        }


        if (table instanceof OlapTable) {
            List<String> partitionNames = clause.getPartitionNames();
            for (String partitionName : partitionNames) {
                if (partitionName.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                    throw new SemanticException("Replace shadow partitions is not allowed");
                }
            }
        }

        return null;
    }

    @Override
    public Void visitPartitionRenameClause(PartitionRenameClause clause, ConnectContext context) {
        try {
            if (Strings.isNullOrEmpty(clause.getPartitionName())) {
                throw new AnalysisException("Partition name is not set");
            }

            if (Strings.isNullOrEmpty(clause.getNewPartitionName())) {
                throw new AnalysisException("New partition name is not set");
            }
            FeNameFormat.checkPartitionName(clause.getNewPartitionName());
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }
        return null;
    }

    @Override
    public Void visitAddRollupClause(AddRollupClause clause, ConnectContext context) {
        String rollupName = clause.getRollupName();
        FeNameFormat.checkTableName(rollupName);

        List<String> columnNames = clause.getColumnNames();
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
        clause.setBaseRollupName(Strings.emptyToNull(clause.getBaseRollupName()));
        return null;
    }

    @Override
    public Void visitDropRollupClause(DropRollupClause clause, ConnectContext context) {
        String rollupName = clause.getRollupName();
        if (Strings.isNullOrEmpty(rollupName)) {
            throw new SemanticException("No rollup in delete rollup.");
        }
        return null;
    }

    @Override
    public Void visitRollupRenameClause(RollupRenameClause clause, ConnectContext context) {
        String rollupName = clause.getRollupName();
        if (Strings.isNullOrEmpty(rollupName)) {
            throw new SemanticException("Rollup name is not set");
        }

        String newRollupName = clause.getNewRollupName();
        if (Strings.isNullOrEmpty(newRollupName)) {
            throw new SemanticException("New rollup name is not set");
        }

        FeNameFormat.checkTableName(newRollupName);
        return null;
    }

    @Override
    public Void visitCompactionClause(CompactionClause clause, ConnectContext context) {
        final List<String> partitionNames = clause.getPartitionNames();
        if (partitionNames.stream().anyMatch(Strings::isNullOrEmpty)) {
            throw new SemanticException("there are empty partition name");
        }
        return null;
    }

    @Override
    public Void visitRefreshSchemeClause(RefreshSchemeClause refreshSchemeDesc, ConnectContext context) {
        if (refreshSchemeDesc.getType() == MaterializedView.RefreshType.SYNC) {
            throw new SemanticException("Unsupported change to SYNC refresh type", refreshSchemeDesc.getPos());
        }
        if (refreshSchemeDesc instanceof AsyncRefreshSchemeDesc) {
            AsyncRefreshSchemeDesc async = (AsyncRefreshSchemeDesc) refreshSchemeDesc;
            final IntervalLiteral intervalLiteral = async.getIntervalLiteral();
            if (intervalLiteral != null) {
                long step = ((IntLiteral) intervalLiteral.getValue()).getLongValue();
                if (step <= 0) {
                    throw new SemanticException("Unsupported negative or zero step value: " + step,
                            async.getPos());
                }
                final String unit = intervalLiteral.getUnitIdentifier().getDescription().toUpperCase();
                try {
                    MaterializedViewAnalyzer.MaterializedViewAnalyzerVisitor.RefreshTimeUnit.valueOf(unit);
                } catch (IllegalArgumentException e) {
                    String msg = String.format("Unsupported interval unit: %s, only timeunit %s are supported", unit,
                            Arrays.asList(MaterializedViewAnalyzer.MaterializedViewAnalyzerVisitor.RefreshTimeUnit.values()));
                    throw new SemanticException(msg, intervalLiteral.getUnitIdentifier().getPos());
                }
            }
        }
        return null;
    }

    @Override
    public Void visitAlterMaterializedViewStatusClause(AlterMaterializedViewStatusClause clause, ConnectContext context) {
        String status = clause.getStatus();
        if (!AlterMaterializedViewStatusClause.SUPPORTED_MV_STATUS.contains(status)) {
            throw new SemanticException("Unsupported modification for materialized view status:" + status);
        }
        return null;
    }

    // ------------------------------------------- Alter partition clause ----------------------------------==--------------------

    @Override
    public Void visitAddPartitionClause(AddPartitionClause clause, ConnectContext context) {
        PartitionDescAnalyzer.analyze(clause.getPartitionDesc());

        if (table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            PartitionDescAnalyzer.analyzePartitionDescWithExistsTable(clause.getPartitionDesc(), olapTable);
        }

        List<PartitionDesc> partitionDescList = Lists.newArrayList();
        PartitionDesc partitionDesc = clause.getPartitionDesc();
        if (partitionDesc instanceof SingleItemListPartitionDesc
                || partitionDesc instanceof MultiItemListPartitionDesc
                || partitionDesc instanceof SingleRangePartitionDesc) {
            partitionDescList = Lists.newArrayList(partitionDesc);
        } else if (partitionDesc instanceof RangePartitionDesc) {
            partitionDescList = Lists.newArrayList(((RangePartitionDesc) partitionDesc).getSingleRangePartitionDescs());
        } else if (partitionDesc instanceof ListPartitionDesc) {
            partitionDescList = Lists.newArrayList(((ListPartitionDesc) partitionDesc).getPartitionDescs());
        } else if (partitionDesc instanceof MultiRangePartitionDesc) {
            if (!(table instanceof OlapTable)) {
                throw new SemanticException("Can't add multi-range partition to table type is not olap");
            }

            OlapTable olapTable = (OlapTable) table;
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();

            List<SingleRangePartitionDesc> singleRangePartitionDescs =
                    convertMultiRangePartitionDescToSingleRangePartitionDescs(
                            partitionInfo.isAutomaticPartition(),
                            olapTable.getTableProperty().getProperties(),
                            clause.isTempPartition(),
                            (MultiRangePartitionDesc) partitionDesc,
                            partitionInfo.getPartitionColumns(table.getIdToColumn()),
                            clause.getProperties());
            partitionDescList = singleRangePartitionDescs.stream()
                    .map(item -> (PartitionDesc) item).collect(Collectors.toList());
        }
        clause.setResolvedPartitionDescList(partitionDescList);

        if (table instanceof OlapTable) {
            try {
                OlapTable olapTable = (OlapTable) table;
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                analyzeAddPartition(olapTable, partitionDescList, clause, partitionInfo);
            } catch (DdlException | AnalysisException | NotImplementedException e) {
                throw new SemanticException(e.getMessage());
            }
        }

        return null;
    }

    private void analyzeAddPartition(OlapTable olapTable, List<PartitionDesc> partitionDescs,
                                     AddPartitionClause addPartitionClause, PartitionInfo partitionInfo)
            throws DdlException, AnalysisException, NotImplementedException {

        Set<String> existPartitionNameSet =
                CatalogUtils.checkPartitionNameExistForAddPartitions(olapTable, partitionDescs);
        // partition properties is prior to clause properties
        // clause properties is prior to table properties
        // partition properties should inherit table properties
        Map<String, String> properties = olapTable.getProperties();
        Map<String, String> clauseProperties = addPartitionClause.getProperties();
        if (clauseProperties != null && !clauseProperties.isEmpty()) {
            properties.putAll(clauseProperties);
        }

        for (PartitionDesc partitionDesc : partitionDescs) {
            Map<String, String> cloneProperties = Maps.newHashMap(properties);
            Map<String, String> sourceProperties = partitionDesc.getProperties();
            if (sourceProperties != null && !sourceProperties.isEmpty()) {
                cloneProperties.putAll(sourceProperties);
            }

            String storageCoolDownTTL = olapTable.getTableProperty()
                    .getProperties().get(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL);
            if (storageCoolDownTTL != null) {
                cloneProperties.putIfAbsent(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL, storageCoolDownTTL);
            }

            if (partitionDesc instanceof SingleRangePartitionDesc) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                SingleRangePartitionDesc singleRangePartitionDesc = ((SingleRangePartitionDesc) partitionDesc);
                singleRangePartitionDesc.analyze(rangePartitionInfo.getPartitionColumnsSize(), cloneProperties);
                if (!existPartitionNameSet.contains(singleRangePartitionDesc.getPartitionName())) {
                    rangePartitionInfo.checkAndCreateRange(table.getIdToColumn(), singleRangePartitionDesc,
                            addPartitionClause.isTempPartition());
                }
            } else if (partitionDesc instanceof SingleItemListPartitionDesc
                    || partitionDesc instanceof MultiItemListPartitionDesc) {
                List<ColumnDef> columnDefList = partitionInfo.getPartitionColumns(olapTable.getIdToColumn()).stream()
                        .map(item -> new ColumnDef(item.getName(), new TypeDef(item.getType())))
                        .collect(Collectors.toList());
                PartitionDescAnalyzer.analyze(partitionDesc);
                partitionDesc.analyze(columnDefList, cloneProperties);
                if (!existPartitionNameSet.contains(partitionDesc.getPartitionName())) {
                    CatalogUtils.checkPartitionValuesExistForAddListPartition(olapTable, partitionDesc,
                            addPartitionClause.isTempPartition());
                }
            } else {
                throw new DdlException("Only support adding partition to range/list partitioned table");
            }
        }
    }

    private List<SingleRangePartitionDesc> convertMultiRangePartitionDescToSingleRangePartitionDescs(
            boolean isAutoPartitionTable,
            Map<String, String> tableProperties,
            boolean isTempPartition,
            MultiRangePartitionDesc partitionDesc,
            List<Column> partitionColumns,
            Map<String, String> properties) {
        Column firstPartitionColumn = partitionColumns.get(0);

        if (properties == null) {
            properties = Maps.newHashMap();
        }
        if (tableProperties != null && tableProperties.containsKey(DynamicPartitionProperty.START_DAY_OF_WEEK)) {
            properties.put(DynamicPartitionProperty.START_DAY_OF_WEEK,
                    tableProperties.get(DynamicPartitionProperty.START_DAY_OF_WEEK));
        }
        PartitionConvertContext context = new PartitionConvertContext();
        context.setAutoPartitionTable(isAutoPartitionTable);
        context.setFirstPartitionColumnType(firstPartitionColumn.getType());
        context.setProperties(properties);
        context.setTempPartition(isTempPartition);
        List<SingleRangePartitionDesc> singleRangePartitionDescs;
        try {
            singleRangePartitionDescs = partitionDesc.convertToSingle(context);
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }
        return singleRangePartitionDescs;
    }

    @Override
    public Void visitDropPartitionClause(DropPartitionClause clause, ConnectContext context) {
        if (clause.getMultiRangePartitionDesc() != null) {
            MultiRangePartitionDesc multiRangePartitionDesc = clause.getMultiRangePartitionDesc();
            PartitionDescAnalyzer.analyze(multiRangePartitionDesc);

            if (!(table instanceof OlapTable)) {
                throw new SemanticException("Can't add multi-range partition to table type is not olap");
            }
            OlapTable olapTable = (OlapTable) table;
            PartitionDescAnalyzer.analyzePartitionDescWithExistsTable(multiRangePartitionDesc, olapTable);

            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            List<SingleRangePartitionDesc> singleRangePartitionDescs =
                    convertMultiRangePartitionDescToSingleRangePartitionDescs(
                            partitionInfo.isAutomaticPartition(),
                            olapTable.getTableProperty().getProperties(),
                            clause.isTempPartition(),
                            multiRangePartitionDesc,
                            partitionInfo.getPartitionColumns(olapTable.getIdToColumn()),
                            null);

            clause.setResolvedPartitionNames(singleRangePartitionDescs.stream()
                    .map(SinglePartitionDesc::getPartitionName).collect(Collectors.toList()));
        } else if (clause.getPartitionName() != null) {
            clause.setResolvedPartitionNames(Lists.newArrayList(clause.getPartitionName()));
        } else if (clause.getPartitionNames() != null) {
            clause.setResolvedPartitionNames(clause.getPartitionNames());
        }

        if (table instanceof OlapTable) {
            if (clause.getPartitionName() != null && clause.getPartitionName().startsWith(
                    ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                throw new SemanticException("Deletion of shadow partitions is not allowed");
            }
            List<String> partitionNames = clause.getPartitionNames();
            if (CollectionUtils.isNotEmpty(partitionNames)) {
                boolean hasShadowPartition = partitionNames.stream().anyMatch(partitionName ->
                        partitionName.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX));
                if (hasShadowPartition) {
                    throw new SemanticException("Deletion of shadow partitions is not allowed");
                }
            }
        }

        return null;
    }

    @Override
    public Void visitAlterTableOperationClause(AlterTableOperationClause clause, ConnectContext context) {
        String tableOperationName = clause.getTableOperationName();
        if (tableOperationName == null) {
            throw new SemanticException("Table operation name should be null");
        }

        List<ConstantOperator> args = new ArrayList<>();
        for (Expr expr : clause.getExprs()) {
            ScalarOperator result;
            try {
                Scope scope = new Scope(RelationId.anonymous(), new RelationFields());
                ExpressionAnalyzer.analyzeExpression(expr, new AnalyzeState(), scope, context);
                ExpressionMapping expressionMapping = new ExpressionMapping(scope);
                result = SqlToScalarOperatorTranslator.translate(expr, expressionMapping, new ColumnRefFactory());
                if (result instanceof ConstantOperator) {
                    args.add((ConstantOperator) result);
                } else {
                    throw new SemanticException("invalid arg " + expr);
                }
            } catch (Exception e) {
                throw new SemanticException("Failed to resolve table operation args %s. msg: %s", expr, e.getMessage());
            }
        }
        clause.setArgs(args);
        return null;
    }
}
