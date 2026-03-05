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
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnBuilder;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
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
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.connector.iceberg.procedure.IcebergTableProcedure;
import com.starrocks.connector.iceberg.procedure.NamedArgument;
import com.starrocks.lake.LakeTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.service.PartitionMeasure;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AddFieldClause;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AddRollupClause;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterMaterializedViewStatusClause;
import com.starrocks.sql.ast.AlterTableAutoIncrementClause;
import com.starrocks.sql.ast.AlterTableModifyDefaultBucketsClause;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ColumnPosition;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.CompactionClause;
import com.starrocks.sql.ast.CreateIndexClause;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.DropFieldClause;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.DropRollupClause;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.IndexDef.IndexType;
import com.starrocks.sql.ast.KeysDesc;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.MergeTabletClause;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.ModifyColumnCommentClause;
import com.starrocks.sql.ast.ModifyPartitionClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.MultiRangePartitionDesc;
import com.starrocks.sql.ast.OptimizeClause;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.PartitionConvertContext;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.PartitionRenameClause;
import com.starrocks.sql.ast.ProcedureArgument;
import com.starrocks.sql.ast.RandomDistributionDesc;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.RefreshSchemeClause;
import com.starrocks.sql.ast.ReorderColumnsClause;
import com.starrocks.sql.ast.ReplacePartitionClause;
import com.starrocks.sql.ast.RollupRenameClause;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.sql.ast.SinglePartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.sql.ast.StructFieldDesc;
import com.starrocks.sql.ast.SyncRefreshSchemeDesc;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.IntervalLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.partition.PartitionSelector;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import org.apache.commons.collections4.CollectionUtils;

import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.parser.ErrorMsgProxy.PARSER_ERROR_MSG;

public class AlterTableClauseAnalyzer implements AstVisitorExtendInterface<Void, ConnectContext> {
    protected final Table table;

    public AlterTableClauseAnalyzer(Table table) {
        this.table = table;
    }

    public void analyze(ConnectContext session, AlterClause statement) {
        visit(statement, session);
    }

    @Override
    public Void visitCreateIndexClause(CreateIndexClause clause, ConnectContext context) {
        IndexAnalyzer.analyze(clause.getIndexDef());
        return null;
    }

    @Override
    public Void visitAlterTableModifyDefaultBucketsClause(AlterTableModifyDefaultBucketsClause clause, ConnectContext context) {
        if (!(table instanceof OlapTable)) {
            throw new SemanticException("Only support OLAP table");
        }
        OlapTable tbl = (OlapTable) table;
        if (!(tbl.getDefaultDistributionInfo() instanceof HashDistributionInfo)) {
            throw new SemanticException("Only support hash distribution tables");
        }
        HashDistributionInfo current = (HashDistributionInfo) tbl.getDefaultDistributionInfo();
        List<Column> cols = MetaUtils.getColumnsByColumnIds(tbl, current.getDistributionColumns());
        List<String> currentNames = cols.stream().map(c -> c.getName()).collect(Collectors.toList());
        List<String> input = clause.getDistributionColumns();
        if (currentNames.size() != input.size()) {
            throw new SemanticException("Distribution columns mismatch: " + input + " vs " + currentNames);
        }
        for (int i = 0; i < currentNames.size(); i++) {
            if (!currentNames.get(i).equalsIgnoreCase(input.get(i))) {
                throw new SemanticException("Distribution columns mismatch: " + input + " vs " + currentNames);
            }
        }
        if (clause.getBucketNum() <= 0) {
            throw new SemanticException("Bucket num must > 0");
        }
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
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL)) {
            PropertyAnalyzer.analyzePartitionTTL(properties, false);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION)) {
            // do nothing
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT)) {
            // do nothing
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
            properties.remove(PropertyAnalyzer.PROPERTIES_INMEMORY);
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
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PERSISTENT_INDEX_TYPE)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_PERSISTENT_INDEX_TYPE).equalsIgnoreCase("CLOUD_NATIVE") &&
                    !properties.get(PropertyAnalyzer.PROPERTIES_PERSISTENT_INDEX_TYPE).equalsIgnoreCase("LOCAL")) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Property " + PropertyAnalyzer.PROPERTIES_PERSISTENT_INDEX_TYPE +
                                " must be CLOUD_NATIVE or LOCAL");
            }
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FILE_BUNDLING)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_FILE_BUNDLING).equalsIgnoreCase("true") &&
                    !properties.get(PropertyAnalyzer.PROPERTIES_FILE_BUNDLING).equalsIgnoreCase("false")) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Property " + PropertyAnalyzer.PROPERTIES_FILE_BUNDLING +
                                " must be bool type(false/true)");
            }

            if (!table.isCloudNativeTable()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Property " + PropertyAnalyzer.PROPERTIES_FILE_BUNDLING +
                                " only support cloud native table");
            }

            boolean fileBundling = properties.get(
                    PropertyAnalyzer.PROPERTIES_FILE_BUNDLING).equalsIgnoreCase("true");
            OlapTable olapTable = (OlapTable) table;
            if (fileBundling == olapTable.isFileBundling()) {
                String msg = String.format("table: %s file_bundling is %s, nothing need to do",
                        olapTable.getName(), fileBundling);
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, msg);
            }

            if (!olapTable.allowUpdateFileBundling()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Property " + PropertyAnalyzer.PROPERTIES_FILE_BUNDLING +
                                " cannot be updated now because this table contains mixed metadata types " +
                                "(both split and aggregate). Please wait until old metadata versions are vacuumed");
            }

            if (!((LakeTable) olapTable).checkLakeRollupAllowFileBundling()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Property " + PropertyAnalyzer.PROPERTIES_FILE_BUNDLING +
                                " cannot be updated now because this table contains LakeRollup created in old version." +
                                " You can rebuild the Rollup and retry");
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
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_STATISTIC_COLLECT_ON_FIRST_LOAD)) {
            PropertyAnalyzer.analyzeEnableStatisticCollectOnFirstLoad(properties);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BASE_COMPACTION_FORBIDDEN_TIME_RANGES)) {
            if (table instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) table;
                if (olapTable.getKeysType() == KeysType.PRIMARY_KEYS
                        || olapTable.isCloudNativeTableOrMaterializedView()) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Property " + PropertyAnalyzer.PROPERTIES_BASE_COMPACTION_FORBIDDEN_TIME_RANGES +
                                    " not support primary keys table or cloud native table");
                }
            }
            PropertyAnalyzer.analyzeBaseCompactionForbiddenTimeRanges(properties);
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
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT)) {
            try {
                String timeDriftConstraintSpec = properties.get(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT);
                PropertyAnalyzer.analyzeTimeDriftConstraint(timeDriftConstraintSpec, table, properties);
            } catch (Throwable e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
            }
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE)) {
            // Allow setting flat_json.enable to true or false
            PropertyAnalyzer.analyzeFlatJsonEnabled(properties);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR) ||
                properties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR) ||
                properties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX)) {
            if (table instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) table;
                if (olapTable.getFlatJsonConfig() != null && olapTable.getFlatJsonConfig().getFlatJsonEnable()) {
                    if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR)) {
                        PropertyAnalyzer.analyzeFlatJsonNullFactor(properties);
                    } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR)) {
                        PropertyAnalyzer.analyzeFlatJsonSparsityFactor(properties);
                    } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX)) {
                        PropertyAnalyzer.analyzeFlatJsonColumnMax(properties);
                    }
                } else {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Property " + PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE +
                                    " haven't been enabled");
                }
            }
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_COMPACTION_STRATEGY)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_COMPACTION_STRATEGY).equalsIgnoreCase("default") &&
                    !properties.get(PropertyAnalyzer.PROPERTIES_COMPACTION_STRATEGY).equalsIgnoreCase("real_time")) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "unknown compaction strategy: " +
                        properties.get(PropertyAnalyzer.PROPERTIES_COMPACTION_STRATEGY));
            }
            if (!table.isCloudNativeTableOrMaterializedView()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Property " + PropertyAnalyzer.PROPERTIES_COMPACTION_STRATEGY +
                                " can be only set to the cloud native table");
            }

            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getKeysType() != KeysType.PRIMARY_KEYS) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "The compaction strategy can be only " +
                        "update for a primary key table. ");
            }
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2)) {
            PropertyAnalyzer.analyzeCloudNativeFastSchemaEvolutionV2(table.getType(), properties, false);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL)) {
            if (!table.isCloudNativeTableOrMaterializedView()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Property " + PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL +
                                " can only be set for cloud native tables");
            }
            String value = properties.get(PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL);
            try {
                int maxParallel = Integer.parseInt(value);
                if (maxParallel < 0) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Invalid lake_compaction_max_parallel value: " + value + ". Value must be non-negative.");
                }
            } catch (NumberFormatException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Invalid lake_compaction_max_parallel value: " + value + ". Value must be an integer.");
            }
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT)) {
            try {
                PropertyAnalyzer.analyzeTableQueryTimeout(Maps.newHashMap(properties));
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
            }
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE).equalsIgnoreCase("true") &&
                    !properties.get(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE).equalsIgnoreCase("false")) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Property " + PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE +
                                " must be bool type(false/true)");
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

        if (olapTable.getAutomaticBucketSize() > 0 && clause.getPartitionDesc() == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "Random distribution table already supports automatic scaling and does not require optimization");
        }

        // set the sort keys into OptimizeClause
        List<String> sortKeys = genOptimizeClauseSortKeys(clause);
        clause.setSortKeys(sortKeys);

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

        // analyze partition desc
        PartitionDesc partitionDesc = clause.getPartitionDesc();
        if (partitionDesc != null) {
            if (!(partitionDesc instanceof ExpressionPartitionDesc)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Unsupported partition type for merge partitions");
            }
            ExpressionPartitionDesc expressionPartitionDesc = (ExpressionPartitionDesc) partitionDesc;
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) expressionPartitionDesc.getExpr();
            String functionName = functionCallExpr.getFunctionName();
            if (!FunctionSet.DATE_TRUNC.equals(functionName) && !FunctionSet.TIME_SLICE.equals(functionName)) {
                ErrorReport.reportSemanticException("Unsupported change to %s partition function when merge partitions",
                        ErrorCode.ERR_COMMON_ERROR, functionName);
            }
            if (clause.getDistributionDesc() != null) {
                DistributionDesc defaultDistributionDesc = olapTable.getDefaultDistributionInfo()
                        .toDistributionDesc(olapTable.getIdToColumn());
                if (DistributionDescAnalyzer.isDifferentDistributionType(clause.getDistributionDesc(),
                        olapTable.getDefaultDistributionInfo())) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Unsupported change distribution type when merge partitions");
                }
                if (defaultDistributionDesc instanceof HashDistributionDesc) {
                    HashDistributionDesc hashDistributionDesc = (HashDistributionDesc) defaultDistributionDesc;
                    if (!hashDistributionDesc.getDistributionColumnNames().equals(
                            ((HashDistributionDesc) clause.getDistributionDesc()).getDistributionColumnNames())) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                                "Unsupported change distribution column when merge partitions");
                    }
                }
            }
            if (clause.getPartitionNames() != null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Unsupported specify partitions when merge partitions");
            }
            PartitionMeasure alterMeasure = null;
            try {
                alterMeasure = AnalyzerUtils.checkAndGetPartitionMeasure(functionCallExpr);
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
            }
            if (alterMeasure.getInterval() != 1) {
                ErrorReport.reportSemanticException("Unsupported partition interval %s when merge partitions",
                        ErrorCode.ERR_COMMON_ERROR, alterMeasure.getInterval());
            }

            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (!(partitionInfo instanceof ExpressionRangePartitionInfo)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Unsupported table partition type when merge partitions");
            }
            ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            List<Expr> partitionExprs = expressionRangePartitionInfo.getPartitionExprs(olapTable.getIdToColumn());
            if (partitionExprs.size() != 1) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Unsupported partition type when merge partitions");
            }
            Expr expr = partitionExprs.get(0);
            PartitionMeasure originMeasure = null;
            try {
                originMeasure = AnalyzerUtils.checkAndGetPartitionMeasure(expr);
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
            }

            if (!AnalyzerUtils.isGranularityGreater(alterMeasure.getGranularity(), originMeasure.getGranularity())) {
                ErrorReport.reportSemanticException("Unsupported from granularity %s to granularity %s when merge partitions",
                        ErrorCode.ERR_COMMON_ERROR, originMeasure.getGranularity(), alterMeasure.getGranularity());
            }

            try {
                PartitionDescAnalyzer.analyze(expressionPartitionDesc, columnDefs, olapTable.getProperties());
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
            }
            clause.setSourcePartitionIds(olapTable.getVisiblePartitions().stream()
                    .map(Partition::getId).collect(Collectors.toList()));

            return null;
        }

        // analyze distribution
        DistributionDesc distributionDesc = clause.getDistributionDesc();
        if (distributionDesc != null) {
            if (distributionDesc instanceof RandomDistributionDesc && targetKeysType != KeysType.DUP_KEYS) {
                throw new SemanticException(targetKeysType.toSql() + " must use hash distribution", distributionDesc.getPos());
            }
            DistributionDescAnalyzer.analyze(distributionDesc, columnSet);
            clause.setDistributionDesc(distributionDesc);

            if (DistributionDescAnalyzer.isDifferentDistributionType(distributionDesc, olapTable.getDefaultDistributionInfo())
                    && clause.getPartitionNames() != null) {
                throw new SemanticException("not support change distribution type when specify partitions");
            }
        }

        // analyze partitions
        PartitionRef partitionNames = clause.getPartitionNames();
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

            Set<Long> partitionIds = Sets.newHashSet();
            for (String partitionName : partitionNameList) {
                Partition partition = olapTable.getPartition(partitionName);
                if (partition == null) {
                    throw new SemanticException("partition %s does not exist", partitionName);
                }
                partitionIds.add(partition.getId());
            }
            clause.setSourcePartitionIds(Lists.newArrayList(partitionIds));
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
            if (table.isOlapOrCloudNativeTable() && ((OlapTable) table).getKeysType() == KeysType.PRIMARY_KEYS) {
                columnDef.setAggregateType(AggregateType.REPLACE);
            }
            ColumnDefAnalyzer.analyze(columnDef, true);
        } catch (AnalysisException e) {
            throw new SemanticException(PARSER_ERROR_MSG.invalidColumnDef(e.getMessage()), columnDef.getPos());
        }

        if (columnDef.getType().isTime()) {
            throw new SemanticException("Unsupported data type: TIME");
        }

        if (columnDef.isGeneratedColumn()) {
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
            if (colPos != ColumnPosition.FIRST && Strings.isNullOrEmpty(colPos.getLastCol())) {
                throw new SemanticException("Column is empty.");
            }
        }

        if (columnDef.getAggregateType() != null && colPos != null && colPos.isFirst()) {
            throw new SemanticException("Cannot add value column[" + columnDef.getName() + "] at first",
                    columnDef.getPos());
        }

        // Make sure return null if rollup name is empty.
        clause.setRollupName(Strings.emptyToNull(clause.getRollupName()));

        Column column = ColumnBuilder.buildColumn(columnDef);
        if (!column.isAllowNull() && column.getDefaultValue() == null && column.getDefaultExpr() == null) {
            throw new SemanticException(PARSER_ERROR_MSG.withOutDefaultVal(column.getName()), columnDef.getPos());
        }
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
                if (table.isOlapOrCloudNativeTable() && ((OlapTable) table).getKeysType() == KeysType.PRIMARY_KEYS) {
                    colDef.setAggregateType(AggregateType.REPLACE);
                }
                ColumnDefAnalyzer.analyze(colDef, true);
            } catch (AnalysisException e) {
                throw new SemanticException(PARSER_ERROR_MSG.invalidColumnDef(e.getMessage()), colDef.getPos());
            }
            if (colDef.isGeneratedColumn()) {
                hasGeneratedColumn = true;

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

        if (!hasGeneratedColumn) {
            columnDefs.forEach(columnDef -> {
                Column column = ColumnBuilder.buildColumn(columnDef);
                if (!column.isAllowNull() && column.getDefaultValue() == null && column.getDefaultExpr() == null) {
                    throw new SemanticException(PARSER_ERROR_MSG.withOutDefaultVal(column.getName()),
                            columnDef.getPos());
                }
            });
        }
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
        StructFieldDescAnalyzer.analyze(fieldDesc, baseColumn, false);
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
        StructFieldDescAnalyzer.analyze(fieldDesc, baseColumn, true);
        return null;
    }

    @Override
    public Void visitModifyColumnClause(ModifyColumnClause clause, ConnectContext context) {
        ColumnDef columnDef = clause.getColumnDef();
        if (columnDef == null) {
            throw new SemanticException("No column definition in modify column clause.");
        }
        try {
            if (table.isOlapOrCloudNativeTable() && ((OlapTable) table).getKeysType() == KeysType.PRIMARY_KEYS) {
                columnDef.setAggregateType(AggregateType.REPLACE);
            }
            ColumnDefAnalyzer.analyze(columnDef, true);
        } catch (AnalysisException e) {
            throw new SemanticException(PARSER_ERROR_MSG.invalidColumnDef(e.getMessage()), columnDef.getPos());
        }

        if (columnDef.getType().isTime()) {
            throw new SemanticException("Unsupported data type: TIME");
        }

        if (columnDef.isGeneratedColumn()) {
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
            return null;
        }

        ColumnPosition colPos = clause.getColPos();
        if (colPos != null) {
            if (colPos != ColumnPosition.FIRST && Strings.isNullOrEmpty(colPos.getLastCol())) {
                throw new SemanticException("Column is empty.");
            }
        }
        if (colPos != null && table instanceof OlapTable && colPos.getLastCol() != null) {
            Column afterColumn = table.getColumn(colPos.getLastCol());
            if (afterColumn.isGeneratedColumn()) {
                throw new SemanticException("Can not modify column after Generated Column");
            }
        }

        clause.setRollupName(Strings.emptyToNull(clause.getRollupName()));
        return null;
    }

    @Override
    public Void visitModifyColumnCommentClause(ModifyColumnCommentClause clause, ConnectContext context) {
        if (Strings.isNullOrEmpty(clause.getColumnName())) {
            throw new SemanticException("Column name is not set");
        }
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
    // 5. datacache.enable
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

        // 5. datacache.enable (validate bool value if present)
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE)) {
            String value = properties.get(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE);
            if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
                throw new AnalysisException("Property " + PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE
                        + " must be bool type(false/true)");
            }
        }
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
        if (refreshSchemeDesc instanceof SyncRefreshSchemeDesc) {
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

    @Override
    public Void visitSplitTabletClause(SplitTabletClause clause, ConnectContext context) {
        if (!table.isCloudNativeTableOrMaterializedView()) {
            throw new SemanticException("Split tablet only support cloud native tables");
        }

        if (clause.getPartitionNames() != null && clause.getTabletList() != null) {
            throw new SemanticException("Partitions and tablets cannot be specified at the same time");
        }

        if (clause.getPartitionNames() != null) {
            if (clause.getPartitionNames().isTemp()) {
                throw new SemanticException("Cannot split tablet in temp partition");
            }
            if (clause.getPartitionNames().getPartitionNames().isEmpty()) {
                throw new SemanticException("Empty partitions");
            }
        }

        if (clause.getTabletList() != null && clause.getTabletList().getTabletIds().isEmpty()) {
            throw new SemanticException("Empty tablets");
        }

        Map<String, String> copiedProperties = clause.getProperties() == null ? Maps.newHashMap()
                : Maps.newHashMap(clause.getProperties());
        try {
            long tabletReshardTargetSize = PropertyAnalyzer.analyzeTabletReshardTargetSize(copiedProperties, true);
            clause.setTabletReshardTargetSize(tabletReshardTargetSize);
        } catch (Exception e) {
            throw new SemanticException(e.getMessage(), e);
        }

        if (!copiedProperties.isEmpty()) {
            throw new SemanticException("Unknown properties: " + copiedProperties);
        }

        return null;
    }

    @Override
    public Void visitMergeTabletClause(MergeTabletClause clause, ConnectContext context) {
        if (!table.isCloudNativeTableOrMaterializedView()) {
            throw new SemanticException("Merge tablet only support cloud native tables");
        }

        if (clause.getPartitionNames() != null && clause.getTabletGroupList() != null) {
            throw new SemanticException("Partitions and tablets cannot be specified at the same time");
        }

        if (clause.getPartitionNames() != null) {
            if (clause.getPartitionNames().isTemp()) {
                throw new SemanticException("Cannot merge tablet in temp partition");
            }
            if (clause.getPartitionNames().getPartitionNames().isEmpty()) {
                throw new SemanticException("Empty partitions");
            }
        }

        if (clause.getTabletGroupList() != null) {
            if (clause.getTabletGroupList().getTabletIdGroups().isEmpty()) {
                throw new SemanticException("Empty tablets");
            }
            for (List<Long> tabletIds : clause.getTabletGroupList().getTabletIdGroups()) {
                if (tabletIds.isEmpty()) {
                    throw new SemanticException("Empty tablets");
                }
                if (tabletIds.size() < 2) {
                    throw new SemanticException("Tablet list must contain at least 2 tablets");
                }
            }
        }

        Map<String, String> copiedProperties = clause.getProperties() == null ? Maps.newHashMap()
                : Maps.newHashMap(clause.getProperties());
        try {
            long tabletReshardTargetSize = PropertyAnalyzer.analyzeTabletReshardTargetSize(copiedProperties, true);
            clause.setTabletReshardTargetSize(tabletReshardTargetSize);
        } catch (Exception e) {
            throw new SemanticException(e.getMessage(), e);
        }

        if (!copiedProperties.isEmpty()) {
            throw new SemanticException("Unknown properties: " + copiedProperties);
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
                upgradeDeprecatedSingleItemListPartitionDesc(olapTable, partitionDescList, clause, partitionInfo);
                analyzeAddPartition(olapTable, partitionDescList, clause, partitionInfo);
            } catch (DdlException | AnalysisException | NotImplementedException e) {
                throw new SemanticException(e.getMessage(), e);
            }
        }

        return null;
    }

    /**
     * {@link SingleItemListPartitionDesc}
     */
    private void upgradeDeprecatedSingleItemListPartitionDesc(OlapTable table,
                                                              List<PartitionDesc> partitionDescs,
                                                              AddPartitionClause addPartitionClause,
                                                              PartitionInfo partitionInfo) throws AnalysisException {
        if (!partitionInfo.isListPartition()) {
            return;
        }
        ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
        boolean addSingleColumnPartition = partitionDescs.get(0) instanceof SingleItemListPartitionDesc;
        if (addSingleColumnPartition &&
                !listPartitionInfo.isMultiColumnPartition() &&
                listPartitionInfo.isDeFactoMultiItemPartition()) {
            Preconditions.checkState(partitionDescs.size() == 1);
            MultiItemListPartitionDesc newDesc =
                    ((SingleItemListPartitionDesc) partitionDescs.get(0)).upgradeToMultiItem();
            partitionDescs.set(0, newDesc);
            addPartitionClause.setPartitionDesc(newDesc);
        }
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

        List<String> rangePartitionNames = null;
        if (addPartitionClause.getPartitionDesc() instanceof RangePartitionDesc) {
            rangePartitionNames =
                    ((RangePartitionDesc) addPartitionClause.getPartitionDesc()).getPartitionNames();
        }

        Iterator<PartitionDesc> iterator = partitionDescs.iterator();
        while (iterator.hasNext()) {
            PartitionDesc partitionDesc = iterator.next();
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
                PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(singleRangePartitionDesc,
                        rangePartitionInfo.getPartitionColumnsSize(), cloneProperties);
                if (!existPartitionNameSet.contains(singleRangePartitionDesc.getPartitionName())) {
                    if (singleRangePartitionDesc.isSystem()) {
                        long enclosingId = rangePartitionInfo.getEnclosingPartitionId(
                                table.getIdToColumn(), singleRangePartitionDesc,
                                addPartitionClause.isTempPartition());
                        if (enclosingId >= 0) {
                            Partition enclosingPartition = olapTable.getPartition(enclosingId);
                            if (enclosingPartition != null && rangePartitionNames != null) {
                                int idx = rangePartitionNames.indexOf(
                                        singleRangePartitionDesc.getPartitionName());
                                if (idx >= 0) {
                                    rangePartitionNames.set(idx, enclosingPartition.getName());
                                }
                            }
                            iterator.remove();
                            continue;
                        }
                    }
                    rangePartitionInfo.checkAndCreateRange(table.getIdToColumn(), singleRangePartitionDesc,
                            addPartitionClause.isTempPartition());
                }
            } else if (partitionDesc instanceof SingleItemListPartitionDesc
                    || partitionDesc instanceof MultiItemListPartitionDesc) {
                List<ColumnDef> columnDefList = partitionInfo.getPartitionColumns(olapTable.getIdToColumn()).stream()
                        .map(item -> new ColumnDef(item.getName(), new TypeDef(item.getType())))
                        .collect(Collectors.toList());
                PartitionDescAnalyzer.analyze(partitionDesc, columnDefList, cloneProperties);
                if (!existPartitionNameSet.contains(partitionDesc.getPartitionName())) {
                    boolean isDuplicate = CatalogUtils.checkPartitionValuesExistForAddListPartition(olapTable,
                            partitionDesc, addPartitionClause.isTempPartition());
                    if (isDuplicate) {
                        if (partitionDesc.isSystem()) {
                            // For system-created partitions (automatic partition), skip if values already exist.
                            // duplicate partition will be ignored in create partition phase
                            continue;
                        } else {
                            // For user-created partitions, throw error
                            throw new DdlException("Duplicate partition values exist for partition: " +
                                    partitionDesc.getPartitionName());
                        }
                    }
                }
            } else {
                throw new DdlException("Only support adding partition to range/list partitioned table");
            }
        }

        if (rangePartitionNames != null) {
            LinkedHashSet<String> deduped = new LinkedHashSet<>(rangePartitionNames);
            rangePartitionNames.clear();
            rangePartitionNames.addAll(deduped);
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

    /**
     * @param statement : optimize clause statement
     * @return : Generate the `sortKeys` of optimize clause based on its `orderByElements`
     * from optimize clause statement.
     */
    private List<String> genOptimizeClauseSortKeys(OptimizeClause statement) {
        List<OrderByElement> orderByElements = statement.getOrderByElements();
        if (orderByElements == null) {
            return null;
        }
        List<String> sortKeys = new ArrayList<>();
        for (OrderByElement orderByElement : orderByElements) {
            Expr expr = orderByElement.getExpr();
            String column = expr instanceof SlotRef ? ((SlotRef) expr).getColumnName() : null;
            if (column == null) {
                throw new SemanticException("Unknown column '%s' in order by clause", ExprToSql.toSql(orderByElement.getExpr()));
            }
            sortKeys.add(column);
        }
        return sortKeys;
    }

    @Override
    public Void visitDropPartitionClause(DropPartitionClause clause, ConnectContext context) {
        if (clause.getMultiRangePartitionDesc() != null) {
            if (!(table instanceof OlapTable)) {
                throw new SemanticException("Can't drop partitions with multi-range since it is not olap table");
            }
            OlapTable olapTable = (OlapTable) table;
            MultiRangePartitionDesc multiRangePartitionDesc = clause.getMultiRangePartitionDesc();
            PartitionDescAnalyzer.analyze(multiRangePartitionDesc);
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
        } else if (clause.getDropWhereExpr() != null) {
            // do check drop partition expression
            if (!(table instanceof OlapTable)) {
                throw new SemanticException("Can't drop partitions with where expression since it is not olap table");
            }
            OlapTable olapTable = (OlapTable) table;
            if (!olapTable.getPartitionInfo().isPartitioned()) {
                throw new SemanticException("Can't drop partitions with where expression since it is not a partition table");
            }
            if (clause.isTempPartition()) {
                throw new SemanticException("Can't drop temp partitions with where expression and `TEMPORARY` keyword");
            }
            if (clause.isSetIfExists()) {
                throw new SemanticException("Can't drop partitions with where expression and `IF EXISTS` keyword");
            }
            Expr expr = clause.getDropWhereExpr();
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getDb(context, context.getCurrentCatalog(), context.getDatabase());
            TableName tableName = new TableName(db.getFullName(), table.getName());
            List<String> dropPartitionNames = PartitionSelector.getPartitionNamesByExpr(context, tableName,
                    olapTable, expr, true);
            clause.setResolvedPartitionNames(dropPartitionNames);
        } else if (clause.isDropAll()) {
            if (!(table instanceof OlapTable)) {
                throw new SemanticException("Can't drop all partitions since it is not olap table");
            }
            if (!clause.isTempPartition()) {
                throw new SemanticException("Can't drop all partitions since it is not temp partition");
            }
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
            throw new SemanticException("Table operation name should not be null");
        }

        if (!(table instanceof IcebergTable icebergTable)) {
            throw new SemanticException("Alter table operation is only supported for Iceberg tables");
        }

        IcebergTableProcedure icebergTableProcedure = icebergTable.getTableProcedure(tableOperationName);
        if (icebergTableProcedure == null) {
            throw new SemanticException("Unknown table operation: " + tableOperationName);
        }
        clause.setTableProcedure(icebergTableProcedure);

        // check named and unnamed arguments are mixing used.
        List<ProcedureArgument> tableOperationArgs = clause.getArguments();
        boolean anyNamedArgs = tableOperationArgs.stream().anyMatch(arg -> arg.getName().isPresent());
        boolean allNamedArgs = tableOperationArgs.stream().allMatch(arg -> arg.getName().isPresent());
        if (anyNamedArgs && !allNamedArgs) {
            throw new SemanticException("Mixing named and positional arguments is not allowed");
        }
        List<ProcedureArgument> lowerTableOperationArgs = tableOperationArgs.stream().map(arg -> {
            if (arg.getName().isPresent()) {
                return new ProcedureArgument(arg.getName().get().toLowerCase(), arg.getValue());
            } else {
                return arg;
            }
        }).toList();

        List<NamedArgument> procedureArgs = icebergTableProcedure.getArguments();
        Map<String, NamedArgument> tableProcedureArgumentMap = new HashMap<>();
        for (NamedArgument procedureArg : procedureArgs) {
            tableProcedureArgumentMap.put(procedureArg.getName(), procedureArg);
        }

        // convert table operation arguments to named arguments
        LinkedHashMap<String, ProcedureArgument> tableOperationNamedArgs = new LinkedHashMap<>();
        for (int index = 0; index < lowerTableOperationArgs.size(); ++index) {
            ProcedureArgument tableOperationArgument = lowerTableOperationArgs.get(index);
            if (tableOperationArgument.getName().isPresent()) {
                String name = tableOperationArgument.getName().get();
                NamedArgument argument = tableProcedureArgumentMap.get(name);
                if (argument == null) {
                    throw new SemanticException("Unknown argument name: " + name);
                }
                if (tableOperationNamedArgs.put(name, tableOperationArgument) != null) {
                    throw new SemanticException("Duplicate argument name: " + name);
                }
            } else if (index < procedureArgs.size()) {
                tableOperationNamedArgs.put(procedureArgs.get(index).getName(), tableOperationArgument);
            } else {
                throw new SemanticException("Too many arguments provided, expected at most %d, got %d",
                        procedureArgs.size(), lowerTableOperationArgs.size());
            }
        }

        // check if all required arguments are provided
        procedureArgs.forEach(arg -> {
            if (arg.isRequired() && !tableOperationNamedArgs.containsKey(arg.getName())) {
                throw new SemanticException("Missing required argument: " + arg.getName());
            }
        });

        // check arguments values
        Map<String, ConstantOperator> constantArgs = new HashMap<>();
        for (Map.Entry<String, ProcedureArgument> entry : tableOperationNamedArgs.entrySet()) {
            Expr tableOperationArgumentValue = entry.getValue().getValue();
            NamedArgument procedureArgument = tableProcedureArgumentMap.get(entry.getKey());
            // check call argument is constant
            ScalarOperator result;
            try {
                Scope scope = new Scope(RelationId.anonymous(), new RelationFields());
                ExpressionAnalyzer.analyzeExpression(tableOperationArgumentValue, new AnalyzeState(), scope, context);
                ExpressionMapping expressionMapping = new ExpressionMapping(scope);
                result = SqlToScalarOperatorTranslator.translate(tableOperationArgumentValue, expressionMapping,
                        new ColumnRefFactory());
                if (result instanceof ConstantOperator resConstantOperator) {
                    // check if constant argument type is compatible with procedure argument type
                    if (resConstantOperator.castTo(procedureArgument.getType()).isEmpty()) {
                        throw new SemanticException("Argument '%s' has invalid type %s, expected %s",
                                entry.getKey(), result.getType(), procedureArgument.getType());
                    }
                    constantArgs.put(entry.getKey(), resConstantOperator);
                } else {
                    throw new SemanticException("Argument '%s' must be a constant, got %s",
                            entry.getKey(), result);
                }
            } catch (Exception e) {
                throw new SemanticException("Failed to resolve table procedure args: %s. msg: %s, " +
                        "expected const argument", tableOperationArgumentValue, e.getMessage());
            }
        }
        clause.setAnalyzedArgs(constantArgs);

        if (clause.getWhere() != null && icebergTableProcedure.getOperation() == IcebergTableOperation.REWRITE_DATA_FILES) {
            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            List<ColumnRefOperator> columnRefOperators = table.getBaseSchema()
                    .stream()
                    .map(col -> columnRefFactory.create(col.getName(), col.getType(), col.isAllowNull()))
                    .collect(Collectors.toList());
            Scope scope = new Scope(RelationId.anonymous(), new RelationFields(columnRefOperators.stream()
                    .map(col -> new Field(col.getName(), col.getType(),
                            new TableName(context.getDatabase(), icebergTable.getName()), null))
                    .collect(Collectors.toList())));
            ExpressionAnalyzer.analyzeExpression(clause.getWhere(), new AnalyzeState(), scope, context);
            ExpressionMapping expressionMapping = new ExpressionMapping(scope, columnRefOperators);
            ScalarOperator res = SqlToScalarOperatorTranslator.translate(
                    clause.getWhere(), expressionMapping, columnRefFactory);
            clause.setPartitionFilter(res);
            List<Column> partitionCols = icebergTable.getPartitionColumnsIncludeTransformed();
            if (res.getColumnRefs().stream().anyMatch(col ->
                    partitionCols.stream().noneMatch(partCol -> partCol.getName().equals(col.getName())))) {
                throw new SemanticException("Partition filter contains columns that are not partition columns");
            }
        }
        return null;
    }

    @Override
    public Void visitAlterTableAutoIncrementClause(AlterTableAutoIncrementClause clause, ConnectContext context) {
        if (!table.isNativeTable()) {
            throw new SemanticException("Only native table supports AUTO_INCREMENT clause");
        }

        long newValue = clause.getAutoIncrementValue();
        if (newValue <= 0) {
            throw new SemanticException("AUTO_INCREMENT value must be positive");
        }

        return null;
    }
}
