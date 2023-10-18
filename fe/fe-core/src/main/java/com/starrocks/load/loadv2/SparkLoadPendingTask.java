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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/SparkLoadPendingTask.java

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

package com.starrocks.load.loadv2;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SparkResource;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.LoadException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.LoadPriority;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import com.starrocks.load.FailMsg;
import com.starrocks.load.Load;
import com.starrocks.load.loadv2.etl.EtlJobConfig;
import com.starrocks.load.loadv2.etl.EtlJobConfig.EtlColumn;
import com.starrocks.load.loadv2.etl.EtlJobConfig.EtlColumnMapping;
import com.starrocks.load.loadv2.etl.EtlJobConfig.EtlFileGroup;
import com.starrocks.load.loadv2.etl.EtlJobConfig.EtlIndex;
import com.starrocks.load.loadv2.etl.EtlJobConfig.EtlJobProperty;
import com.starrocks.load.loadv2.etl.EtlJobConfig.EtlPartition;
import com.starrocks.load.loadv2.etl.EtlJobConfig.EtlPartitionInfo;
import com.starrocks.load.loadv2.etl.EtlJobConfig.EtlTable;
import com.starrocks.load.loadv2.etl.EtlJobConfig.FilePatternVersion;
import com.starrocks.load.loadv2.etl.EtlJobConfig.SourceType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

// 1. create etl job config and write it into jobconfig.json file
// 2. submit spark etl job
public class SparkLoadPendingTask extends LoadTask {
    private static final Logger LOG = LogManager.getLogger(SparkLoadPendingTask.class);

    private final Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToBrokerFileGroups;
    private final SparkResource resource;
    private final BrokerDesc brokerDesc;
    private final long dbId;
    private final String loadLabel;
    private final long loadJobId;
    private final long transactionId;
    private EtlJobConfig etlJobConfig;
    private SparkLoadAppHandle sparkLoadAppHandle;

    public SparkLoadPendingTask(SparkLoadJob loadTaskCallback,
                                Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToBrokerFileGroups,
                                SparkResource resource, BrokerDesc brokerDesc) {
        super(loadTaskCallback, TaskType.PENDING, LoadPriority.NORMAL_VALUE);
        this.retryTime = 3;
        this.attachment = new SparkPendingTaskAttachment(signature);
        this.aggKeyToBrokerFileGroups = aggKeyToBrokerFileGroups;
        this.resource = resource;
        this.brokerDesc = brokerDesc;
        this.dbId = loadTaskCallback.getDbId();
        this.loadJobId = loadTaskCallback.getId();
        this.loadLabel = loadTaskCallback.getLabel();
        this.transactionId = loadTaskCallback.getTransactionId();
        this.sparkLoadAppHandle = loadTaskCallback.getHandle();
        this.failMsg = new FailMsg(FailMsg.CancelType.ETL_SUBMIT_FAIL);
    }

    @Override
    void executeTask() throws LoadException {
        LOG.info("begin to execute spark pending task. load job id: {}", loadJobId);
        submitEtlJob();
    }

    private void submitEtlJob() throws LoadException {
        SparkPendingTaskAttachment sparkAttachment = (SparkPendingTaskAttachment) attachment;
        // retry different output path
        etlJobConfig.outputPath = EtlJobConfig.getOutputPath(resource.getWorkingDir(), dbId, loadLabel, signature);
        sparkAttachment.setOutputPath(etlJobConfig.outputPath);

        // handler submit etl job
        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        Long sparkLoadSubmitTimeout = ((SparkLoadJob) callback).sparkLoadSubmitTimeoutSecond;
        handler.submitEtlJob(loadJobId, loadLabel, etlJobConfig, resource, brokerDesc, sparkLoadAppHandle,
                sparkAttachment, sparkLoadSubmitTimeout);
        LOG.info("submit spark etl job success. load job id: {}, attachment: {}", loadJobId, sparkAttachment);
    }

    @Override
    public void init() throws LoadException {
        createEtlJobConf();
    }

    private void createEtlJobConf() throws LoadException {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new LoadException("db does not exist. id: " + dbId);
        }

        Map<Long, EtlTable> tables = Maps.newHashMap();
        db.readLock();
        try {
            Map<Long, Set<Long>> tableIdToPartitionIds = Maps.newHashMap();
            Set<Long> allPartitionsTableIds = Sets.newHashSet();
            prepareTablePartitionInfos(db, tableIdToPartitionIds, allPartitionsTableIds);

            for (Map.Entry<FileGroupAggKey, List<BrokerFileGroup>> entry : aggKeyToBrokerFileGroups.entrySet()) {
                FileGroupAggKey aggKey = entry.getKey();
                long tableId = aggKey.getTableId();

                OlapTable table = (OlapTable) db.getTable(tableId);
                if (table == null) {
                    throw new LoadException("table does not exist. id: " + tableId);
                }

                EtlTable etlTable = null;
                if (tables.containsKey(tableId)) {
                    etlTable = tables.get(tableId);
                } else {
                    // indexes
                    List<EtlIndex> etlIndexes = createEtlIndexes(table);
                    // partition info
                    EtlPartitionInfo etlPartitionInfo = createEtlPartitionInfo(table,
                            tableIdToPartitionIds.get(tableId));
                    etlTable = new EtlTable(etlIndexes, etlPartitionInfo);
                    tables.put(tableId, etlTable);

                    // add table indexes to transaction state
                    TransactionState txnState = GlobalStateMgr.getCurrentGlobalTransactionMgr()
                            .getTransactionState(dbId, transactionId);
                    if (txnState == null) {
                        throw new LoadException("txn does not exist. id: " + transactionId);
                    }
                    txnState.addTableIndexes(table);
                }

                // file group
                for (BrokerFileGroup fileGroup : entry.getValue()) {
                    etlTable.addFileGroup(createEtlFileGroup(fileGroup, tableIdToPartitionIds.get(tableId), db, table));
                }
            }
        } finally {
            db.readUnlock();
        }

        String outputFilePattern = EtlJobConfig.getOutputFilePattern(loadLabel, FilePatternVersion.V1);
        // strictMode timezone properties
        EtlJobProperty properties = new EtlJobProperty();
        properties.strictMode = ((LoadJob) callback).strictMode;
        properties.timezone = ((LoadJob) callback).timezone;
        etlJobConfig = new EtlJobConfig(tables, outputFilePattern, loadLabel, properties);
    }

    private void prepareTablePartitionInfos(Database db, Map<Long, Set<Long>> tableIdToPartitionIds,
                                            Set<Long> allPartitionsTableIds) throws LoadException {
        for (FileGroupAggKey aggKey : aggKeyToBrokerFileGroups.keySet()) {
            long tableId = aggKey.getTableId();
            if (allPartitionsTableIds.contains(tableId)) {
                continue;
            }

            OlapTable table = (OlapTable) db.getTable(tableId);
            if (table == null) {
                throw new LoadException("table does not exist. id: " + tableId);
            }

            Set<Long> partitionIds = null;
            if (tableIdToPartitionIds.containsKey(tableId)) {
                partitionIds = tableIdToPartitionIds.get(tableId);
            } else {
                partitionIds = Sets.newHashSet();
                tableIdToPartitionIds.put(tableId, partitionIds);
            }

            Set<Long> groupPartitionIds = aggKey.getPartitionIds();
            // if not assign partition, use all partitions
            if (groupPartitionIds == null || groupPartitionIds.isEmpty()) {
                for (Partition partition : table.getPartitions()) {
                    partitionIds.add(partition.getId());
                }

                allPartitionsTableIds.add(tableId);
            } else {
                partitionIds.addAll(groupPartitionIds);
            }
        }
    }

    private List<EtlIndex> createEtlIndexes(OlapTable table) throws LoadException {
        List<EtlIndex> etlIndexes = Lists.newArrayList();

        for (Map.Entry<Long, List<Column>> entry : table.getIndexIdToSchema().entrySet()) {
            long indexId = entry.getKey();
            int schemaHash = table.getSchemaHashByIndexId(indexId);

            // columns
            List<EtlColumn> etlColumns = Lists.newArrayList();
            for (Column column : entry.getValue()) {
                etlColumns.add(createEtlColumn(column));
            }

            // check distribution type
            DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
            if (distributionInfo.getType() != DistributionInfoType.HASH) {
                // RANDOM not supported
                String errMsg = "Unsupported distribution type. type: " + distributionInfo.getType().name();
                LOG.warn(errMsg);
                throw new LoadException(errMsg);
            }

            // index type
            String indexType = null;
            KeysType keysType = table.getKeysTypeByIndexId(indexId);
            switch (keysType) {
                case DUP_KEYS:
                    indexType = "DUPLICATE";
                    break;
                case AGG_KEYS:
                    indexType = "AGGREGATE";
                    break;
                case UNIQUE_KEYS:
                    indexType = "UNIQUE";
                    break;
                default:
                    String errMsg = "unknown keys type. type: " + keysType.name();
                    LOG.warn(errMsg);
                    throw new LoadException(errMsg);
            }

            // is base index
            boolean isBaseIndex = indexId == table.getBaseIndexId() ? true : false;

            etlIndexes.add(new EtlIndex(indexId, etlColumns, schemaHash, indexType, isBaseIndex));
        }

        return etlIndexes;
    }

    private EtlColumn createEtlColumn(Column column) {
        // column name
        String name = column.getName();
        // column type
        PrimitiveType type = column.getPrimitiveType();
        String columnType = column.getPrimitiveType().toString();
        // is allow null
        boolean isAllowNull = column.isAllowNull();
        // is key
        boolean isKey = column.isKey();

        // aggregation type
        String aggregationType = null;
        if (column.getAggregationType() != null) {
            aggregationType = column.getAggregationType().toString();
        }

        // default value
        String defaultValue = null;
        Column.DefaultValueType defaultValueType = column.getDefaultValueType();
        if (defaultValueType == Column.DefaultValueType.VARY) {
            throw new SemanticException("Column " + column.getName() + " has unsupported default value:" +
                    column.getDefaultExpr().getExpr());
        }
        if (defaultValueType == Column.DefaultValueType.CONST) {
            defaultValue = column.calculatedDefaultValue();
        }
        if (column.isAllowNull() && defaultValueType == Column.DefaultValueType.NULL) {
            defaultValue = "\\N";
        }

        // string length
        int stringLength = 0;
        if (type.isStringType()) {
            stringLength = column.getStrLen();
        }

        // decimal precision scale
        int precision = 0;
        int scale = 0;
        if (type.isDecimalOfAnyVersion()) {
            precision = column.getPrecision();
            scale = column.getScale();
        }

        return new EtlColumn(name, columnType, isAllowNull, isKey, aggregationType, defaultValue,
                stringLength, precision, scale);
    }

    private EtlPartitionInfo createEtlPartitionInfo(OlapTable table, Set<Long> partitionIds) throws LoadException {
        PartitionType type = table.getPartitionInfo().getType();

        List<String> partitionColumnRefs = Lists.newArrayList();
        List<EtlPartition> etlPartitions = Lists.newArrayList();
        switch (type) {
            case RANGE:
                etlPartitions = initEtlRangePartition(partitionColumnRefs, table, partitionIds);
                break;
            case LIST:
                etlPartitions = initEtlListPartition(partitionColumnRefs, table, partitionIds);
                break;
            case UNPARTITIONED:
                etlPartitions = initEtlUnPartitioned(table, partitionIds);
                break;
        }

        // distribution column refs
        List<String> distributionColumnRefs = Lists.newArrayList();
        DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
        Preconditions.checkState(distributionInfo.getType() == DistributionInfoType.HASH);
        for (Column column : ((HashDistributionInfo) distributionInfo).getDistributionColumns()) {
            distributionColumnRefs.add(column.getName());
        }

        return new EtlPartitionInfo(type.typeString, partitionColumnRefs, distributionColumnRefs, etlPartitions);
    }

    private List<EtlPartition> initEtlListPartition(
            List<String> partitionColumnRefs, OlapTable table, Set<Long> partitionIds) throws LoadException {
        ListPartitionInfo listPartitionInfo = (ListPartitionInfo) table.getPartitionInfo();
        for (Column column : listPartitionInfo.getPartitionColumns()) {
            partitionColumnRefs.add(column.getName());
        }
        List<EtlPartition> etlPartitions = Lists.newArrayList();
        Map<Long, List<List<LiteralExpr>>> multiLiteralExprValues = listPartitionInfo.getMultiLiteralExprValues();
        Map<Long, List<LiteralExpr>>  literalExprValues = listPartitionInfo.getLiteralExprValues();
        for (Long partitionId : partitionIds) {
            Partition partition = table.getPartition(partitionId);
            if (partition == null) {
                throw new LoadException("partition does not exist. id: " + partitionId);
            }
            // bucket num
            int bucketNum = partition.getDistributionInfo().getBucketNum();
            // list partition values
            List<List<LiteralExpr>> multiValueList = multiLiteralExprValues.get(partitionId);
            List<List<Object>> inKeys = Lists.newArrayList();
            if (multiValueList != null && !multiValueList.isEmpty()) {
                for (List<LiteralExpr> list : multiValueList) {
                    inKeys.add(initItemOfInKeys(list));
                }
            }
            List<LiteralExpr> valueList = literalExprValues.get(partitionId);
            if (valueList != null && !valueList.isEmpty()) {
                for (LiteralExpr literalExpr : valueList) {
                    inKeys.add(initItemOfInKeys(Lists.newArrayList(literalExpr)));
                }
            }
            etlPartitions.add(new EtlPartition(partitionId, inKeys, bucketNum));
        }
        return etlPartitions;
    }

    private List<Object> initItemOfInKeys(List<LiteralExpr> list) {
        List<Object> curList = new ArrayList<>();
        for (LiteralExpr literalExpr : list) {
            Object keyValue;
            if (literalExpr instanceof DateLiteral) {
                keyValue = convertDateLiteralToNumber((DateLiteral) literalExpr);
            } else {
                keyValue = literalExpr.getRealObjectValue();
            }
            curList.add(keyValue);
        }
        return curList;
    }

    private List<EtlPartition> initEtlRangePartition(
            List<String> partitionColumnRefs, OlapTable table, Set<Long> partitionIds) throws LoadException {
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        List<EtlPartition> etlPartitions = Lists.newArrayList();
        for (Column column : rangePartitionInfo.getPartitionColumns()) {
            partitionColumnRefs.add(column.getName());
        }

        List<Map.Entry<Long, Range<PartitionKey>>> sortedRanges = null;
        try {
            sortedRanges = rangePartitionInfo.getSortedRangeMap(partitionIds);
        } catch (AnalysisException e) {
            throw new LoadException(e.getMessage());
        }
        for (Map.Entry<Long, Range<PartitionKey>> entry : sortedRanges) {
            long partitionId = entry.getKey();
            Partition partition = table.getPartition(partitionId);
            if (partition == null) {
                throw new LoadException("partition does not exist. id: " + partitionId);
            }

            // bucket num
            int bucketNum = partition.getDistributionInfo().getBucketNum();

            // is min|max partition
            Range<PartitionKey> range = entry.getValue();
            boolean isMaxPartition = range.upperEndpoint().isMaxValue();
            boolean isMinPartition = range.lowerEndpoint().isMinValue();

            // start keys
            List<LiteralExpr> rangeKeyExprs = null;
            List<Object> startKeys = Lists.newArrayList();
            if (!isMinPartition) {
                rangeKeyExprs = range.lowerEndpoint().getKeys();
                for (int i = 0; i < rangeKeyExprs.size(); ++i) {
                    LiteralExpr literalExpr = rangeKeyExprs.get(i);

                    Object keyValue;
                    if (literalExpr instanceof DateLiteral) {
                        keyValue = convertDateLiteralToNumber((DateLiteral) literalExpr);
                    } else {
                        keyValue = literalExpr.getRealObjectValue();
                    }

                    startKeys.add(keyValue);
                }
            }

            // end keys
            // is empty list when max partition
            List<Object> endKeys = Lists.newArrayList();
            if (!isMaxPartition) {
                rangeKeyExprs = range.upperEndpoint().getKeys();
                for (int i = 0; i < rangeKeyExprs.size(); ++i) {
                    LiteralExpr literalExpr = rangeKeyExprs.get(i);

                    Object keyValue;
                    if (literalExpr instanceof DateLiteral) {
                        keyValue = convertDateLiteralToNumber((DateLiteral) literalExpr);
                    } else {
                        keyValue = literalExpr.getRealObjectValue();
                    }
                    endKeys.add(keyValue);
                }
            }

            etlPartitions.add(new EtlPartition(partitionId, startKeys, endKeys, isMinPartition, isMaxPartition, bucketNum));
        }
        return etlPartitions;
    }

    private List<EtlPartition> initEtlUnPartitioned(OlapTable table, Set<Long> partitionIds) throws LoadException {
        PartitionType type = table.getPartitionInfo().getType();
        List<EtlPartition> etlPartitions = Lists.newArrayList();
        Preconditions.checkState(type == PartitionType.UNPARTITIONED);
        Preconditions.checkState(partitionIds.size() == 1);

        for (Long partitionId : partitionIds) {
            Partition partition = table.getPartition(partitionId);
            if (partition == null) {
                throw new LoadException("partition does not exist. id: " + partitionId);
            }

            // bucket num
            int bucketNum = partition.getDistributionInfo().getBucketNum();

            etlPartitions.add(new EtlPartition(partitionId, Lists.newArrayList(), Lists.newArrayList(),
                    true, true, bucketNum));
        }
        return etlPartitions;
    }

    private EtlFileGroup createEtlFileGroup(BrokerFileGroup fileGroup, Set<Long> tablePartitionIds,
                                            Database db, OlapTable table) throws LoadException {
        List<ImportColumnDesc> copiedColumnExprList = Lists.newArrayList(fileGroup.getColumnExprList());
        Map<String, Expr> exprByName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (ImportColumnDesc columnDesc : copiedColumnExprList) {
            if (!columnDesc.isColumn()) {
                exprByName.put(columnDesc.getColumnName(), columnDesc.getExpr());
            }
        }

        // check columns
        try {
            Load.initColumns(table, copiedColumnExprList, fileGroup.getColumnToHadoopFunction());
        } catch (UserException e) {
            throw new LoadException(e.getMessage());
        }
        // add generated column mapping
        for (ImportColumnDesc generatedColumnDesc : Load.getMaterializedShadowColumnDesc(table, db.getFullName(), false)) {
            Expr expr = generatedColumnDesc.getExpr();
            List<SlotRef> slots = Lists.newArrayList();
            expr.collect(SlotRef.class, slots);
            for (SlotRef slot : slots) {
                for (ImportColumnDesc columnDesc : copiedColumnExprList) {
                    if (!columnDesc.isColumn() && slot.getColumnName().equals(columnDesc.getColumnName())) {
                        throw new LoadException("generated column can not refenece the column which is the " +
                                                "result of the expression in spark load");
                    }
                }
            }
            copiedColumnExprList.add(generatedColumnDesc);
            exprByName.put(generatedColumnDesc.getColumnName(), generatedColumnDesc.getExpr());
        }
        // add shadow column mapping when schema change
        for (ImportColumnDesc columnDesc : Load.getSchemaChangeShadowColumnDesc(table, exprByName)) {
            copiedColumnExprList.add(columnDesc);
            exprByName.put(columnDesc.getColumnName(), columnDesc.getExpr());
        }

        // check negative for sum aggregate type
        if (fileGroup.isNegative()) {
            for (Column column : table.getBaseSchema()) {
                if (!column.isKey() && column.getAggregationType() != AggregateType.SUM) {
                    throw new LoadException("Column is not SUM AggreateType. column:" + column.getName());
                }
            }
        }

        // fill file field names if empty
        List<String> fileFieldNames = fileGroup.getFileFieldNames();
        if (fileFieldNames == null || fileFieldNames.isEmpty()) {
            fileFieldNames = Lists.newArrayList();
            for (Column column : table.getBaseSchema()) {
                fileFieldNames.add(column.getName());
            }
        }

        // column mappings
        Map<String, Pair<String, List<String>>> columnToHadoopFunction = fileGroup.getColumnToHadoopFunction();
        Map<String, EtlColumnMapping> columnMappings = Maps.newHashMap();
        if (columnToHadoopFunction != null) {
            for (Map.Entry<String, Pair<String, List<String>>> entry : columnToHadoopFunction.entrySet()) {
                columnMappings.put(entry.getKey(),
                        new EtlColumnMapping(entry.getValue().first, entry.getValue().second));
            }
        }
        for (ImportColumnDesc columnDesc : copiedColumnExprList) {
            if (columnDesc.isColumn() || columnMappings.containsKey(columnDesc.getColumnName())) {
                continue;
            }
            // the left must be column expr
            columnMappings.put(columnDesc.getColumnName(), new EtlColumnMapping(columnDesc.getExpr().toSql()));
        }

        // partition ids
        List<Long> partitionIds = fileGroup.getPartitionIds();
        if (partitionIds == null || partitionIds.isEmpty()) {
            partitionIds = Lists.newArrayList(tablePartitionIds);
        }

        // where
        // TODO: check
        String where = "";
        if (fileGroup.getWhereExpr() != null) {
            where = fileGroup.getWhereExpr().toSql();
        }

        // load from table
        String hiveDbTableName = "";
        Map<String, String> hiveTableProperties = Maps.newHashMap();
        if (fileGroup.isLoadFromTable()) {
            long srcTableId = fileGroup.getSrcTableId();
            HiveTable srcHiveTable = (HiveTable) db.getTable(srcTableId);
            if (srcHiveTable == null) {
                throw new LoadException("table does not exist. id: " + srcTableId);
            }
            hiveDbTableName = srcHiveTable.getHiveDbTable();
            hiveTableProperties.putAll(srcHiveTable.getProperties());
        }

        // check hll and bitmap func
        // TODO: more check
        for (Column column : table.getBaseSchema()) {
            String columnName = column.getName();
            PrimitiveType columnType = column.getPrimitiveType();
            Expr expr = exprByName.get(columnName);
            if (columnType == PrimitiveType.HLL) {
                checkHllMapping(columnName, expr);
            }
            if (columnType == PrimitiveType.BITMAP) {
                checkBitmapMapping(columnName, expr, fileGroup.isLoadFromTable());
            }
        }

        EtlFileGroup etlFileGroup = null;
        if (fileGroup.isLoadFromTable()) {
            etlFileGroup = new EtlFileGroup(SourceType.HIVE, fileFieldNames, hiveDbTableName, hiveTableProperties,
                    fileGroup.isNegative(), columnMappings, where, partitionIds);
        } else {
            etlFileGroup = new EtlFileGroup(SourceType.FILE, fileGroup.getFilePaths(), fileFieldNames,
                    fileGroup.getColumnsFromPath(), fileGroup.getColumnSeparator(),
                    fileGroup.getRowDelimiter(), fileGroup.isNegative(),
                    fileGroup.getFileFormat(), columnMappings,
                    where, partitionIds);
        }

        return etlFileGroup;
    }

    private void checkHllMapping(String columnName, Expr expr) throws LoadException {
        if (expr == null) {
            throw new LoadException("HLL column func is not assigned. column:" + columnName);
        }

        String msg = "HLL column must use hll function, like " + columnName + "=hll_hash(xxx) or "
                + columnName + "=hll_empty()";
        if (!(expr instanceof FunctionCallExpr)) {
            throw new LoadException(msg);
        }
        FunctionCallExpr fn = (FunctionCallExpr) expr;
        String functionName = fn.getFnName().getFunction();
        if (!functionName.equalsIgnoreCase(FunctionSet.HLL_HASH)
                && !functionName.equalsIgnoreCase("hll_empty")) {
            throw new LoadException(msg);
        }
    }

    private void checkBitmapMapping(String columnName, Expr expr, boolean isLoadFromTable) throws LoadException {
        if (expr == null) {
            throw new LoadException("BITMAP column func is not assigned. column:" + columnName);
        }

        String msg = "BITMAP column must use bitmap function, like " + columnName + "=to_bitmap(xxx) or "
                + columnName + "=bitmap_hash() or " + columnName + "=bitmap_dict()";
        if (!(expr instanceof FunctionCallExpr)) {
            throw new LoadException(msg);
        }
        FunctionCallExpr fn = (FunctionCallExpr) expr;
        String functionName = fn.getFnName().getFunction();
        if (!functionName.equalsIgnoreCase(FunctionSet.TO_BITMAP)
                && !functionName.equalsIgnoreCase(FunctionSet.BITMAP_HASH)
                && !functionName.equalsIgnoreCase(FunctionSet.BITMAP_DICT)) {
            throw new LoadException(msg);
        }

        if (functionName.equalsIgnoreCase("bitmap_dict") && !isLoadFromTable) {
            throw new LoadException("Bitmap global dict should load data from hive table");
        }
    }

    // This is to be compatible with Spark Load Job formats for Date type.
    // Because the historical version is serialized and deserialized with a special hash number for DateLiteral,
    // special processing is also done here for DateLiteral to keep the historical version compatible.
    // The deserialized code is in "SparkDpp.createPartitionRangeKeys"
    public static Object convertDateLiteralToNumber(DateLiteral dateLiteral) {
        if (dateLiteral.getType().isDate()) {
            return (dateLiteral.getYear() * 16 * 32L
                    + dateLiteral.getMonth() * 32
                    + dateLiteral.getDay());
        } else if (dateLiteral.getType().isDatetime()) {
            return dateLiteral.getLongValue();
        } else {
            throw new StarRocksPlannerException("Invalid date type: " + dateLiteral.getType(), ErrorType.INTERNAL_ERROR);
        }
    }
}
