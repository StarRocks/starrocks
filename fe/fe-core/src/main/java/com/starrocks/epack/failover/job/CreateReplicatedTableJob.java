// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.persist.ColumnIdExpr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.PartitionDescAnalyzer;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.ast.KeysDesc;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.parser.NodePosition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CreateReplicatedTableJob extends FailoverGroupJob {
    private static final Logger LOG = LogManager.getLogger(CreateReplicatedTableJob.class);

    private final Database remoteDatabase;
    private final OlapTable remoteTable;
    private final Database localDatabase;
    private final boolean isIncludeObject;

    public CreateReplicatedTableJob(FailoverGroup failoverGroup, Database remoteDatabase,
            OlapTable remoteTable, Database localDatabase, boolean isIncludeObject) {
        super(failoverGroup);
        this.remoteDatabase = remoteDatabase;
        this.remoteTable = remoteTable;
        this.localDatabase = localDatabase;
        this.isIncludeObject = isIncludeObject;
    }

    @Override
    public void execute() {
        LOG.info("Creating table {}.{} in failover group {}", localDatabase.getFullName(), remoteTable.getName(),
                failoverGroup.getName());

        CreateTableStmt createTableStmt = getCreateTableStmt(localDatabase, remoteTable);
        try {
            GlobalStateMgr.getServingState().getLocalMetastore().createTable(createTableStmt);
        } catch (Exception e) {
            failoverGroup.addErrorMessage("Failed to create table " + localDatabase.getFullName() + "." +
                    remoteTable.getName() + ", error: " + e.getMessage());
            LOG.warn("Failed to create table {}.{} in failover group {}, ", localDatabase.getFullName(),
                    remoteTable.getName(), failoverGroup.getName(), e);
            return;
        }

        CheckReplicatedTableJob job = new CheckReplicatedTableJob(failoverGroup,
                remoteDatabase, remoteTable, localDatabase, isIncludeObject);
        job.execute();
    }

    private static CreateTableStmt getCreateTableStmt(Database database, OlapTable table) {
        QualifiedName qualifiedName = QualifiedName.of(Lists.newArrayList(
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, database.getFullName(), table.getName()));
        TableRef tableRef = new TableRef(qualifiedName, null, NodePosition.ZERO);
        List<Column> columns = table.getBaseSchema();
        List<String> keysColumnNames = columns.stream().filter(Column::isKey).map(Column::getName)
                .collect(Collectors.toList());
        List<ColumnDef> columnDefs = columns.stream().map(column -> column.toColumnDef(table))
                .collect(Collectors.toList());
        List<Index> indexes = table.getIndexes();
        List<IndexDef> indexDefs = indexes.stream().map(index -> indexToIndexDef(table.getIdToColumn(), index))
                .collect(Collectors.toList());
        String engine = table.getType() == Table.TableType.CLOUD_NATIVE ? "OLAP" : table.getType().name();
        KeysDesc keysDesc = new KeysDesc(table.getKeysType(), keysColumnNames);
        PartitionDesc partitionDesc = getPartitionDesc(table);
        DistributionDesc distributionDesc = getDistributionDesc(table);
        Map<String, String> properties = getProperties(table);
        List<OrderByElement> orderByElements = getSortKeysColumnNames(table);

        CreateTableStmt createTableStmt = new CreateTableStmt(true, false, tableRef, columnDefs, indexDefs,
                engine, "utf8", keysDesc, partitionDesc, distributionDesc, properties, null,
                table.getComment(), null, orderByElements, NodePosition.ZERO);

        createTableStmt.setColumns(columns);
        createTableStmt.setIndexes(indexes);

        return createTableStmt;
    }

    private static IndexDef indexToIndexDef(Map<ColumnId, Column> idColumnMap, Index index) {
        return new IndexDef(index.getIndexName(), MetaUtils.getColumnNamesByColumnIds(idColumnMap, index.getColumns()),
                index.getIndexType(), index.getComment());
    }

    private static PartitionDesc getPartitionDesc(OlapTable table) {
        PartitionInfo partitionInfo = table.getPartitionInfo();
        switch (partitionInfo.getType()) {
            case UNPARTITIONED:
                return null;
            case RANGE:
                return getRangePartitionDesc(table);
            case LIST:
                return getListPartitionDesc(table);
            case EXPR_RANGE:
                return getExprRangePartitionDesc(table);
            case EXPR_RANGE_V2:
                return getExprRangeV2PartitionDesc(table);
            default:
                LOG.warn("Invalid partition type {} of table {}", partitionInfo.getType(), table.getName());
                return null;
        }
    }

    private static RangePartitionDesc getRangePartitionDesc(OlapTable table) {
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();

        List<String> partitionColumnNames = rangePartitionInfo.getPartitionColumns(table.getIdToColumn()).stream()
                .map(Column::getName).collect(Collectors.toList());

        List<Map.Entry<Long, Range<PartitionKey>>> partitionEntries = rangePartitionInfo.getSortedRangeMap(false);
        List<PartitionDesc> partitionDescs = Lists.newArrayListWithCapacity(partitionEntries.size());
        for (Map.Entry<Long, Range<PartitionKey>> partitionEntry : partitionEntries) {
            Partition partition = table.getPartition(partitionEntry.getKey());
            long partitionId = partition.getId();
            String partitionName = partition.getName();
            if (partitionName.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                continue;
            }
            Range<PartitionKey> partitionRange = partitionEntry.getValue();
            List<PartitionValue> lowerValues = partitionRange.lowerEndpoint().getKeys().stream()
                    .map(v -> new PartitionValue(v.getStringValue())).collect(Collectors.toList());
            List<PartitionValue> upperValues = partitionRange.upperEndpoint().getKeys().stream()
                    .map(v -> new PartitionValue(v.getStringValue())).collect(Collectors.toList());
            PartitionKeyDesc partitionKeyDesc = new PartitionKeyDesc(lowerValues, upperValues);
            SingleRangePartitionDesc singleRangePartitionDesc = new SingleRangePartitionDesc(
                    true, partitionName,
                    rangePartitionInfo.getReplicationNum(partitionId),
                    rangePartitionInfo.getDataProperty(partitionId),
                    Partition.PARTITION_INIT_VERSION,
                    rangePartitionInfo.getDataCacheInfo(partitionId),
                    partitionKeyDesc);
            singleRangePartitionDesc.setSystem(true);
            partitionDescs.add(singleRangePartitionDesc);
        }

        RangePartitionDesc rangePartitionDesc = new RangePartitionDesc(partitionColumnNames, partitionDescs);
        rangePartitionDesc.setAutoPartitionTable(rangePartitionInfo.isAutomaticPartition());
        rangePartitionDesc.setSystem(true);
        return rangePartitionDesc;
    }

    private static ListPartitionDesc getListPartitionDesc(OlapTable table) {
        ListPartitionInfo listPartitionInfo = (ListPartitionInfo) table.getPartitionInfo();

        List<String> partitionColumnNames = listPartitionInfo.getPartitionColumns(table.getIdToColumn()).stream()
                .map(Column::getName).collect(Collectors.toList());
        List<ColumnDef> partitionColumnDefs = listPartitionInfo.getPartitionColumns(table.getIdToColumn()).stream()
                .map(column -> column.toColumnDef(table)).collect(Collectors.toList());

        Map<Long, List<String>> idToValues = listPartitionInfo.getIdToValues();
        Map<Long, List<List<String>>> idToMultiValues = listPartitionInfo.getIdToMultiValues();

        List<PartitionDesc> partitionDescs = Lists.newArrayListWithCapacity(idToValues.size() + idToMultiValues.size());
        for (Map.Entry<Long, List<String>> partitionEntry : idToValues.entrySet()) {
            Partition partition = table.getPartition(partitionEntry.getKey());
            long partitionId = partition.getId();
            String partitionName = partition.getName();
            if (partitionName.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                continue;
            }
            SingleItemListPartitionDesc singleItemListPartitionDesc = new SingleItemListPartitionDesc(
                    true, partitionName,
                    listPartitionInfo.getReplicationNum(partitionId),
                    listPartitionInfo.getDataProperty(partitionId),
                    Partition.PARTITION_INIT_VERSION,
                    listPartitionInfo.getDataCacheInfo(partitionId),
                    partitionEntry.getValue(),
                    partitionColumnDefs);
            singleItemListPartitionDesc.setSystem(true);
            partitionDescs.add(singleItemListPartitionDesc);
        }

        for (Map.Entry<Long, List<List<String>>> partitionEntry : idToMultiValues.entrySet()) {
            Partition partition = table.getPartition(partitionEntry.getKey());
            long partitionId = partition.getId();
            String partitionName = partition.getName();
            if (partitionName.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                continue;
            }
            MultiItemListPartitionDesc multiItemListPartitionDesc = new MultiItemListPartitionDesc(
                    true, partitionName,
                    listPartitionInfo.getReplicationNum(partitionId),
                    listPartitionInfo.getDataProperty(partitionId),
                    Partition.PARTITION_INIT_VERSION,
                    listPartitionInfo.getDataCacheInfo(partitionId),
                    partitionEntry.getValue(),
                    partitionColumnDefs);
            multiItemListPartitionDesc.setSystem(true);
            partitionDescs.add(multiItemListPartitionDesc);
        }

        ListPartitionDesc listPartitionDesc = new ListPartitionDesc(partitionColumnNames, partitionDescs);
        listPartitionDesc.setAutoPartitionTable(listPartitionInfo.isAutomaticPartition());
        listPartitionDesc.setSystem(true);
        return listPartitionDesc;
    }

    private static PartitionDesc getExprRangePartitionDesc(OlapTable table) {
        ExpressionRangePartitionInfo exprRangePartitionInfo = (ExpressionRangePartitionInfo) table.getPartitionInfo();

        ExpressionPartitionDesc expressionPartitionDesc = new ExpressionPartitionDesc(getRangePartitionDesc(table),
                exprRangePartitionInfo.getPartitionExprs(table.getIdToColumn()).get(0));

        List<ColumnDef> columnDefs = table.getBaseSchema().stream().map(column -> column.toColumnDef(table))
                .collect(Collectors.toList());
        try {
            PartitionDescAnalyzer.analyzeExpressionPartitionDesc(expressionPartitionDesc, columnDefs, Collections.emptyMap());
        } catch (Exception e) {
            throw new RuntimeException("Failed to analyze expression partition desc, ", e);
        }

        expressionPartitionDesc.setSystem(true);
        return expressionPartitionDesc;
    }

    private static PartitionDesc getExprRangeV2PartitionDesc(OlapTable table) {
        ExpressionRangePartitionInfoV2 exprRangeV2PartitionInfo = (ExpressionRangePartitionInfoV2) table
                .getPartitionInfo();

        ExpressionPartitionDesc expressionPartitionDesc = new ExpressionPartitionDesc(getRangePartitionDesc(table),
                exprRangeV2PartitionInfo.getPartitionExprs(table.getIdToColumn()).get(0));

        List<ColumnDef> columnDefs = table.getBaseSchema().stream().map(column -> column.toColumnDef(table))
                .collect(Collectors.toList());
        try {
            PartitionDescAnalyzer.analyzeExpressionPartitionDesc(expressionPartitionDesc, columnDefs, Collections.emptyMap());
        } catch (Exception e) {
            throw new RuntimeException("Failed to analyze expression partition desc, ", e);
        }

        expressionPartitionDesc.setSystem(true);
        return expressionPartitionDesc;
    }

    private static DistributionDesc getDistributionDesc(OlapTable table) {
        if (table.getPartitionInfo().isPartitioned()) {
            return table.getDefaultDistributionInfo().toDistributionDesc(table.getIdToColumn());
        }

        Collection<Partition> partitions = table.getPartitions();
        if (partitions.isEmpty()) {
            return table.getDefaultDistributionInfo().toDistributionDesc(table.getIdToColumn());
        }

        // Use distribution desc of the first partition
        return partitions.iterator().next().getDistributionInfo().toDistributionDesc(table.getIdToColumn());
    }

    private static Map<String, String> getProperties(OlapTable table) {
        Map<String, String> properties = table.getProperties();
        // add schema_version
        MaterializedIndexMeta baseIndexMeta = table.getIndexMetaByMetaId(table.getBaseIndexMetaId());
        properties.put(PropertyAnalyzer.PROPERTIES_SCHEMA_VERSION, String.valueOf(baseIndexMeta.getSchemaVersion()));
        // labels.location is not supported now
        properties.remove(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION);
        // colocate_with is not supported now
        properties.remove(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH);
        return properties;
    }

    private static List<OrderByElement> getSortKeysColumnNames(OlapTable table) {
        List<OrderByElement> orderByElements = null;
        MaterializedIndexMeta baseIndexMeta = table.getIndexMetaByMetaId(table.getBaseIndexMetaId());
        if (baseIndexMeta.getSortKeyIdxes() != null) {
            orderByElements = Lists.newArrayListWithCapacity(baseIndexMeta.getSortKeyIdxes().size());
            for (Integer i : baseIndexMeta.getSortKeyIdxes()) {
                String columnName = table.getBaseSchema().get(i).getName();
                OrderByElement orderByElement = new OrderByElement(
                        ColumnIdExpr.fromSql(columnName).getExpr(), true, true);
                orderByElements.add(orderByElement);
            }
        }
        return orderByElements;
    }
}
