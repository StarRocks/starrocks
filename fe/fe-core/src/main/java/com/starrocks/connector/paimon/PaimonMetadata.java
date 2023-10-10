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

package com.starrocks.connector.paimon;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.system.SchemasTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class PaimonMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(PaimonMetadata.class);
    private final Catalog paimonNativeCatalog;
    private final HdfsEnvironment hdfsEnvironment;
    private final String catalogType;
    private final String metastoreUris;
    private final String warehousePath;
    private final String catalogName;
    private final Map<Identifier, Table> tables = new ConcurrentHashMap<>();
    private final Map<String, Database> databases = new ConcurrentHashMap<>();
    private final Map<PaimonFilter, PaimonSplitsInfo> paimonSplits = new ConcurrentHashMap<>();

    public PaimonMetadata(String catalogName, HdfsEnvironment hdfsEnvironment, Catalog paimonNativeCatalog,
                          String catalogType, String metastoreUris, String warehousePath) {
        this.paimonNativeCatalog = paimonNativeCatalog;
        this.hdfsEnvironment = hdfsEnvironment;
        this.catalogType = catalogType;
        this.metastoreUris = metastoreUris;
        this.warehousePath = warehousePath;
        this.catalogName = catalogName;
    }

    @Override
    public List<String> listDbNames() {
        return paimonNativeCatalog.listDatabases();
    }

    @Override
    public List<String> listTableNames(String dbName) {
        try {
            return paimonNativeCatalog.listTables(dbName);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new StarRocksConnectorException("Database %s not exists", dbName);
        }
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName) {
        Identifier identifier = new Identifier(databaseName, tableName);
        org.apache.paimon.table.Table paimonTable;
        try {
            paimonTable = this.paimonNativeCatalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            throw new StarRocksConnectorException(String.format("Paimon table %s.%s does not exist.", databaseName, tableName));
        }

        List<String> partitionNames = Lists.newArrayList();

        List<String> partitionColumnNames = paimonTable.partitionKeys();
        if (partitionColumnNames.isEmpty()) {
            return partitionNames;
        }

        AbstractFileStoreTable dataTable = (AbstractFileStoreTable) paimonTable;
        RowDataConverter converter = new RowDataConverter(dataTable.schema().logicalPartitionType());

        ReadBuilder readBuilder = paimonTable.newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();

        for (Split split : splits) {
            DataSplit dataSplit = (DataSplit) split;
            List<String> partitionValues = dataSplit.partition() == null ? null :
                    converter.convert(dataSplit.partition(), partitionColumnNames);
            String partitionName = FileUtils.makePartName(partitionColumnNames, partitionValues);
            partitionNames.add(partitionName);
        }
        return partitionNames;
    }

    @Override
    public Database getDb(String dbName) {
        if (databases.containsKey(dbName)) {
            return databases.get(dbName);
        }
        if (paimonNativeCatalog.databaseExists(dbName)) {
            Database db = new Database(CONNECTOR_ID_GENERATOR.getNextId().asInt(), dbName);
            databases.put(dbName, db);
            return db;
        } else {
            LOG.error("Paimon database {}.{} done not exist.", catalogName, dbName);
            return null;
        }
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        Identifier identifier = new Identifier(dbName, tblName);
        if (tables.containsKey(identifier)) {
            return tables.get(identifier);
        }
        org.apache.paimon.table.Table paimonNativeTable;
        try {
            paimonNativeTable = this.paimonNativeCatalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            LOG.error("Paimon table {}.{} does not exist.", dbName, tblName, e);
            return null;
        }
        List<DataField> fields = paimonNativeTable.rowType().getFields();
        ArrayList<Column> fullSchema = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            String fieldName = field.name();
            DataType type = field.type();
            Type fieldType = ColumnTypeConverter.fromPaimonType(type);
            Column column = new Column(fieldName, fieldType, true);
            fullSchema.add(column);
        }
        long createTime = 0;
        try {
            createTime = getTableCreateTime(dbName, tblName);
        } catch (Exception e) {
            LOG.error("Get paimon table {}.{} createtime failed, error: {}", dbName, tblName, e);
        }
        PaimonTable table = new PaimonTable(catalogName, dbName, tblName, fullSchema,
                catalogType, metastoreUris, warehousePath, paimonNativeTable, createTime);
        tables.put(identifier, table);
        return table;
    }

    @Override
    public List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys,
                                                   long snapshotId, ScalarOperator predicate,
                                                   List<String> fieldNames, long limit) {
        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        PaimonTable paimonTable = (PaimonTable) table;
        PaimonFilter filter = new PaimonFilter(paimonTable.getDbName(), paimonTable.getTableName(), predicate, fieldNames);
        if (!paimonSplits.containsKey(filter)) {
            ReadBuilder readBuilder = paimonTable.getNativeTable().newReadBuilder();
            int[] projected = fieldNames.stream().mapToInt(name -> (paimonTable.getFieldNames().indexOf(name))).toArray();
            List<Predicate> predicates = extractPredicates(paimonTable, predicate);
            List<Split> splits = readBuilder.withFilter(predicates).withProjection(projected).newScan().plan().splits();
            PaimonSplitsInfo paimonSplitsInfo = new PaimonSplitsInfo(predicates, splits);
            paimonSplits.put(filter, paimonSplitsInfo);
            List<RemoteFileDesc> remoteFileDescs = ImmutableList.of(
                    RemoteFileDesc.createPamonRemoteFileDesc(paimonSplitsInfo));
            remoteFileInfo.setFiles(remoteFileDescs);
        } else {
            List<RemoteFileDesc> remoteFileDescs = ImmutableList.of(
                    RemoteFileDesc.createPamonRemoteFileDesc(paimonSplits.get(filter)));
            remoteFileInfo.setFiles(remoteFileDescs);
        }

        return Lists.newArrayList(remoteFileInfo);
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate,
                                         long limit) {
        Statistics.Builder builder = Statistics.builder();
        for (ColumnRefOperator columnRefOperator : columns.keySet()) {
            builder.addColumnStatistic(columnRefOperator, ColumnStatistic.unknown());
        }

        List<String> fieldNames = columns.keySet().stream().map(ColumnRefOperator::getName).collect(Collectors.toList());
        List<RemoteFileInfo> fileInfos = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFileInfos(
                catalogName, table, null, -1, predicate, fieldNames, limit);
        RemoteFileDesc remoteFileDesc = fileInfos.get(0).getFiles().get(0);
        List<Split> splits = remoteFileDesc.getPaimonSplitsInfo().getPaimonSplits();
        long rowCount = getRowCount(splits);
        if (rowCount == 0) {
            builder.setOutputRowCount(1);
        } else {
            builder.setOutputRowCount(rowCount);
        }

        return builder.build();
    }

    public static long getRowCount(List<? extends Split> splits) {
        long rowCount = 0;
        for (Split split : splits) {
            DataSplit dataSplit = (DataSplit) split;
            rowCount += dataSplit.dataFiles().stream().map(DataFileMeta::rowCount).reduce(0L, Long::sum);
        }
        return rowCount;
    }

    private List<Predicate> extractPredicates(PaimonTable paimonTable, ScalarOperator predicate) {
        List<ScalarOperator> scalarOperators = Utils.extractConjuncts(predicate);
        List<Predicate> predicates = new ArrayList<>(scalarOperators.size());

        PaimonPredicateConverter converter = new PaimonPredicateConverter(paimonTable.getNativeTable().rowType());
        for (ScalarOperator operator : scalarOperators) {
            Predicate filter = converter.convert(operator);
            if (filter != null) {
                predicates.add(filter);
            }
        }
        return predicates;
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        return hdfsEnvironment.getCloudConfiguration();
    }

    public long getTableCreateTime(String dbName, String tblName) throws Exception {
        Identifier sysIdentifier = new Identifier(dbName, String.format("%s%s", tblName, "$schemas"));
        RecordReaderIterator<InternalRow> iterator = null;
        try {
            SchemasTable table = (SchemasTable) paimonNativeCatalog.getTable(sysIdentifier);
            RowType rowType = table.rowType();
            if (!rowType.getFieldNames().contains("update_time")) {
                return 0;
            }
            int[] projected = new int[] {0, 6};
            PredicateBuilder predicateBuilder = new PredicateBuilder(rowType);
            Predicate equal = predicateBuilder.equal(predicateBuilder.indexOf("schema_id"), 0);
            RecordReader<InternalRow> recordReader = table.newReadBuilder().withProjection(projected).
                    withFilter(equal).newRead().createReader(table.newScan().plan());
            iterator = new RecordReaderIterator<>(recordReader);
            while (iterator.hasNext()) {
                InternalRow rowData = iterator.next();
                Long schemaIdValue = rowData.getLong(0);
                org.apache.paimon.data.Timestamp updateTime = rowData.getTimestamp(1, 3);
                if (schemaIdValue == 0) {
                    return updateTime.getMillisecond();
                }
            }
        } catch (Exception e) {
            LOG.error("Get paimon table {}.{} createtime failed, error: {}", dbName, tblName, e);
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
        return 0;
    }
}
