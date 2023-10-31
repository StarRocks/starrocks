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

package com.starrocks.connector.odps;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Partition;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.split.InputSplitAssigner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OdpsTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.aliyun.AliyunCloudConfiguration;
import com.starrocks.credential.aliyun.AliyunCloudCredential;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class OdpsMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(OdpsMetadata.class);

    private final Odps odps;
    private final String catalogName;
    private final EnvironmentSettings settings;
    private final AliyunCloudCredential aliyunCloudCredential;

    public OdpsMetadata(Odps odps, String catalogName, AliyunCloudCredential aliyunCloudCredential) {
        this.odps = odps;
        this.catalogName = catalogName;
        this.aliyunCloudCredential = aliyunCloudCredential;
        settings = EnvironmentSettings.newBuilder().withServiceEndpoint(odps.getEndpoint())
                .withCredentials(Credentials.newBuilder().withAccount(odps.getAccount()).build()).build();
    }

    @Override
    public List<String> listDbNames() {
        return ImmutableList.of(odps.getDefaultProject());
    }

    @Override
    public Database getDb(String name) {
        try {
            return new Database(0, name);
        } catch (StarRocksConnectorException e) {
            return null;
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        Iterator<com.aliyun.odps.Table> iterator = odps.tables().iterator(dbName);
        while (iterator.hasNext()) {
            builder.add(iterator.next().getName());
        }
        return builder.build();
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        com.aliyun.odps.Table table = odps.tables().get(dbName, tblName);
        return new OdpsTable(catalogName, table);
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName) {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (Partition partition : odps.tables().get(databaseName, tableName).getPartitions()) {
            builder.add(partition.getPartitionSpec().toString());
        }
        return builder.build();
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        ImmutableList.Builder<PartitionInfo> builder = ImmutableList.builder();
        for (String partitionName : partitionNames) {
            com.starrocks.connector.hive.Partition partition =
                    com.starrocks.connector.hive.Partition.builder().setFullPath(partitionName).build();
            builder.add(partition);
        }
        return builder.build();
    }

    @Override
    public List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys,
                                                   long snapshotId, ScalarOperator predicate,
                                                   List<String> columnNames, long limit) {
        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        OdpsTable odpsTable = (OdpsTable) table;
        TableReadSessionBuilder scanBuilder = new TableReadSessionBuilder();

        TableSchema schema = odps.tables().get(odpsTable.getProjectName(), odpsTable.getTableName()).getSchema();
        Set<String> set = columnNames.stream().collect(Collectors.toSet());
        List<String> orderedColumnNames = new ArrayList<>();
        for (Column column : schema.getColumns()) {
            if (set.contains(column.getName())) {
                orderedColumnNames.add(column.getName());
            }
        }
        try {
            LOG.info("get remote file infos, project:{}, table:{}, columns:{}", odpsTable.getProjectName(),
                    odpsTable.getTableName(), columnNames);
            TableBatchReadSession
                    scan =
                    scanBuilder.identifier(TableIdentifier.of(odpsTable.getProjectName(), odpsTable.getTableName()))
                            .withSettings(settings)
                            .requiredDataColumns(orderedColumnNames)
                            .withSplitOptions(SplitOptions.createDefault())
                            .buildBatchReadSession();
            InputSplitAssigner assigner = scan.getInputSplitAssigner();
            OdpsSplitsInfo odpsSplitsInfo = new OdpsSplitsInfo(Arrays.asList(assigner.getAllSplits()), scan);
            RemoteFileDesc odpsRemoteFileDesc = RemoteFileDesc.createOdpsRemoteFileDesc(odpsSplitsInfo);
            List<RemoteFileDesc> remoteFileDescs = ImmutableList.of(odpsRemoteFileDesc);
            remoteFileInfo.setFiles(remoteFileDescs);
            return Lists.newArrayList(remoteFileInfo);
        } catch (Exception e) {
            LOG.error("getRemoteFileInfos error", e);
        }
        return Collections.emptyList();
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        AliyunCloudConfiguration configuration = new AliyunCloudConfiguration(aliyunCloudCredential);
        configuration.loadCommonFields(new HashMap<>(0));
        return configuration;
    }
}
