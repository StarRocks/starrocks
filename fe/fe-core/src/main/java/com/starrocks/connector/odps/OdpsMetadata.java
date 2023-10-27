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

import com.aliyun.odps.Odps;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.split.InputSplitAssigner;
import com.aliyun.odps.table.read.split.InputSplitWithRowRange;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class OdpsMetadata implements ConnectorMetadata {

    private static Logger LOG = LogManager.getLogger(OdpsMetadata.class);

    private Odps odps;
    private String catalogName;
    private EnvironmentSettings settings;
    private AliyunCloudCredential aliyunCloudCredential;

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
            if (listDbNames().contains(name)) {
                return new Database(0, name);
            } else {
                return null;
            }
        } catch (StarRocksConnectorException e) {
            return null;
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return ImmutableList.of("test_table");
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        com.aliyun.odps.Table table = odps.tables().get(dbName, tblName);
        return new OdpsTable(catalogName, table);
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName) {
        return ImmutableList.of();
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        return ImmutableList.of();
    }

    @Override
    public List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys,
                                                   long snapshotId, ScalarOperator predicate,
                                                   List<String> columnNames, long limit) {
        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        OdpsTable odpsTable = (OdpsTable) table;
        TableReadSessionBuilder scanBuilder = new TableReadSessionBuilder();
        try {
            TableBatchReadSession
                    scan =
                    scanBuilder.identifier(TableIdentifier.of(odpsTable.getProjectName(), odpsTable.getTableName()))
                            .withSettings(settings)
                            .requiredDataColumns(columnNames)
                            .withSplitOptions(SplitOptions.newBuilder().SplitByRowOffset().build())
                            .buildBatchReadSession();
            InputSplitAssigner assigner = scan.getInputSplitAssigner();
            InputSplitWithRowRange[] allSplits = (InputSplitWithRowRange[]) assigner.getAllSplits();
            RemoteFileDesc odpsRemoteFileDesc = RemoteFileDesc.createOdpsRemoteFileDesc(Arrays.asList(allSplits));
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
        return new AliyunCloudConfiguration(aliyunCloudCredential);
    }
}
