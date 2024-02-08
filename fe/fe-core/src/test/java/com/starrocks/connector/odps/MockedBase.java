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
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Project;
import com.aliyun.odps.Projects;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.Tables;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.impl.batch.TableBatchReadSessionImpl;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitAssigner;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplitAssigner;
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplit;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableList;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.informationschema.InformationSchemaConnector;
import com.starrocks.credential.aliyun.AliyunCloudConfiguration;
import com.starrocks.credential.aliyun.AliyunCloudCredential;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

public class MockedBase {
    protected static AliyunCloudCredential aliyunCloudCredential =
            new AliyunCloudCredential("ak", "sk", "http://127.0.0.1");
    protected static OdpsProperties odpsProperties;
    protected static Odps odps = Mockito.mock(Odps.class);
    protected static Tables tables = Mockito.mock(Tables.class);
    protected static Projects projects = Mockito.mock(Projects.class);
    protected static Partition partition = Mockito.mock(Partition.class);
    protected static Iterator<Table> tableIterator = Mockito.mock(Iterator.class);
    protected static Iterator<Project> projectIterator = Mockito.mock(Iterator.class);
    protected static Table table = Mockito.mock(Table.class);
    protected static Project project = Mockito.mock(Project.class);
    protected static SecurityManager securityManager = Mockito.mock(SecurityManager.class);
    protected static Account account = Mockito.mock(AliyunAccount.class);
    protected static OdpsConnector odpsConnector = Mockito.mock(OdpsConnector.class);
    protected static OdpsMetadata odpsMetadata = Mockito.mock(OdpsMetadata.class);
    protected static ConnectorContext context = Mockito.mock(ConnectorContext.class);

    @Mock
    static TableReadSessionBuilder mockTableReadSessionBuilder = Mockito.mock(TableReadSessionBuilder.class);
    static TableBatchReadSession tableBatchReadSession = Mockito.mock(TableBatchReadSessionImpl.class);
    static InputSplitAssigner inputSplitAssigner = Mockito.mock(IndexedInputSplitAssigner.class);

    static MockedStatic<GlobalStateMgr> mockedStatic = Mockito.mockStatic(GlobalStateMgr.class);
    static GlobalStateMgr globalStateMgr = Mockito.mock(GlobalStateMgr.class);
    static CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
    static ConnectorMgr connectorMgr = Mockito.mock(ConnectorMgr.class);
    static MetadataMgr metadataMgr = Mockito.mock(MetadataMgr.class);
    static OdpsSplitsInfo odpsSplitsInfo = Mockito.mock(OdpsSplitsInfo.class);

    public static void initMock() throws OdpsException, IOException {
        Map<String, String> properties = new HashMap<>();
        properties.put("odps.access.id", "ak");
        properties.put("odps.access.key", "sk");
        properties.put("odps.endpoint", "http://127.0.0.1");
        properties.put("odps.project", "project");
        odpsProperties = new OdpsProperties(properties);

        when(odps.getEndpoint()).thenReturn("http://127.0.0.1");
        when(odps.getDefaultProject()).thenReturn("default_project");
        when(odps.tables()).thenReturn(tables);
        when(odps.getAccount()).thenReturn(account);
        when(odps.projects()).thenReturn(projects);

        when(tables.iterator(anyString())).thenReturn(tableIterator);
        when(tables.get(anyString(), anyString())).thenReturn(table);

        when(projects.iterator(anyString())).thenReturn(projectIterator);
        when(projects.get()).thenReturn(project);

        when(project.getSecurityManager()).thenReturn(securityManager);
        when(securityManager.runQuery(eq("whoami"), anyBoolean())).thenReturn("{\n" +
                "    \"DisplayName\": \"ALIYUN$test@test.aliyun.com\",\n" +
                "    \"ID\": \"123456789\",\n" +
                "    \"SourceIP\": \"127.0.0.1\"}");

        when(project.getName()).thenReturn("project");
        when(tableIterator.hasNext()).thenReturn(true, false);
        when(tableIterator.next()).thenReturn(table);

        when(projectIterator.hasNext()).thenReturn(true, false);
        when(projectIterator.next()).thenReturn(project);

        when(table.getName()).thenReturn("tableName");
        when(table.getCreatedTime()).thenReturn(new Date());
        when(table.getProject()).thenReturn("project");
        doNothing().when(table).reload();
        when(table.getPartitions()).thenReturn(ImmutableList.of(partition));

        when(partition.getPartitionSpec()).thenReturn(new PartitionSpec("p1=a/p2=b"));
        when(partition.getLastDataModifiedTime()).thenReturn(new Date());

        TableSchema tableSchema = new TableSchema();
        tableSchema.addColumn(new Column("c1", TypeInfoFactory.STRING));
        tableSchema.addColumn(new Column("c2", TypeInfoFactory.BIGINT));
        tableSchema.addPartitionColumn(new Column("p1", TypeInfoFactory.STRING));
        tableSchema.addPartitionColumn(new Column("p2", TypeInfoFactory.STRING));
        when(table.getSchema()).thenReturn(tableSchema);

        when(mockTableReadSessionBuilder.identifier(any())).thenReturn(mockTableReadSessionBuilder);
        when(mockTableReadSessionBuilder.withSettings(any())).thenReturn(mockTableReadSessionBuilder);
        when(mockTableReadSessionBuilder.requiredPartitions(any())).thenReturn(mockTableReadSessionBuilder);
        when(mockTableReadSessionBuilder.requiredDataColumns(any())).thenReturn(mockTableReadSessionBuilder);
        when(mockTableReadSessionBuilder.withSplitOptions(any())).thenReturn(mockTableReadSessionBuilder);

        when(mockTableReadSessionBuilder.buildBatchReadSession()).thenReturn(tableBatchReadSession);
        when(tableBatchReadSession.getInputSplitAssigner()).thenReturn(inputSplitAssigner);

        InputSplit split0 = new IndexedInputSplit("session", 0);
        InputSplit split1 = new IndexedInputSplit("session", 1);

        when(inputSplitAssigner.getAllSplits()).thenReturn(new InputSplit[] {split0, split1});
        when(inputSplitAssigner.getSplit(0)).thenReturn(split0);
        when(inputSplitAssigner.getSplit(1)).thenReturn(split1);
        when(inputSplitAssigner.getSplitsCount()).thenReturn(2);

        InputSplit split2 = new RowRangeInputSplit("session", 0, 10000L);
        when(inputSplitAssigner.getTotalRowCount()).thenReturn(10000L);
        when(inputSplitAssigner.getSplitByRowOffset(0, 10000L)).thenReturn(split2);

        Mockito.when(context.getProperties()).thenReturn(properties);
        Mockito.when(context.getCatalogName()).thenReturn("catalog");
        Mockito.when(context.getType()).thenReturn("odps");

        when(odpsMetadata.getCloudConfiguration()).thenReturn(new AliyunCloudConfiguration(aliyunCloudCredential));

        when(odpsConnector.getMetadata()).thenReturn(odpsMetadata);
        mockedStatic.when(GlobalStateMgr::getCurrentState).thenReturn(globalStateMgr);
        when(globalStateMgr.getCatalogMgr()).thenReturn(catalogMgr);
        when(globalStateMgr.getConnectorMgr()).thenReturn(connectorMgr);
        when(globalStateMgr.getMetadataMgr()).thenReturn(metadataMgr);
        when(connectorMgr.getConnector(anyString())).thenReturn(
                new CatalogConnector(odpsConnector, new InformationSchemaConnector("catalog")));
        when(odpsMetadata.getCloudConfiguration()).thenReturn(new AliyunCloudConfiguration(aliyunCloudCredential));

        RemoteFileInfo fileInfo = new RemoteFileInfo();
        fileInfo.setFiles(ImmutableList.of(RemoteFileDesc.createOdpsRemoteFileDesc(odpsSplitsInfo)));
        when(metadataMgr.getRemoteFileInfos(any(), any(), any(), anyLong(), any(), any(), anyLong())).thenReturn(
                ImmutableList.of(fileInfo));
        when(odpsMetadata.getRemoteFileInfos(any(), any(), anyLong(), any(), any(), anyLong(), any())).thenReturn(
                ImmutableList.of(fileInfo));
    }
}
