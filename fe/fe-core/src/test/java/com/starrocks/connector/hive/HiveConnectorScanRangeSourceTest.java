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

package com.starrocks.connector.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.type.PrimitiveType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class HiveConnectorScanRangeSourceTest {

    private MockedStatic<GlobalStateMgr> globalStateMgrMockedStatic;

    @BeforeEach
    public void before() {
        globalStateMgrMockedStatic = Mockito.mockStatic(GlobalStateMgr.class);
    }

    @AfterEach
    public void after() {
        ConnectContext.remove();
        globalStateMgrMockedStatic.close();
    }

    @Test
    public void testInitRemoteFileInfoSourceUsesPartitionKeys() {
        GlobalStateMgr globalStateMgr = Mockito.mock(GlobalStateMgr.class);
        globalStateMgrMockedStatic.when(GlobalStateMgr::getCurrentState).thenReturn(globalStateMgr);
        Mockito.when(globalStateMgr.getVariableMgr()).thenReturn(new VariableMgr());

        MetadataMgr metadataMgr = Mockito.mock(MetadataMgr.class);
        Mockito.when(globalStateMgr.getMetadataMgr()).thenReturn(metadataMgr);

        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setEnableScanDataCache(false);
        ConnectContext.set(context);

        HiveTable table = Mockito.mock(HiveTable.class);
        Mockito.when(table.getPartitionColumnNames()).thenReturn(ImmutableList.of("col1"));
        Mockito.when(table.getCatalogName()).thenReturn("hive_catalog");
        Mockito.when(table.getCatalogDBName()).thenReturn("db1");
        Mockito.when(table.getCatalogTableName()).thenReturn("tbl1");

        PartitionKey partitionKey = new PartitionKey();
        partitionKey.pushColumn(new IntLiteral(1), PrimitiveType.INT);
        Map<Long, PartitionKey> idToPartitionKey = Maps.newHashMap();
        idToPartitionKey.put(100L, partitionKey);

        HDFSScanNodePredicates predicates = new HDFSScanNodePredicates();
        predicates.setIdToPartitionKey(idToPartitionKey);
        predicates.setSelectedPartitionIds(Lists.newArrayList(100L));

        DescriptorTable descriptorTable = new DescriptorTable();
        TestHiveConnectorScanRangeSource scanRangeSource =
                new TestHiveConnectorScanRangeSource(descriptorTable, table, predicates);

        ArgumentCaptor<GetRemoteFilesParams> paramsCaptor = ArgumentCaptor.forClass(GetRemoteFilesParams.class);
        Mockito.when(metadataMgr.getRemoteFilesAsync(Mockito.eq(table), paramsCaptor.capture()))
                .thenReturn(Mockito.mock(RemoteFileInfoSource.class));

        scanRangeSource.invokeInitRemoteFileInfoSource();

        GetRemoteFilesParams capturedParams = paramsCaptor.getValue();
        assertThat(capturedParams.getPartitionNames()).isNull();
        assertThat(capturedParams.getPartitionKeys()).containsExactly(partitionKey);
        assertThat(capturedParams.getPartitionAttachments()).hasSize(1);
        HiveConnectorScanRangeSource.PartitionAttachment attachment =
                (HiveConnectorScanRangeSource.PartitionAttachment) capturedParams.getPartitionAttachments().get(0);
        assertThat(attachment.partitionId).isEqualTo(100L);
    }

    private static class TestHiveConnectorScanRangeSource extends HiveConnectorScanRangeSource {
        public TestHiveConnectorScanRangeSource(DescriptorTable descriptorTable, HiveTable table,
                                               HDFSScanNodePredicates scanNodePredicates) {
            super(descriptorTable, table, scanNodePredicates);
        }

        public void invokeInitRemoteFileInfoSource() {
            super.initRemoteFileInfoSource();
        }
    }
}
