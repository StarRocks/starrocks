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

package com.starrocks.planner;

import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergRemoteFileInfo;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class IcebergScanNodeTest {
    @Test
    public void testInit(@Mocked GlobalStateMgr globalStateMgr,
                         @Mocked CatalogConnector connector,
                         @Mocked IcebergTable table,
                         @Mocked IcebergTableMORParams tableMORParams,
                         @Mocked FileScanTask fileScanTask,
                         @Mocked DataFile mockDataFile) {
        String catalog = "XXX";
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(new HashMap<>());
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalog);
                result = connector;
                connector.getMetadata().getCloudConfiguration();
                result = cc;
                table.getCatalogName();
                result = catalog;
                fileScanTask.file();
                result = mockDataFile;
                mockDataFile.fileSizeInBytes();
                result = 10000000L;
            }
        };
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        IcebergScanNode scanNode = new IcebergScanNode(new PlanNodeId(0), desc, "XXX", 
                tableMORParams, IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE);
        scanNode.setSnapshotId(Optional.of(12345L));        
        IcebergRemoteFileInfo remoteFileInfo = new IcebergRemoteFileInfo(fileScanTask);
        List<RemoteFileInfo> remoteFileInfos = List.of(remoteFileInfo);
        try {
            scanNode.rebuildScanRange(remoteFileInfos);
        } catch (Exception e) {
            Assertions.fail("Rebuild scan range should not throw exception: " + e.getMessage());
        }
        List<FileScanTask> res = scanNode.getSourceRange().getSourceFileScanOutputs(10, 256 * 1024 * 1024);
        Assertions.assertEquals(1, res.size());
        Assertions.assertEquals(1, scanNode.getScannedDataFiles().size());
        Assertions.assertEquals(0, scanNode.getAppliedDeleteFiles().size());
    }
}