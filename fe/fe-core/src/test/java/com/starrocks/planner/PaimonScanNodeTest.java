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

import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Type;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.RawFile;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.paimon.io.DataFileMeta.DUMMY_LEVEL;
import static org.apache.paimon.io.DataFileMeta.EMPTY_MAX_KEY;
import static org.apache.paimon.io.DataFileMeta.EMPTY_MIN_KEY;
import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;

public class PaimonScanNodeTest {
    @Test
    public void testInit(@Mocked GlobalStateMgr globalStateMgr,
                         @Mocked CatalogConnector connector,
                         @Mocked PaimonTable table) {
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
            }
        };
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");
    }

    @Test
    public void testTotalFileLength(@Mocked PaimonTable table) {
        BinaryRow row1 = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row1, 10);
        writer.writeInt(0, 2000);
        writer.writeInt(1, 4444);
        writer.complete();

        List<DataFileMeta> meta1 = new ArrayList<>();
        meta1.add(new DataFileMeta("file1", 100, 200, EMPTY_MIN_KEY, EMPTY_MAX_KEY, EMPTY_STATS, EMPTY_STATS,
                1, 1, 1, DUMMY_LEVEL, 0L, null, null, null));
        meta1.add(new DataFileMeta("file2", 100, 300, EMPTY_MIN_KEY, EMPTY_MAX_KEY, EMPTY_STATS, EMPTY_STATS,
                1, 1, 1, DUMMY_LEVEL, 0L, null, null, null));

        DataSplit split = DataSplit.builder().withSnapshot(1L).withPartition(row1).withBucket(1)
                .withBucketPath("not used").withDataFiles(meta1).isStreaming(false).build();

        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");
        long totalFileLength = scanNode.getTotalFileLength(split);

        Assert.assertEquals(200, totalFileLength);
    }

    @Test
    public void testEstimatedLength(@Mocked PaimonTable table) {
        BinaryRow row1 = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row1, 10);
        writer.writeInt(0, 2000);
        writer.writeInt(1, 4444);
        writer.complete();

        List<DataFileMeta> meta1 = new ArrayList<>();
        meta1.add(new DataFileMeta("file1", 100, 200, EMPTY_MIN_KEY, EMPTY_MAX_KEY, EMPTY_STATS, EMPTY_STATS,
                1, 1, 1, DUMMY_LEVEL, 0L, null, null, null));
        meta1.add(new DataFileMeta("file2", 100, 300, EMPTY_MIN_KEY, EMPTY_MAX_KEY, EMPTY_STATS, EMPTY_STATS,
                1, 1, 1, DUMMY_LEVEL, 0L, null, null, null));

        DataSplit split = DataSplit.builder().withSnapshot(1L).withPartition(row1).withBucket(1)
                .withBucketPath("not used").withDataFiles(meta1).isStreaming(false).build();

        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        SlotDescriptor slot1 = new SlotDescriptor(new SlotId(1), "id", Type.INT, false);
        slot1.setColumn(new Column("id", Type.INT));
        SlotDescriptor slot2 = new SlotDescriptor(new SlotId(2), "name", Type.STRING, false);
        slot2.setColumn(new Column("name", Type.STRING));
        desc.addSlot(slot1);
        desc.addSlot(slot2);
        PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");
        long totalFileLength = scanNode.getEstimatedLength(split.rowCount(), desc);
        Assert.assertEquals(10000, totalFileLength);
    }

    @Test
    public void testSplitRawFileScanRange(@Mocked PaimonTable table, @Mocked RawFile rawFile) {
        BinaryRow row1 = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row1, 10);
        writer.writeInt(0, 2000);
        writer.writeInt(1, 4444);
        writer.complete();

        List<DataFileMeta> meta1 = new ArrayList<>();

        meta1.add(new DataFileMeta("file1", 100, 200, EMPTY_MIN_KEY, EMPTY_MAX_KEY, EMPTY_STATS, EMPTY_STATS,
                1, 1, 1, DUMMY_LEVEL, 0L, null, null, null));
        meta1.add(new DataFileMeta("file2", 100, 300, EMPTY_MIN_KEY, EMPTY_MAX_KEY, EMPTY_STATS, EMPTY_STATS,
                1, 1, 1, DUMMY_LEVEL, 0L, null, null, null));

        DataSplit split = DataSplit.builder().withSnapshot(1L).withPartition(row1).withBucket(1)
                .withBucketPath("not used").withDataFiles(meta1).isStreaming(false).build();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        new Expectations() {
            {
                rawFile.format();
                result = "orc";
            }
        };
        desc.setTable(table);
        PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");
        DeletionFile deletionFile = new DeletionFile("dummy", 1, 22, 0L);
        scanNode.splitRawFileScanRangeLocations(rawFile, deletionFile);
        scanNode.splitScanRangeLocations(rawFile, 0, 256 * 1024 * 1024, 64 * 1024 * 1024, null);
        scanNode.addSplitScanRangeLocations(split, null, 256 * 1024 * 1024);
    }
}