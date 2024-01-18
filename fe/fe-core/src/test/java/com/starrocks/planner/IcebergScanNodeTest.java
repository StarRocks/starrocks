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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.exception.UserException;
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TIcebergDeleteFile;
import com.starrocks.thrift.TIcebergFileContent;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static com.starrocks.catalog.Type.INT;
import static com.starrocks.catalog.Type.STRING;

public class IcebergScanNodeTest extends TableTestBase {
    StarRocksAssert starRocksAssert = new StarRocksAssert();

    public IcebergScanNodeTest() throws IOException {
    }

    @Before
    public void before() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        String createCatalog = "CREATE EXTERNAL CATALOG iceberg_catalog PROPERTIES(\"type\"=\"iceberg\", " +
                "\"iceberg.catalog.hive.metastore.uris\"=\"thrift://127.0.0.1:9083\", \"iceberg.catalog.type\"=\"hive\")";
        starRocksAssert.withCatalog(createCatalog);
    }

    @After
    public void after() throws Exception {
        starRocksAssert.dropCatalog("iceberg_catalog");
    }

    @Test
    public void testGetScanRangeLocations() throws Exception {
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("k2", INT));
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "iceberg_db",
                "iceberg_table", columns, mockedNativeTableC, Maps.newHashMap());
        Analyzer analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), new ConnectContext());
        DescriptorTable descTable = analyzer.getDescTbl();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        tupleDesc.setTable(icebergTable);
        IcebergScanNode scanNode = new IcebergScanNode(new PlanNodeId(0), tupleDesc, "IcebergScanNode", tupleDesc);

        mockedNativeTableC.newRowDelta().addRows(FILE_B_1).addDeletes(FILE_C_1).commit();
        mockedNativeTableC.refresh();

        scanNode.setupScanRangeLocations(descTable);

        List<TScanRangeLocations> result = scanNode.getScanRangeLocations(1);
        Assert.assertTrue(result.size() > 0);
        TScanRange scanRange = result.get(0).scan_range;
        Assert.assertTrue(scanRange.isSetHdfs_scan_range());
        THdfsScanRange hdfsScanRange = scanRange.hdfs_scan_range;
        Assert.assertEquals("/path/to/data-b1.parquet", hdfsScanRange.full_path);
        Assert.assertEquals(1, hdfsScanRange.delete_files.size());
        TIcebergDeleteFile deleteFile = hdfsScanRange.delete_files.get(0);
        Assert.assertEquals("delete.orc", deleteFile.full_path);
        Assert.assertEquals(TIcebergFileContent.POSITION_DELETES, deleteFile.file_content);
    }

    @Test
    public void testEqualityDelete() throws UserException {
        List<Column> columns = Lists.newArrayList(new Column("id", INT), new Column("data", STRING));
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "iceberg_db",
                "iceberg_table", columns, mockedNativeTableA, Maps.newHashMap());
        Analyzer analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), new ConnectContext());
        DescriptorTable descTable = analyzer.getDescTbl();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        TupleDescriptor eqTupleDesc = descTable.createTupleDescriptor("eqTuple");
        tupleDesc.setTable(icebergTable);
        IcebergScanNode scanNode = new IcebergScanNode(new PlanNodeId(0), tupleDesc, "IcebergScanNode", eqTupleDesc);

        mockedNativeTableA.newAppend().appendFile(FILE_A).commit();
        // FILE_A_DELETES = positionalDelete / FILE_A2_DELETES = equalityDelete
        mockedNativeTableA.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_A2_DELETES).commit();
        mockedNativeTableC.refresh();

        scanNode.setupScanRangeLocations(descTable);

        List<TScanRangeLocations> result = scanNode.getScanRangeLocations(1);
        Assert.assertTrue(result.size() > 0);
        TScanRange scanRange = result.get(0).scan_range;
        Assert.assertTrue(scanRange.isSetHdfs_scan_range());
        THdfsScanRange hdfsScanRange = scanRange.hdfs_scan_range;
        Assert.assertEquals("/path/to/data-a.parquet", hdfsScanRange.full_path);
        Assert.assertEquals(2, hdfsScanRange.delete_files.size());
        TIcebergDeleteFile tDeleteFile = hdfsScanRange.delete_files.get(0);
        Assert.assertEquals("/path/to/data-a2-deletes.parquet", tDeleteFile.full_path);
        Assert.assertEquals(TIcebergFileContent.EQUALITY_DELETES, tDeleteFile.file_content);
        Assert.assertEquals(2147473647, hdfsScanRange.delete_column_slot_ids.get(0).intValue());
        Assert.assertEquals(1, hdfsScanRange.delete_column_slot_ids.size());
        Assert.assertEquals(1, eqTupleDesc.getSlots().size());
    }
}
