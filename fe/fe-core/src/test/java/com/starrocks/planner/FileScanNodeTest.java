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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.sql.ast.DataDescription;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TBrokerRangeDesc;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TUniqueId;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FileScanNodeTest {
    private long jobId;
    private long txnId;
    private TUniqueId loadId;
    private BrokerDesc brokerDesc;

    // config
    private int loadParallelInstanceNum;

    // backends
    private ImmutableMap<Long, Backend> idToBackend;

    @Mocked
    Partition partition;

    @Before
    public void setUp() {
        jobId = 1L;
        txnId = 2L;
        loadId = new TUniqueId(3, 4);
        brokerDesc = new BrokerDesc("broker0", null);

        loadParallelInstanceNum = Config.load_parallel_instance_num;

        // backends
        Map<Long, Backend> idToBackendTmp = Maps.newHashMap();
        Backend b1 = new Backend(0L, "host0", 9050);
        b1.setAlive(true);
        idToBackendTmp.put(0L, b1);
        Backend b2 = new Backend(1L, "host1", 9050);
        b2.setAlive(true);
        idToBackendTmp.put(1L, b2);
        Backend b3 = new Backend(2L, "host2", 9050);
        b3.setAlive(true);
        idToBackendTmp.put(2L, b3);
        idToBackend = ImmutableMap.copyOf(idToBackendTmp);
    }

    @Test
    public void testCreateScanRangeLocations(@Mocked GlobalStateMgr globalStateMgr,
                                             @Mocked SystemInfoService systemInfoService,
                                             @Injectable Database db, @Injectable OlapTable table)
            throws UserException {
        // table schema
        List<Column> columns = Lists.newArrayList();
        Column c1 = new Column("c1", Type.BIGINT, true);
        columns.add(c1);
        Column c2 = new Column("c2", Type.BIGINT, true);
        columns.add(c2);
        List<String> columnNames = Lists.newArrayList("c1", "c2");

        new Expectations() {
            {
                GlobalStateMgr.getCurrentSystemInfo();
                result = systemInfoService;
                systemInfoService.getIdToBackend();
                result = idToBackend;
                table.getBaseSchema();
                result = columns;
                table.getFullSchema();
                result = columns;
                table.getPartitions();
                minTimes = 0;
                result = Arrays.asList(partition);
                partition.getId();
                minTimes = 0;
                result = 0;
                table.getColumn("c1");
                result = columns.get(0);
                table.getColumn("c2");
                result = columns.get(1);
            }
        };

        // case 0
        // 2 csv files: file1 512M+, file2 256M-
        // result: 3 ranges. file1 3 ranges, file2 1 range

        // file groups
        List<BrokerFileGroup> fileGroups = Lists.newArrayList();
        List<String> files = Lists.newArrayList("hdfs://127.0.0.1:9001/file1", "hdfs://127.0.0.1:9001/file2");
        DataDescription desc =
                new DataDescription("testTable", null, files, columnNames, null, null, null, false, null);
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", "\t");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        fileGroups.add(brokerFileGroup);

        // file status
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        List<TBrokerFileStatus> fileStatusList = Lists.newArrayList();
        fileStatusList.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file1", false, 536870968, true));
        fileStatusList.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file2", false, 268435400, true));
        fileStatusesList.add(fileStatusList);

        Analyzer analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), new ConnectContext());
        DescriptorTable descTable = analyzer.getDescTbl();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        FileScanNode scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList, 2);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        scanNode.init(analyzer);
        scanNode.finalizeStats(analyzer);

        // check
        List<TScanRangeLocations> locationsList = scanNode.getScanRangeLocations(0);
        System.out.println(locationsList);
        Assert.assertEquals(3, locationsList.size());
        int file1RangesNum = 0;
        int file2RangesNum = 0;
        Set<Long> file1StartOffsetResult = Sets.newHashSet();
        Set<Long> file1RangeSizeResult = Sets.newHashSet();
        for (TScanRangeLocations locations : locationsList) {
            for (TBrokerRangeDesc rangeDesc : locations.scan_range.broker_scan_range.ranges) {
                long start = rangeDesc.start_offset;
                long size = rangeDesc.size;
                if (rangeDesc.path.endsWith("file1")) {
                    ++file1RangesNum;
                    file1StartOffsetResult.add(start);
                    file1RangeSizeResult.add(size);
                } else if (rangeDesc.path.endsWith("file2")) {
                    ++file2RangesNum;
                    Assert.assertTrue(start == 0);
                    Assert.assertTrue(size == 268435400);
                }
            }
        }
        Assert.assertEquals(Sets.newHashSet(0L, 268435456L, 536870912L), file1StartOffsetResult);
        Assert.assertEquals(Sets.newHashSet(56L, 268435456L), file1RangeSizeResult);
        Assert.assertEquals(3, file1RangesNum);
        Assert.assertEquals(1, file2RangesNum);

        // case 1
        // 4 parquet files
        // result: 3 ranges. 2 files in one range and 1 file in every other range

        // file groups
        fileGroups = Lists.newArrayList();
        files = Lists.newArrayList("hdfs://127.0.0.1:9001/file1", "hdfs://127.0.0.1:9001/file2",
                "hdfs://127.0.0.1:9001/file3", "hdfs://127.0.0.1:9001/file4");
        desc = new DataDescription("testTable", null, files, columnNames, null, null, "parquet", false, null);
        brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", "\t");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        Deencapsulation.setField(brokerFileGroup, "fileFormat", "parquet");
        fileGroups.add(brokerFileGroup);

        // file status
        fileStatusesList = Lists.newArrayList();
        fileStatusList = Lists.newArrayList();
        fileStatusList.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file1", false, 268435454, false));
        fileStatusList.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file2", false, 268435453, false));
        fileStatusList.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file3", false, 268435452, false));
        fileStatusList.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file4", false, 268435451, false));
        fileStatusesList.add(fileStatusList);

        analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), new ConnectContext());
        descTable = analyzer.getDescTbl();
        tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList, 4);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        scanNode.init(analyzer);
        scanNode.finalizeStats(analyzer);

        // check
        locationsList = scanNode.getScanRangeLocations(0);
        Assert.assertEquals(3, locationsList.size());
        for (TScanRangeLocations locations : locationsList) {
            List<TBrokerRangeDesc> rangeDescs = locations.scan_range.broker_scan_range.ranges;
            Assert.assertTrue(rangeDescs.size() == 1 || rangeDescs.size() == 2);
        }

        // case 2
        // 2 file groups
        // result: 4 ranges. group1 3 ranges, group2 1 range

        // file groups
        fileGroups = Lists.newArrayList();
        files = Lists.newArrayList("hdfs://127.0.0.1:9001/file1", "hdfs://127.0.0.1:9001/file2",
                "hdfs://127.0.0.1:9001/file3");
        desc = new DataDescription("testTable", null, files, columnNames, null, null, "parquet", false, null);
        brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", "\t");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        Deencapsulation.setField(brokerFileGroup, "fileFormat", "parquet");
        fileGroups.add(brokerFileGroup);

        List<String> files2 = Lists.newArrayList("hdfs://127.0.0.1:9001/file4", "hdfs://127.0.0.1:9001/file5");
        DataDescription desc2 =
                new DataDescription("testTable", null, files2, columnNames, null, null, null, false, null);
        BrokerFileGroup brokerFileGroup2 = new BrokerFileGroup(desc2);
        Deencapsulation.setField(brokerFileGroup2, "columnSeparator", "\t");
        Deencapsulation.setField(brokerFileGroup2, "rowDelimiter", "\n");
        fileGroups.add(brokerFileGroup2);

        // file status
        fileStatusesList = Lists.newArrayList();
        fileStatusList = Lists.newArrayList();
        fileStatusList.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file1", false, 268435456, false));
        fileStatusList.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file2", false, 10, false));
        fileStatusList.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file3", false, 10, false));
        fileStatusesList.add(fileStatusList);

        List<TBrokerFileStatus> fileStatusList2 = Lists.newArrayList();
        fileStatusList2.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file4", false, 10, true));
        fileStatusList2.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file5", false, 10, true));
        fileStatusesList.add(fileStatusList2);

        analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), new ConnectContext());
        descTable = analyzer.getDescTbl();
        tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList, 5);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        scanNode.init(analyzer);
        scanNode.finalizeStats(analyzer);

        // check
        locationsList = scanNode.getScanRangeLocations(0);
        Assert.assertEquals(4, locationsList.size());
        int group1RangesNum = 0;
        int group2RangesNum = 0;
        for (TScanRangeLocations locations : locationsList) {
            List<TBrokerRangeDesc> rangeDescs = locations.scan_range.broker_scan_range.ranges;
            String path = rangeDescs.get(0).path;
            if (path.endsWith("file1") || path.endsWith("file2") || path.endsWith("file3")) {
                Assert.assertEquals(1, rangeDescs.size());
                ++group1RangesNum;
            } else if (path.endsWith("file4") || path.endsWith("file5")) {
                Assert.assertEquals(2, rangeDescs.size());
                ++group2RangesNum;
            }
        }
        Assert.assertEquals(3, group1RangesNum);
        Assert.assertEquals(1, group2RangesNum);

        // case 4
        // 2 parquet file and one is very large
        // result: 2 ranges

        // file groups
        fileGroups = Lists.newArrayList();
        files = Lists.newArrayList("hdfs://127.0.0.1:9001/file1", "hdfs://127.0.0.1:9001/file2");
        desc = new DataDescription("testTable", null, files, columnNames, null, null, "parquet", false, null);
        brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", "\t");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        Deencapsulation.setField(brokerFileGroup, "fileFormat", "parquet");
        fileGroups.add(brokerFileGroup);

        // file status
        fileStatusesList = Lists.newArrayList();
        fileStatusList = Lists.newArrayList();
        fileStatusList.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file1", false, 268435456000L, false));
        fileStatusList.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file2", false, 10, false));
        fileStatusesList.add(fileStatusList);

        analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), new ConnectContext());
        descTable = analyzer.getDescTbl();
        tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList, 2);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        scanNode.init(analyzer);
        scanNode.finalizeStats(analyzer);

        // check
        locationsList = scanNode.getScanRangeLocations(0);
        System.out.println(locationsList);
        Assert.assertEquals(2, locationsList.size());

        // case 5
        // 1 file which size is 0
        // result: 1 range

        // file groups
        fileGroups = Lists.newArrayList();
        files = Lists.newArrayList("hdfs://127.0.0.1:9001/file1");
        desc = new DataDescription("testTable", null, files, columnNames, null, null, "parquet", false, null);
        brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", "\t");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        Deencapsulation.setField(brokerFileGroup, "fileFormat", "parquet");
        fileGroups.add(brokerFileGroup);

        // file status
        fileStatusesList = Lists.newArrayList();
        fileStatusList = Lists.newArrayList();
        fileStatusList.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file1", false, 0, false));
        fileStatusesList.add(fileStatusList);

        analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), new ConnectContext());
        descTable = analyzer.getDescTbl();
        tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList, 1);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        scanNode.init(analyzer);
        scanNode.finalizeStats(analyzer);

        // check
        locationsList = scanNode.getScanRangeLocations(0);
        System.out.println(locationsList);
        Assert.assertEquals(1, locationsList.size());
        List<TBrokerRangeDesc> rangeDescs = locationsList.get(0).scan_range.broker_scan_range.ranges;
        Assert.assertEquals(1, rangeDescs.size());
        Assert.assertEquals(0, rangeDescs.get(0).size);

        // case 5
        // 1 file which size is 0 in json format
        // result: 1 range
        // file groups

        fileGroups = Lists.newArrayList();
        files = Lists.newArrayList("hdfs://127.0.0.1:9001/file1");
        desc = new DataDescription("testTable", null, files, columnNames, null, null, "json", false, null);
        brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", "\t");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        Deencapsulation.setField(brokerFileGroup, "fileFormat", "parquet");
        fileGroups.add(brokerFileGroup);

        // file status
        fileStatusesList = Lists.newArrayList();
        fileStatusList = Lists.newArrayList();
        fileStatusList.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file1", false, 0, false));
        fileStatusesList.add(fileStatusList);

        analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), new ConnectContext());
        descTable = analyzer.getDescTbl();
        tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList, 1);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        scanNode.init(analyzer);
        scanNode.finalizeStats(analyzer);

        // check
        locationsList = scanNode.getScanRangeLocations(0);
        System.out.println(locationsList);
        Assert.assertEquals(1, locationsList.size());
        rangeDescs = locationsList.get(0).scan_range.broker_scan_range.ranges;
        Assert.assertEquals(1, rangeDescs.size());
        Assert.assertEquals(0, rangeDescs.get(0).size);
    }
}