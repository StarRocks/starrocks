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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.common.Config;
import com.starrocks.common.CsvFormat;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.CsvSplitFinder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.ast.DataDescription;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TBrokerRangeDesc;
import com.starrocks.thrift.TBrokerScanRange;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
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

    @BeforeEach
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
            throws StarRocksException {
        // table schema
        List<Column> columns = Lists.newArrayList();
        Column c1 = new Column("c1", IntegerType.BIGINT, true);
        columns.add(c1);
        Column c2 = new Column("c2", IntegerType.BIGINT, true);
        columns.add(c2);
        List<String> columnNames = Lists.newArrayList("c1", "c2");

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
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

        
        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        FileScanNode scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList,
                2, WarehouseManager.DEFAULT_RESOURCE);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        scanNode.init(descTable);
        scanNode.finalizeStats();

        // check
        List<TScanRangeLocations> locationsList = scanNode.getScanRangeLocations(0);
        System.out.println(locationsList);
        Assertions.assertEquals(3, locationsList.size());
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
                    Assertions.assertTrue(start == 0);
                    Assertions.assertTrue(size == 268435400);
                }
            }
        }
        Assertions.assertEquals(Sets.newHashSet(0L, 268435456L, 536870912L), file1StartOffsetResult);
        Assertions.assertEquals(Sets.newHashSet(56L, 268435456L), file1RangeSizeResult);
        Assertions.assertEquals(3, file1RangesNum);
        Assertions.assertEquals(1, file2RangesNum);

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

        
        descTable = new DescriptorTable();
        tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList, 4,
                WarehouseManager.DEFAULT_RESOURCE);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        scanNode.init(descTable);
        scanNode.finalizeStats();

        // check
        locationsList = scanNode.getScanRangeLocations(0);
        Assertions.assertEquals(3, locationsList.size());
        for (TScanRangeLocations locations : locationsList) {
            List<TBrokerRangeDesc> rangeDescs = locations.scan_range.broker_scan_range.ranges;
            Assertions.assertTrue(rangeDescs.size() == 1 || rangeDescs.size() == 2);
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

        
        descTable = new DescriptorTable();
        tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList, 5,
                WarehouseManager.DEFAULT_RESOURCE);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        scanNode.init(descTable);
        scanNode.finalizeStats();

        // check
        locationsList = scanNode.getScanRangeLocations(0);
        Assertions.assertEquals(4, locationsList.size());
        int group1RangesNum = 0;
        int group2RangesNum = 0;
        for (TScanRangeLocations locations : locationsList) {
            List<TBrokerRangeDesc> rangeDescs = locations.scan_range.broker_scan_range.ranges;
            String path = rangeDescs.get(0).path;
            if (path.endsWith("file1") || path.endsWith("file2") || path.endsWith("file3")) {
                Assertions.assertEquals(1, rangeDescs.size());
                ++group1RangesNum;
            } else if (path.endsWith("file4") || path.endsWith("file5")) {
                Assertions.assertEquals(2, rangeDescs.size());
                ++group2RangesNum;
            }
        }
        Assertions.assertEquals(3, group1RangesNum);
        Assertions.assertEquals(1, group2RangesNum);

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

        
        descTable = new DescriptorTable();
        tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList, 2,
                WarehouseManager.DEFAULT_RESOURCE);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        scanNode.init(descTable);
        scanNode.finalizeStats();

        // check
        locationsList = scanNode.getScanRangeLocations(0);
        System.out.println(locationsList);
        Assertions.assertEquals(2, locationsList.size());

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

        
        descTable = new DescriptorTable();
        tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList, 1,
                WarehouseManager.DEFAULT_RESOURCE);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        scanNode.init(descTable);
        scanNode.finalizeStats();

        // check
        locationsList = scanNode.getScanRangeLocations(0);
        System.out.println(locationsList);
        Assertions.assertEquals(1, locationsList.size());
        List<TBrokerRangeDesc> rangeDescs = locationsList.get(0).scan_range.broker_scan_range.ranges;
        Assertions.assertEquals(1, rangeDescs.size());
        Assertions.assertEquals(0, rangeDescs.get(0).size);

        // case 5
        // 2 file groups, one is 0, one is very large in json format
        // result: 2 ranges

        fileGroups = Lists.newArrayList();
        files = Lists.newArrayList("hdfs://127.0.0.1:9001/file1");
        desc = new DataDescription("testTable", null, files, columnNames, null, null, "json", false, null);
        brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", "\t");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        Deencapsulation.setField(brokerFileGroup, "fileFormat", "json");
        fileGroups.add(brokerFileGroup);

        files2 = Lists.newArrayList("hdfs://127.0.0.1:9001/file2");
        desc2 = new DataDescription("testTable", null, files2, columnNames, null, null, "json", false, null);
        brokerFileGroup2 = new BrokerFileGroup(desc2);
        Deencapsulation.setField(brokerFileGroup2, "columnSeparator", "\t");
        Deencapsulation.setField(brokerFileGroup2, "rowDelimiter", "\n");
        Deencapsulation.setField(brokerFileGroup2, "fileFormat", "json");
        fileGroups.add(brokerFileGroup2);

        fileStatusesList = Lists.newArrayList();
        fileStatusList = Lists.newArrayList();
        fileStatusList.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file1", false, 0, false));
        fileStatusesList.add(fileStatusList);

        fileStatusList2 = Lists.newArrayList();
        fileStatusList2.add(new TBrokerFileStatus("hdfs://127.0.0.1:9001/file2", false, 1073741824, true));
        fileStatusesList.add(fileStatusList2);
        
        descTable = new DescriptorTable();
        tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList, 2,
                WarehouseManager.DEFAULT_RESOURCE);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        scanNode.init(descTable);
        scanNode.finalizeStats();

        // check
        locationsList = scanNode.getScanRangeLocations(0);
        Assertions.assertEquals(2, locationsList.size());
        for (TScanRangeLocations locations : locationsList) {
            rangeDescs = locations.scan_range.broker_scan_range.ranges;
            String path = rangeDescs.get(0).path;
            if (path.endsWith("file1")) {
                Assertions.assertEquals(1, rangeDescs.size());
                Assertions.assertEquals(0, rangeDescs.get(0).size);
            } else {
                Assertions.assertTrue(path.endsWith("file2"));
                Assertions.assertEquals(1, rangeDescs.size());
                Assertions.assertEquals(1073741824, rangeDescs.get(0).size);
            }
        }

        // case 6
        // csv file compression type
        // result: CSV_PLAIN, CSV_GZ, CSV_BZ2, CSV_LZ4, CSV_DFLATE, CSV_ZSTD

        // file groups
        fileGroups = Lists.newArrayList();
        files = Lists.newArrayList("hdfs://127.0.0.1:9001/file1", "hdfs://127.0.0.1:9001/file2.csv",
                "hdfs://127.0.0.1:9001/file3.gz", "hdfs://127.0.0.1:9001/file4.bz2", "hdfs://127.0.0.1:9001/file5.lz4",
                "hdfs://127.0.0.1:9001/file6.deflate", "hdfs://127.0.0.1:9001/file7.zst");
        desc =
                new DataDescription("testTable", null, files, columnNames, null, null, "csv", false, null);
        brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", "\t");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        Deencapsulation.setField(brokerFileGroup, "fileFormat", "csv");
        fileGroups.add(brokerFileGroup);

        // file status
        fileStatusesList = Lists.newArrayList();
        fileStatusList = Lists.newArrayList();
        for (String file : files) {
            fileStatusList.add(new TBrokerFileStatus(file, false, 1024, true));
        }
        fileStatusesList.add(fileStatusList);

        
        descTable = new DescriptorTable();
        tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList, 2,
                WarehouseManager.DEFAULT_RESOURCE);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        scanNode.init(descTable);
        scanNode.finalizeStats();

        // check
        locationsList = scanNode.getScanRangeLocations(0);
        Assertions.assertEquals(1, locationsList.size());

        Assertions.assertEquals(7, locationsList.get(0).scan_range.broker_scan_range.ranges.size());

        Assertions.assertEquals(TFileFormatType.FORMAT_CSV_PLAIN,
                locationsList.get(0).scan_range.broker_scan_range.ranges.get(0).format_type);
        Assertions.assertEquals(TFileFormatType.FORMAT_CSV_PLAIN,
                locationsList.get(0).scan_range.broker_scan_range.ranges.get(1).format_type);
        Assertions.assertEquals(TFileFormatType.FORMAT_CSV_GZ,
                locationsList.get(0).scan_range.broker_scan_range.ranges.get(2).format_type);
        Assertions.assertEquals(TFileFormatType.FORMAT_CSV_BZ2,
                locationsList.get(0).scan_range.broker_scan_range.ranges.get(3).format_type);
        Assertions.assertEquals(TFileFormatType.FORMAT_CSV_LZ4_FRAME,
                locationsList.get(0).scan_range.broker_scan_range.ranges.get(4).format_type);
        Assertions.assertEquals(TFileFormatType.FORMAT_CSV_DEFLATE,
                locationsList.get(0).scan_range.broker_scan_range.ranges.get(5).format_type);
        Assertions.assertEquals(TFileFormatType.FORMAT_CSV_ZSTD,
                locationsList.get(0).scan_range.broker_scan_range.ranges.get(6).format_type);
    }

    private BrokerFileGroup enclosedCsvFileGroup(List<String> columnNames, List<String> files) {
        DataDescription desc =
                new DataDescription("testTable", null, files, columnNames, null, null, null, false, null);
        BrokerFileGroup fileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(fileGroup, "columnSeparator", ",");
        Deencapsulation.setField(fileGroup, "rowDelimiter", "\n");
        // An enclose character means a row delimiter can sit inside a field, so an arbitrary offset
        // is no longer a safe place to start reading.
        Deencapsulation.setField(fileGroup, "csvFormat", new CsvFormat((byte) '"', (byte) 0, 0, false));
        return fileGroup;
    }

    private void expectTableSchema(SystemInfoService systemInfoService, OlapTable table, List<Column> columns) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
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
    }

    @Test
    public void testEnclosedCsvSplitsOnlyOnRecordBoundaries(@Mocked GlobalStateMgr globalStateMgr,
                                                            @Mocked SystemInfoService systemInfoService,
                                                            @Mocked CsvSplitFinder csvSplitFinder,
                                                            @Injectable Database db, @Injectable OlapTable table)
            throws StarRocksException {
        List<Column> columns = Lists.newArrayList(new Column("c1", IntegerType.BIGINT, true),
                new Column("c2", IntegerType.BIGINT, true));
        List<String> columnNames = Lists.newArrayList("c1", "c2");
        expectTableSchema(systemInfoService, table, columns);

        final long fileSize = 500000000L;
        final String path = "hdfs://127.0.0.1:9001/enclosed";

        // Record starts at an interval that lines up with nothing the planner would pick on its own,
        // so a range landing on one can only have come from these.
        List<Long> boundaries = Lists.newArrayList();
        for (long offset = 0; offset < fileSize; offset += 7000003L) {
            boundaries.add(offset);
        }
        Set<Long> boundarySet = Sets.newHashSet(boundaries);
        List<List<Long>> splitResult = Lists.newArrayList();
        splitResult.add(boundaries);

        new Expectations() {
            {
                CsvSplitFinder.allSupport((Collection<TNetworkAddress>) any);
                result = true;
                CsvSplitFinder.findSplits((TNetworkAddress) any, (TBrokerScanRange) any, anyLong);
                result = splitResult;
            }
        };

        List<BrokerFileGroup> fileGroups =
                Lists.newArrayList(enclosedCsvFileGroup(columnNames, Lists.newArrayList(path)));
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        fileStatusesList.add(Lists.newArrayList(new TBrokerFileStatus(path, false, fileSize, true)));

        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        FileScanNode scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList,
                1, WarehouseManager.DEFAULT_RESOURCE);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        scanNode.init(descTable);
        scanNode.finalizeStats();

        List<TBrokerRangeDesc> ranges = Lists.newArrayList();
        for (TScanRangeLocations locations : scanNode.getScanRangeLocations(0)) {
            ranges.addAll(locations.scan_range.broker_scan_range.ranges);
        }
        ranges.sort((left, right) -> Long.compare(left.start_offset, right.start_offset));

        Assertions.assertTrue(ranges.size() > 1, "expected the file to be split, got " + ranges.size() + " range");

        long expectedStart = 0;
        for (int i = 0; i < ranges.size(); i++) {
            TBrokerRangeDesc range = ranges.get(i);
            Assertions.assertTrue(range.isSetRecord_aligned() && range.record_aligned,
                    "range at " + range.start_offset + " is not marked record aligned");
            // Every range starts where the previous one ended, so the file is covered once over with
            // no gap and no overlap.
            Assertions.assertEquals(expectedStart, range.start_offset);
            Assertions.assertTrue(boundarySet.contains(range.start_offset),
                    "range starts at " + range.start_offset + ", which is not a record boundary");
            expectedStart = range.start_offset + range.size;
            // Every range but the last ends on a boundary too; the last runs to end of file.
            if (i < ranges.size() - 1) {
                Assertions.assertTrue(boundarySet.contains(expectedStart),
                        "range ends at " + expectedStart + ", which is not a record boundary");
            }
        }
        Assertions.assertEquals(fileSize, expectedStart);
    }

    @Test
    public void testEnclosedCsvIsNotSplitWhenBoundariesAreUnavailable(@Mocked GlobalStateMgr globalStateMgr,
                                                                      @Mocked SystemInfoService systemInfoService,
                                                                      @Mocked CsvSplitFinder csvSplitFinder,
                                                                      @Injectable Database db,
                                                                      @Injectable OlapTable table)
            throws StarRocksException {
        List<Column> columns = Lists.newArrayList(new Column("c1", IntegerType.BIGINT, true),
                new Column("c2", IntegerType.BIGINT, true));
        List<String> columnNames = Lists.newArrayList("c1", "c2");
        expectTableSchema(systemInfoService, table, columns);

        final long fileSize = 500000000L;
        final String path = "hdfs://127.0.0.1:9001/enclosed";

        new Expectations() {
            {
                CsvSplitFinder.allSupport((Collection<TNetworkAddress>) any);
                result = true;
                CsvSplitFinder.findSplits((TNetworkAddress) any, (TBrokerScanRange) any, anyLong);
                result = new StarRocksException("no backend answered");
            }
        };

        List<BrokerFileGroup> fileGroups =
                Lists.newArrayList(enclosedCsvFileGroup(columnNames, Lists.newArrayList(path)));
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        fileStatusesList.add(Lists.newArrayList(new TBrokerFileStatus(path, false, fileSize, true)));

        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        FileScanNode scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList,
                1, WarehouseManager.DEFAULT_RESOURCE);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        scanNode.init(descTable);
        scanNode.finalizeStats();

        List<TBrokerRangeDesc> ranges = Lists.newArrayList();
        for (TScanRangeLocations locations : scanNode.getScanRangeLocations(0)) {
            ranges.addAll(locations.scan_range.broker_scan_range.ranges);
        }

        // With nowhere safe to cut, the file goes to a single range whole. Slower than splitting it,
        // but splitting it at a guessed offset is what issue #65245 is.
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals(0, ranges.get(0).start_offset);
        Assertions.assertEquals(fileSize, ranges.get(0).size);
        Assertions.assertFalse(ranges.get(0).isSetRecord_aligned() && ranges.get(0).record_aligned);
    }

    /**
     * A load retry re-runs assignBackends and can move a range to a node that joined since the
     * ranges were cut, so the check made at planning time no longer covers everyone. The ranges
     * cannot be un-aligned to make that safe - their offsets are already record boundaries, and a
     * backend applying the old rules would discard the record a range starts on and read past its
     * end - so they must stay where they are and the load task retries.
     */
    @Test
    public void testRecordAlignedRangesAreNotMovedToUncheckedBackends(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked SystemInfoService systemInfoService,
            @Mocked CsvSplitFinder csvSplitFinder, @Injectable Database db, @Injectable OlapTable table)
            throws StarRocksException {
        List<Column> columns = Lists.newArrayList(new Column("c1", IntegerType.BIGINT, true),
                new Column("c2", IntegerType.BIGINT, true));
        List<String> columnNames = Lists.newArrayList("c1", "c2");
        expectTableSchema(systemInfoService, table, columns);

        final long fileSize = 500000000L;
        final String path = "hdfs://127.0.0.1:9001/enclosed";

        List<Long> boundaries = Lists.newArrayList();
        for (long offset = 0; offset < fileSize; offset += 7000003L) {
            boundaries.add(offset);
        }
        List<List<Long>> splitResult = Lists.newArrayList();
        splitResult.add(boundaries);

        new Expectations() {
            {
                // True while the ranges are being cut, false when the retry asks again.
                CsvSplitFinder.allSupport((Collection<TNetworkAddress>) any);
                returns(true, false);
                CsvSplitFinder.findSplits((TNetworkAddress) any, (TBrokerScanRange) any, anyLong);
                result = splitResult;
            }
        };

        List<BrokerFileGroup> fileGroups =
                Lists.newArrayList(enclosedCsvFileGroup(columnNames, Lists.newArrayList(path)));
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        fileStatusesList.add(Lists.newArrayList(new TBrokerFileStatus(path, false, fileSize, true)));

        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        FileScanNode scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList,
                1, WarehouseManager.DEFAULT_RESOURCE);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        scanNode.init(descTable);
        scanNode.finalizeStats();

        List<TBrokerRangeDesc> before = Lists.newArrayList();
        for (TScanRangeLocations locations : scanNode.getScanRangeLocations(0)) {
            before.addAll(locations.scan_range.broker_scan_range.ranges);
        }
        Assertions.assertTrue(before.size() > 1, "expected the file to be split");

        scanNode.updateScanRangeLocations();

        List<TBrokerRangeDesc> after = Lists.newArrayList();
        for (TScanRangeLocations locations : scanNode.getScanRangeLocations(0)) {
            after.addAll(locations.scan_range.broker_scan_range.ranges);
        }

        // Still the same ranges, and still aligned. Clearing the flag would be the tempting fix and
        // is precisely the one that reintroduces issue #65245.
        Assertions.assertEquals(before.size(), after.size());
        for (int i = 0; i < after.size(); i++) {
            Assertions.assertTrue(after.get(i).isSetRecord_aligned() && after.get(i).record_aligned,
                    "range at " + after.get(i).start_offset + " lost its record aligned flag");
            Assertions.assertEquals(before.get(i).start_offset, after.get(i).start_offset);
            Assertions.assertEquals(before.get(i).size, after.get(i).size);
        }
    }

    // A backend from before this RPC drops the record_aligned field it does not know, so it must
    // not be handed a range that relies on it.
    @Test
    public void testEnclosedCsvIsNotSplitWhenABackendCannotReadAlignedRanges(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked SystemInfoService systemInfoService,
            @Mocked CsvSplitFinder csvSplitFinder, @Injectable Database db, @Injectable OlapTable table)
            throws StarRocksException {
        List<Column> columns = Lists.newArrayList(new Column("c1", IntegerType.BIGINT, true),
                new Column("c2", IntegerType.BIGINT, true));
        List<String> columnNames = Lists.newArrayList("c1", "c2");
        expectTableSchema(systemInfoService, table, columns);

        final long fileSize = 500000000L;
        final String path = "hdfs://127.0.0.1:9001/enclosed";

        new Expectations() {
            {
                CsvSplitFinder.allSupport((Collection<TNetworkAddress>) any);
                result = false;
            }
        };

        List<BrokerFileGroup> fileGroups =
                Lists.newArrayList(enclosedCsvFileGroup(columnNames, Lists.newArrayList(path)));
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        fileStatusesList.add(Lists.newArrayList(new TBrokerFileStatus(path, false, fileSize, true)));

        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        FileScanNode scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList,
                1, WarehouseManager.DEFAULT_RESOURCE);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        scanNode.init(descTable);
        scanNode.finalizeStats();

        List<TBrokerRangeDesc> ranges = Lists.newArrayList();
        for (TScanRangeLocations locations : scanNode.getScanRangeLocations(0)) {
            ranges.addAll(locations.scan_range.broker_scan_range.ranges);
        }

        // One node that would ignore the record_aligned flag is enough to make aligned ranges
        // unsafe for the whole file: it would discard the record its range starts on and read past
        // the end, which is issue #65245 again by another route. So the file is loaded whole.
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals(0, ranges.get(0).start_offset);
        Assertions.assertEquals(fileSize, ranges.get(0).size);
        Assertions.assertFalse(ranges.get(0).isSetRecord_aligned() && ranges.get(0).record_aligned);
    }

    @Test
    public void testNoFilesFound() {

        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        fileStatusesList.add(Lists.newArrayList());
        FileScanNode scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode",
                fileStatusesList, 0, WarehouseManager.DEFAULT_RESOURCE);

        List<String> files = Lists.newArrayList("hdfs://127.0.0.1:9001/file1", "hdfs://127.0.0.1:9001/file2",
                "hdfs://127.0.0.1:9001/file3", "hdfs://127.0.0.1:9001/file4");
        DataDescription desc =
                new DataDescription("testTable", null, files, null, null, null, "csv", false, null);
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "filePaths", files);
        List<BrokerFileGroup> fileGroups = Lists.newArrayList(brokerFileGroup);
        scanNode.setLoadInfo(jobId, txnId, null, brokerDesc, fileGroups, true, loadParallelInstanceNum);

        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class,
                "No files were found matching the pattern(s) or path(s): " +
                        "'hdfs://127.0.0.1:9001/file1, hdfs://127.0.0.1:9001/file2, hdfs://127.0.0.1:9001/file3, ...'",
                () -> Deencapsulation.invoke(scanNode, "getFileStatusAndCalcInstance"));
    }

    @Test
    public void testNoFilesFoundOnePath() {
        
        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        fileStatusesList.add(Lists.newArrayList());
        FileScanNode scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode",
                fileStatusesList, 0, WarehouseManager.DEFAULT_RESOURCE);

        List<String> files = Lists.newArrayList("hdfs://127.0.0.1:9001/file*");
        DataDescription desc =
                new DataDescription("testTable", null, files, null, null, null, "csv", false, null);
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "filePaths", files);
        List<BrokerFileGroup> fileGroups = Lists.newArrayList(brokerFileGroup);
        scanNode.setLoadInfo(jobId, txnId, null, brokerDesc, fileGroups, true, loadParallelInstanceNum);

        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class,
                "No files were found matching the pattern(s) or path(s): 'hdfs://127.0.0.1:9001/file*'",
                () -> Deencapsulation.invoke(scanNode, "getFileStatusAndCalcInstance"));
    }

    @Test
    public void testIllegalColumnSeparator(@Mocked GlobalStateMgr globalStateMgr, @Mocked SystemInfoService systemInfoService,
                                     @Injectable Database db, @Injectable OlapTable table) {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_NOTHING;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = systemInfoService;
                systemInfoService.getIdToBackend();
                result = idToBackend;
                table.getPartitions();
                minTimes = 0;
                result = Arrays.asList(partition);
                partition.getId();
                minTimes = 0;
                result = 0;
            }
        };

        // file groups
        List<BrokerFileGroup> fileGroups = Lists.newArrayList();
        List<String> files = Lists.newArrayList("hdfs://127.0.0.1:9001/file1", "hdfs://127.0.0.1:9001/file2");
        DataDescription desc =
                new DataDescription("testTable", null, files, Lists.newArrayList("c1", "c2"),
                        null, null, null, false, null);
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator",
                "012345678901234567890123456789012345678901234567890123456789");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        fileGroups.add(brokerFileGroup);

        // file status
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        List<TBrokerFileStatus> fileStatusList = Lists.newArrayList();
        fileStatusesList.add(fileStatusList);

        
        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        FileScanNode scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList,
                2, WarehouseManager.DEFAULT_RESOURCE);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class,
                "The valid bytes length for 'column separator' is [1, 50]",
                () -> scanNode.init(descTable));
    }
    @Test
    public void testIllegalRowDelimiter(@Mocked GlobalStateMgr globalStateMgr, @Mocked SystemInfoService systemInfoService,
                                           @Injectable Database db, @Injectable OlapTable table) {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_NOTHING;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = systemInfoService;
                systemInfoService.getIdToBackend();
                result = idToBackend;
                table.getPartitions();
                minTimes = 0;
                result = Arrays.asList(partition);
                partition.getId();
                minTimes = 0;
                result = 0;
            }
        };

        // file groups
        List<BrokerFileGroup> fileGroups = Lists.newArrayList();
        List<String> files = Lists.newArrayList("hdfs://127.0.0.1:9001/file1", "hdfs://127.0.0.1:9001/file2");
        DataDescription desc =
                new DataDescription("testTable", null, files, Lists.newArrayList("c1", "c2"),
                        null, null, null, false, null);
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter",
                "012345678901234567890123456789012345678901234567890123456789");
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", "\t");
        fileGroups.add(brokerFileGroup);

        // file status
        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        List<TBrokerFileStatus> fileStatusList = Lists.newArrayList();
        fileStatusesList.add(fileStatusList);

        
        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        FileScanNode scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode", fileStatusesList,
                2, WarehouseManager.DEFAULT_RESOURCE);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);
        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class,
                "The valid bytes length for 'row delimiter' is [1, 50]",
                () -> scanNode.init(descTable));
    }

    @Test
    public void testEnvelopeDebeziumRequiresPrimaryKeyTable(@Injectable OlapTable table)
            throws StarRocksException {
        new Expectations() {{
            table.getKeysType();
            result = KeysType.DUP_KEYS;
        }};

        List<BrokerFileGroup> fileGroups = Lists.newArrayList();
        List<String> files = Lists.newArrayList("hdfs://127.0.0.1:9001/file1");
        DataDescription desc = new DataDescription("testTable", null, files,
                Lists.newArrayList("c1"), null, null, "json", false, null);
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        Deencapsulation.setField(brokerFileGroup, "columnSeparator", "\t");
        Deencapsulation.setField(brokerFileGroup, "rowDelimiter", "\n");
        fileGroups.add(brokerFileGroup);

        List<List<TBrokerFileStatus>> fileStatusesList = Lists.newArrayList();
        fileStatusesList.add(Lists.newArrayList());

        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        FileScanNode scanNode = new FileScanNode(new PlanNodeId(0), tupleDesc, "FileScanNode",
                fileStatusesList, 1, WarehouseManager.DEFAULT_RESOURCE);
        scanNode.setLoadInfo(jobId, txnId, table, brokerDesc, fileGroups, true, loadParallelInstanceNum);

        LoadJob.JSONOptions jsonOptions = new LoadJob.JSONOptions();
        jsonOptions.envelope = LoadStmt.ENVELOPE_DEBEZIUM;
        scanNode.setJSONOptions(jsonOptions);

        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class,
                "envelope=debezium is only supported on PRIMARY KEY tables",
                scanNode::finalizeStats);
    }

    @Test
    public void testInferCompressionByName() {
        // Test GZIP compression
        Assertions.assertEquals(TCompressionType.GZIP,
                FileScanNode.inferCompressionByName("file.json.gz"));
        Assertions.assertEquals(TCompressionType.GZIP,
                FileScanNode.inferCompressionByName("file.json.gzip"));
        Assertions.assertEquals(TCompressionType.GZIP,
                FileScanNode.inferCompressionByName("FILE.JSON.GZ"));
        Assertions.assertEquals(TCompressionType.GZIP,
                FileScanNode.inferCompressionByName("FILE.JSON.GZIP"));

        // Test BZIP2 compression
        Assertions.assertEquals(TCompressionType.BZIP2,
                FileScanNode.inferCompressionByName("file.json.bz2"));
        Assertions.assertEquals(TCompressionType.BZIP2,
                FileScanNode.inferCompressionByName("FILE.JSON.BZ2"));

        // Test ZSTD compression
        Assertions.assertEquals(TCompressionType.ZSTD,
                FileScanNode.inferCompressionByName("file.json.zst"));
        Assertions.assertEquals(TCompressionType.ZSTD,
                FileScanNode.inferCompressionByName("file.json.zstd"));
        Assertions.assertEquals(TCompressionType.ZSTD,
                FileScanNode.inferCompressionByName("FILE.JSON.ZST"));
        Assertions.assertEquals(TCompressionType.ZSTD,
                FileScanNode.inferCompressionByName("FILE.JSON.ZSTD"));

        // Test LZ4 compression
        Assertions.assertEquals(TCompressionType.LZ4_FRAME,
                FileScanNode.inferCompressionByName("file.json.lz4"));
        Assertions.assertEquals(TCompressionType.LZ4_FRAME,
                FileScanNode.inferCompressionByName("FILE.JSON.LZ4"));

        // Test DEFLATE compression
        Assertions.assertEquals(TCompressionType.DEFLATE,
                FileScanNode.inferCompressionByName("file.json.deflate"));
        Assertions.assertEquals(TCompressionType.DEFLATE,
                FileScanNode.inferCompressionByName("FILE.JSON.DEFLATE"));

        // Test SNAPPY compression
        Assertions.assertEquals(TCompressionType.SNAPPY,
                FileScanNode.inferCompressionByName("file.json.snappy"));
        Assertions.assertEquals(TCompressionType.SNAPPY,
                FileScanNode.inferCompressionByName("FILE.JSON.SNAPPY"));

        // Test files with no compression (should return null)
        Assertions.assertNull(FileScanNode.inferCompressionByName("file.json"));
        Assertions.assertNull(FileScanNode.inferCompressionByName("file.txt"));
        Assertions.assertNull(FileScanNode.inferCompressionByName("file.parquet"));
        Assertions.assertNull(FileScanNode.inferCompressionByName(""));
        Assertions.assertNull(FileScanNode.inferCompressionByName(null));

        // Test files with unsupported compression extensions
        Assertions.assertNull(FileScanNode.inferCompressionByName("file.json.xz"));
        Assertions.assertNull(FileScanNode.inferCompressionByName("file.json.lzo"));
        Assertions.assertNull(FileScanNode.inferCompressionByName("file.json.brotli"));

        // Test edge cases
        Assertions.assertNull(FileScanNode.inferCompressionByName("gz"));
        Assertions.assertNull(FileScanNode.inferCompressionByName("file.gz.txt"));
        Assertions.assertNull(FileScanNode.inferCompressionByName("file.gz.json"));
    }

}
