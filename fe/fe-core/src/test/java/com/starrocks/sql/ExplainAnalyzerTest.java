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

package com.starrocks.sql;

// import com.starrocks.common.util.RuntimeProfile;
// import com.starrocks.common.util.ProfileManager;
// import com.starrocks.common.util.ProfilingExecPlan;
// import com.starrocks.common.util.Counter;
// import com.starrocks.thrift.TUnit;
// import org.junit.jupiter.api.Test;
// import org.junit.jupiter.api.BeforeEach;
// import org.junit.jupiter.api.AfterEach;
// import static org.junit.jupiter.api.Assertions.*;

// import java.util.HashMap;
// import java.util.Map;

public class ExplainAnalyzerTest {
    //     private RuntimeProfile mockProfile;
    //     private ProfilingExecPlan mockPlan;

    //     @BeforeEach
    //     public void setUp() {
    //         // Create mock profile structure
    //         mockProfile = new RuntimeProfile("Query");

    //         // Create Summary profile
    //         RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
    //         summaryProfile.addInfoString(ProfileManager.QUERY_ID, "test-query-id");
    //         summaryProfile.addInfoString(ProfileManager.QUERY_STATE, "Finished");
    //         summaryProfile.addInfoString("Query State", "Finished");
    //         summaryProfile.addCounter(ProfileManager.PROFILE_COLLECT_TIME, new Counter(TUnit.TIME_MS, null, 100));
    //         mockProfile.addChild(summaryProfile);

    //         // Create Planner profile
    //         RuntimeProfile plannerProfile = new RuntimeProfile("Planner");
    //         mockProfile.addChild(plannerProfile);

    //         // Create Execution profile
    //         RuntimeProfile executionProfile = new RuntimeProfile("Execution");
    //         executionProfile.addCounter("FrontendProfileMergeTime", new Counter(TUnit.TIME_NS, null, 1000));
    //         executionProfile.addCounter("QueryPeakMemoryUsage", new Counter(TUnit.BYTES, null, 1024 * 1024));
    //         executionProfile.addCounter("QueryAllocatedMemoryUsage", new Counter(TUnit.BYTES, null, 512 * 1024));
    //         mockProfile.addChild(executionProfile);

    //         // Create Fragment profile
    //         RuntimeProfile fragmentProfile = new RuntimeProfile("Fragment 0");
    //         fragmentProfile.addCounter("BackendNum", new Counter(TUnit.UNIT, null, 1));
    //         fragmentProfile.addCounter("InstancePeakMemoryUsage", new Counter(TUnit.BYTES, null, 1024 * 1024));
    //         fragmentProfile.addCounter("InstanceAllocatedMemoryUsage", new Counter(TUnit.BYTES, null, 512 * 1024));
    //         fragmentProfile.addCounter("FragmentInstancePrepareTime", new Counter(TUnit.TIME_NS, null, 1000));
    //         executionProfile.addChild(fragmentProfile);

    //         // Create OLAP_SCAN operator profile
    //         RuntimeProfile olapScanProfile = new RuntimeProfile("OLAP_SCAN (id=0)");
    //         olapScanProfile.addCounter("TotalTime", new Counter(TUnit.TIME_MS, null, 228));
    //         olapScanProfile.addCounter("CPUTime", new Counter(TUnit.TIME_MS, null, 31));
    //         olapScanProfile.addCounter("ScanTime", new Counter(TUnit.TIME_MS, null, 197));
    //         olapScanProfile.addCounter("OutputRows", new Counter(TUnit.UNIT, null, 8553457));

    //         // Create UniqueMetrics profile with grouped metrics
    //         RuntimeProfile uniqueMetrics = new RuntimeProfile("UniqueMetrics");

    //         // Scan Filters
    //         uniqueMetrics.addCounter("BitmapIndexFilter", new Counter(TUnit.TIME_NS, null, 0));
    //         uniqueMetrics.addCounter("BitmapIndexFilterRows", new Counter(TUnit.UNIT, null, 0));
    //         uniqueMetrics.addCounter("ZoneMapFilter", new Counter(TUnit.TIME_NS, null, 28208));
    //         uniqueMetrics.addCounter("ZoneMapFilterRows", new Counter(TUnit.UNIT, null, 37558774));
    //         uniqueMetrics.addCounter("PredFilter", new Counter(TUnit.TIME_NS, null, 184998));
    //         uniqueMetrics.addCounter("PredFilterRows", new Counter(TUnit.UNIT, null, 45617439));
    //         uniqueMetrics.addCounter("BloomFilterFilter", new Counter(TUnit.TIME_NS, null, 2340));
    //         uniqueMetrics.addCounter("BloomFilterFilterRows", new Counter(TUnit.UNIT, null, 0));
    //         uniqueMetrics.addCounter("ShortKeyFilter", new Counter(TUnit.TIME_NS, null, 3916));
    //         uniqueMetrics.addCounter("ShortKeyFilterRows", new Counter(TUnit.UNIT, null, -51182470));

    //         // Row Processing
    //         uniqueMetrics.addCounter("RawRowsRead", new Counter(TUnit.UNIT, null, 54170896));
    //         uniqueMetrics.addCounter("RowsRead", new Counter(TUnit.UNIT, null, 8553457));
    //         uniqueMetrics.addCounter("RemainingRowsAfterShortKeyFilter", new Counter(TUnit.UNIT, null, 153182454));
    //         uniqueMetrics.addCounter("DictDecode", new Counter(TUnit.TIME_NS, null, 479337));
    //         uniqueMetrics.addCounter("DictDecodeCount", new Counter(TUnit.UNIT, null, 8553457));
    //         uniqueMetrics.addCounter("ChunkCopy", new Counter(TUnit.TIME_NS, null, 40600));

    //         // I/O Metrics
    //         uniqueMetrics.addCounter("IOTime", new Counter(TUnit.TIME_NS, null, 16403));
    //         uniqueMetrics.addCounter("BytesRead", new Counter(TUnit.BYTES, null, 37 * 1024 * 1024));
    //         uniqueMetrics.addCounter("CompressedBytesRead", new Counter(TUnit.BYTES, null, 89 * 1024));
    //         uniqueMetrics.addCounter("UncompressedBytesRead", new Counter(TUnit.BYTES, null, 308 * 1024));
    //         uniqueMetrics.addCounter("ReadPagesNum", new Counter(TUnit.UNIT, null, 13475));
    //         uniqueMetrics.addCounter("CachedPagesNum", new Counter(TUnit.UNIT, null, 12781));
    //         uniqueMetrics.addCounter("BlockFetch", new Counter(TUnit.TIME_NS, null, 5603000));
    //         uniqueMetrics.addCounter("BlockFetchCount", new Counter(TUnit.UNIT, null, 13480));
    //         uniqueMetrics.addCounter("BlockSeek", new Counter(TUnit.TIME_NS, null, 182919));
    //         uniqueMetrics.addCounter("BlockSeekCount", new Counter(TUnit.UNIT, null, 10847));
    //         uniqueMetrics.addCounter("DecompressTime", new Counter(TUnit.TIME_NS, null, 19813));

    //         // Segment Processing
    //         uniqueMetrics.addCounter("TabletCount", new Counter(TUnit.UNIT, null, 13));
    //         uniqueMetrics.addCounter("SegmentsReadCount", new Counter(TUnit.UNIT, null, 1921));
    //         uniqueMetrics.addCounter("RowsetsReadCount", new Counter(TUnit.UNIT, null, 627));
    //         uniqueMetrics.addCounter("TotalColumnsDataPageCount", new Counter(TUnit.UNIT, null, 187867));
    //         uniqueMetrics.addCounter("ColumnIteratorInit", new Counter(TUnit.TIME_NS, null, 967830));
    //         uniqueMetrics.addCounter("BitmapIndexIteratorInit", new Counter(TUnit.TIME_NS, null, 17860));
    //         uniqueMetrics.addCounter("FlatJsonInit", new Counter(TUnit.TIME_NS, null, 2412));
    //         uniqueMetrics.addCounter("FlatJsonMerge", new Counter(TUnit.TIME_NS, null, 69318000));

    //         // Task Management
    //         uniqueMetrics.addCounter("IOTaskExecTime", new Counter(TUnit.TIME_NS, null, 11372000));
    //         uniqueMetrics.addCounter("IOTaskWaitTime", new Counter(TUnit.TIME_NS, null, 101996));
    //         uniqueMetrics.addCounter("SubmitTaskCount", new Counter(TUnit.UNIT, null, 527));
    //         uniqueMetrics.addCounter("SubmitTaskTime", new Counter(TUnit.TIME_NS, null, 137910));
    //         uniqueMetrics.addCounter("PrepareChunkSourceTime", new Counter(TUnit.TIME_NS, null, 6270000));
    //         uniqueMetrics.addCounter("MorselsCount", new Counter(TUnit.UNIT, null, 527));
    //         uniqueMetrics.addCounter("PeakIOTasks", new Counter(TUnit.UNIT, null, 3));
    //         uniqueMetrics.addCounter("PeakScanTaskQueueSize", new Counter(TUnit.UNIT, null, 8));

    //         // Memory Usage
    //         uniqueMetrics.addCounter("PeakChunkBufferMemoryUsage", new Counter(TUnit.BYTES, null, 12 * 1024 * 1024));
    //         uniqueMetrics.addCounter("PeakChunkBufferSize", new Counter(TUnit.UNIT, null, 42));
    //         uniqueMetrics.addCounter("ChunkBufferCapacity", new Counter(TUnit.UNIT, null, 1024));
    //         uniqueMetrics.addCounter("DefaultChunkBufferCapacity", new Counter(TUnit.UNIT, null, 1024));

    //         // Other Metrics
    //         uniqueMetrics.addCounter("CreateSegmentIter", new Counter(TUnit.TIME_NS, null, 102503));
    //         uniqueMetrics.addCounter("GetDelVec", new Counter(TUnit.TIME_NS, null, 0));
    //         uniqueMetrics.addCounter("GetDeltaColumnGroup", new Counter(TUnit.TIME_NS, null, 13908));
    //         uniqueMetrics.addCounter("GetRowsets", new Counter(TUnit.TIME_NS, null, 10708));
    //         uniqueMetrics.addCounter("ReadPKIndex", new Counter(TUnit.TIME_NS, null, 0));
    //         uniqueMetrics.addCounter("GetVectorRowRangesTime", new Counter(TUnit.TIME_NS, null, 0));
    //         uniqueMetrics.addCounter("ProcessVectorDistanceAndIdTime", new Counter(TUnit.TIME_NS, null, 0));
    //         uniqueMetrics.addCounter("VectorSearchTime", new Counter(TUnit.TIME_NS, null, 0));
    //         uniqueMetrics.addCounter("PushdownAccessPaths", new Counter(TUnit.UNIT, null, 0));
    //         uniqueMetrics.addCounter("PushdownPredicates", new Counter(TUnit.UNIT, null, 3));

    //         olapScanProfile.addChild(uniqueMetrics);
    //         fragmentProfile.addChild(olapScanProfile);

    //         // Create mock plan
    //         mockPlan = new ProfilingExecPlan();
    //     }

    //     @AfterEach
    //     public void tearDown() {
    //         mockProfile = null;
    //         mockPlan = null;
    //     }

    //     @Test
    //     public void testGroupedMetricsFormat() {
    //         String result = ExplainAnalyzer.analyze(mockPlan, mockProfile, null, false);

    //         // Verify that the output contains the new grouped format
    //         assertTrue(result.contains("# ===== SCAN FILTERS & ROW PROCESSING ====="),
    //                 "Should contain SCAN FILTERS & ROW PROCESSING section");
    //         assertTrue(result.contains("ScanFilters:"),
    //                 "Should contain ScanFilters section");
    //         assertTrue(result.contains("RowProcessing:"),
    //                 "Should contain RowProcessing section");

    //         assertTrue(result.contains("# ===== I/O METRICS ====="),
    //                 "Should contain I/O METRICS section");
    //         assertTrue(result.contains("IOMetrics:"),
    //                 "Should contain IOMetrics section");

    //         assertTrue(result.contains("# ===== SEGMENT PROCESSING ====="),
    //                 "Should contain SEGMENT PROCESSING section");
    //         assertTrue(result.contains("SegmentProcessing:"),
    //                 "Should contain SegmentProcessing section");

    //         assertTrue(result.contains("# ===== TASK MANAGEMENT ====="),
    //                 "Should contain TASK MANAGEMENT section");
    //         assertTrue(result.contains("TaskManagement:"),
    //                 "Should contain TaskManagement section");

    //         assertTrue(result.contains("# ===== MEMORY USAGE ====="),
    //                 "Should contain MEMORY USAGE section");
    //         assertTrue(result.contains("MemoryUsage:"),
    //                 "Should contain MemoryUsage section");

    //         assertTrue(result.contains("# ===== OTHER METRICS ====="),
    //                 "Should contain OTHER METRICS section");
    //         assertTrue(result.contains("OtherMetrics:"),
    //                 "Should contain OtherMetrics section");
    //     }

    //     @Test
    //     public void testScanFiltersFormat() {
    //         String result = ExplainAnalyzer.analyze(mockPlan, mockProfile, null, false);

    //         // Verify ScanFilters format: Rows: X, Time: Y
    //         assertTrue(result.contains("ZoneMapFilter: Rows: 37.559M, Time: 28.208us"),
    //                 "ZoneMapFilter should be formatted as Rows: X, Time: Y");
    //         assertTrue(result.contains("PredFilter: Rows: 45.617M, Time: 184.998us"),
    //                 "PredFilter should be formatted as Rows: X, Time: Y");
    //         assertTrue(result.contains("ShortKeyFilter: Rows: -51.182M, Time: 3.916us"),
    //                 "ShortKeyFilter should be formatted as Rows: X, Time: Y");
    //         assertTrue(result.contains("BitmapIndexFilter: Rows: 0, Time: 0ns"),
    //                 "BitmapIndexFilter should be formatted as Rows: X, Time: Y");
    //     }

    //     @Test
    //     public void testIOMetricsGrouping() {
    //         String result = ExplainAnalyzer.analyze(mockPlan, mockProfile, null, false);

    //         // Verify I/O metrics are grouped together
    //         String ioSection = extractSection(result, "IOMetrics:");
    //         assertTrue(ioSection.contains("IOTime:"), "IOTime should be in IOMetrics section");
    //         assertTrue(ioSection.contains("BytesRead:"), "BytesRead should be in IOMetrics section");
    //         assertTrue(ioSection.contains("CompressedBytesRead:"), "CompressedBytesRead should be in IOMetrics section");
    //         assertTrue(ioSection.contains("UncompressedBytesRead:"), "UncompressedBytesRead should be in IOMetrics section");
    //         assertTrue(ioSection.contains("ReadPagesNum:"), "ReadPagesNum should be in IOMetrics section");
    //         assertTrue(ioSection.contains("CachedPagesNum:"), "CachedPagesNum should be in IOMetrics section");
    //         assertTrue(ioSection.contains("BlockFetch:"), "BlockFetch should be in IOMetrics section");
    //         assertTrue(ioSection.contains("BlockSeek:"), "BlockSeek should be in IOMetrics section");
    //         assertTrue(ioSection.contains("DecompressTime:"), "DecompressTime should be in IOMetrics section");
    //     }

    //     @Test
    //     public void testSegmentProcessingGrouping() {
    //         String result = ExplainAnalyzer.analyze(mockPlan, mockProfile, null, false);

    //         // Verify Segment Processing metrics are grouped together
    //         String segmentSection = extractSection(result, "SegmentProcessing:");
    //         assertTrue(segmentSection.contains("TabletCount:"), "TabletCount should be in SegmentProcessing section");
    //         assertTrue(segmentSection.contains("SegmentsReadCount:"), "SegmentsReadCount should be in SegmentProcessing section");
    //         assertTrue(segmentSection.contains("RowsetsReadCount:"), "RowsetsReadCount should be in SegmentProcessing section");
    //         assertTrue(segmentSection.contains("TotalColumnsDataPageCount:"), "TotalColumnsDataPageCount should be in SegmentProcessing section");
    //         assertTrue(segmentSection.contains("ColumnIteratorInit:"), "ColumnIteratorInit should be in SegmentProcessing section");
    //         assertTrue(segmentSection.contains("BitmapIndexIteratorInit:"), "BitmapIndexIteratorInit should be in SegmentProcessing section");
    //         assertTrue(segmentSection.contains("FlatJsonInit:"), "FlatJsonInit should be in SegmentProcessing section");
    //         assertTrue(segmentSection.contains("FlatJsonMerge:"), "FlatJsonMerge should be in SegmentProcessing section");
    //     }

    //     @Test
    //     public void testTaskManagementGrouping() {
    //         String result = ExplainAnalyzer.analyze(mockPlan, mockProfile, null, false);

    //         // Verify Task Management metrics are grouped together
    //         String taskSection = extractSection(result, "TaskManagement:");
    //         assertTrue(taskSection.contains("IOTaskExecTime:"), "IOTaskExecTime should be in TaskManagement section");
    //         assertTrue(taskSection.contains("IOTaskWaitTime:"), "IOTaskWaitTime should be in TaskManagement section");
    //         assertTrue(taskSection.contains("SubmitTaskCount:"), "SubmitTaskCount should be in TaskManagement section");
    //         assertTrue(taskSection.contains("SubmitTaskTime:"), "SubmitTaskTime should be in TaskManagement section");
    //         assertTrue(taskSection.contains("PrepareChunkSourceTime:"), "PrepareChunkSourceTime should be in TaskManagement section");
    //         assertTrue(taskSection.contains("MorselsCount:"), "MorselsCount should be in TaskManagement section");
    //         assertTrue(taskSection.contains("PeakIOTasks:"), "PeakIOTasks should be in TaskManagement section");
    //         assertTrue(taskSection.contains("PeakScanTaskQueueSize:"), "PeakScanTaskQueueSize should be in TaskManagement section");
    //     }

    //     @Test
    //     public void testMemoryUsageGrouping() {
    //         String result = ExplainAnalyzer.analyze(mockPlan, mockProfile, null, false);

    //         // Verify Memory Usage metrics are grouped together
    //         String memorySection = extractSection(result, "MemoryUsage:");
    //         assertTrue(memorySection.contains("PeakChunkBufferMemoryUsage:"), "PeakChunkBufferMemoryUsage should be in MemoryUsage section");
    //         assertTrue(memorySection.contains("PeakChunkBufferSize:"), "PeakChunkBufferSize should be in MemoryUsage section");
    //         assertTrue(memorySection.contains("ChunkBufferCapacity:"), "ChunkBufferCapacity should be in MemoryUsage section");
    //         assertTrue(memorySection.contains("DefaultChunkBufferCapacity:"), "DefaultChunkBufferCapacity should be in MemoryUsage section");
    //     }

    //     @Test
    //     public void testOtherMetricsGrouping() {
    //         String result = ExplainAnalyzer.analyze(mockPlan, mockProfile, null, false);

    //         // Verify Other Metrics are grouped together
    //         String otherSection = extractSection(result, "OtherMetrics:");
    //         assertTrue(otherSection.contains("CreateSegmentIter:"), "CreateSegmentIter should be in OtherMetrics section");
    //         assertTrue(otherSection.contains("GetDelVec:"), "GetDelVec should be in OtherMetrics section");
    //         assertTrue(otherSection.contains("GetDeltaColumnGroup:"), "GetDeltaColumnGroup should be in OtherMetrics section");
    //         assertTrue(otherSection.contains("GetRowsets:"), "GetRowsets should be in OtherMetrics section");
    //         assertTrue(otherSection.contains("ReadPKIndex:"), "ReadPKIndex should be in OtherMetrics section");
    //         assertTrue(otherSection.contains("PushdownAccessPaths:"), "PushdownAccessPaths should be in OtherMetrics section");
    //         assertTrue(otherSection.contains("PushdownPredicates:"), "PushdownPredicates should be in OtherMetrics section");
    //     }

    //     private String extractSection(String result, String sectionName) {
    //         int startIndex = result.indexOf(sectionName);
    //         if (startIndex == -1) {
    //             return "";
    //         }

    //         int endIndex = result.indexOf("# ===== ", startIndex + sectionName.length());
    //         if (endIndex == -1) {
    //             endIndex = result.length();
    //         }

    //         return result.substring(startIndex, endIndex);
    //     }
}
