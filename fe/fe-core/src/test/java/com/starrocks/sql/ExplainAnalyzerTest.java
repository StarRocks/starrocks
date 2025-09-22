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

import com.starrocks.common.util.Counter;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.ProfilingExecPlan;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.ScanNode;
import com.starrocks.thrift.TUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExplainAnalyzerTest {
    private RuntimeProfile mockProfile;
    private ProfilingExecPlan mockPlan;

    @BeforeEach
    public void setUp() throws NoSuchFieldException, IllegalAccessException {
        // Create mock profile structure
        mockProfile = new RuntimeProfile("Query");

        // Create Summary profile
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, "test-query-id");
        summaryProfile.addInfoString(ProfileManager.QUERY_STATE, "Finished");
        summaryProfile.addInfoString("Query State", "Finished");
        summaryProfile.addInfoString("NonDefaultSessionVariables", "");
        summaryProfile.addCounter(ProfileManager.PROFILE_COLLECT_TIME, "Summary",
                new Counter(TUnit.TIME_NS, null, 100000000));
        mockProfile.addChild(summaryProfile);

        // Create Planner profile
        RuntimeProfile plannerProfile = new RuntimeProfile("Planner");
        mockProfile.addChild(plannerProfile);

        // Create Execution profile
        RuntimeProfile executionProfile = new RuntimeProfile("Execution");
        executionProfile.addCounter("FrontendProfileMergeTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 1000));
        executionProfile.addCounter("QueryPeakMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 1024 * 1024));
        executionProfile.addCounter("QueryAllocatedMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 512 * 1024));
        executionProfile.addCounter("QueryCumulativeOperatorTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 1000000));
        executionProfile.addCounter("QueryCumulativeScanTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 500000));
        executionProfile.addCounter("QueryCumulativeNetworkTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 100000));
        executionProfile.addCounter("QueryPeakScheduleTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 50000));
        mockProfile.addChild(executionProfile);

        // Create Fragment profile
        RuntimeProfile fragmentProfile = new RuntimeProfile("Fragment 0");
        fragmentProfile.addCounter("BackendNum", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 1));
        fragmentProfile.addCounter("InstancePeakMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 1024 * 1024));
        fragmentProfile.addCounter("InstanceAllocatedMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 512 * 1024));
        fragmentProfile.addCounter("FragmentInstancePrepareTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 1000));
        executionProfile.addChild(fragmentProfile);

        // Create OLAP_SCAN operator profile (id must match plan_node_id pattern)
        RuntimeProfile olapScanProfile = new RuntimeProfile("OLAP_SCAN (plan_node_id=0)");
        olapScanProfile.addCounter("TotalTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 228000000));
        olapScanProfile.addCounter("CPUTime", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 31000000));
        olapScanProfile.addCounter("ScanTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 197000000));
        olapScanProfile.addCounter("OutputRows", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 8553457));

        // Create CommonMetrics profile that ExplainAnalyzer expects
        RuntimeProfile commonMetrics = new RuntimeProfile("CommonMetrics");
        commonMetrics.addCounter("OperatorTotalTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 228000000));
        commonMetrics.addCounter("PullRowNum", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 8553457));
        commonMetrics.addCounter("OperatorPeakMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 1024 * 1024));
        commonMetrics.addCounter("OperatorAllocatedMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 512 * 1024));
        olapScanProfile.addChild(commonMetrics);

        // Create UniqueMetrics profile with grouped metrics
        RuntimeProfile uniqueMetrics = new RuntimeProfile("UniqueMetrics");

        // Scan Filters
        uniqueMetrics.addCounter("BitmapIndexFilter", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 0));
        uniqueMetrics.addCounter("BitmapIndexFilterRows", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 0));
        uniqueMetrics.addCounter("ZoneMapFilter", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 28208));
        uniqueMetrics.addCounter("ZoneMapFilterRows", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 37558774));
        uniqueMetrics.addCounter("PredFilter", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 184998));
        uniqueMetrics.addCounter("PredFilterRows", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 45617439));
        uniqueMetrics.addCounter("BloomFilterFilter", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 2340));
        uniqueMetrics.addCounter("BloomFilterFilterRows", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 0));
        uniqueMetrics.addCounter("ShortKeyFilter", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 3916));
        uniqueMetrics.addCounter("ShortKeyFilterRows", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, -51182470));

        // Row Processing
        uniqueMetrics.addCounter("RawRowsRead", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 54170896));
        uniqueMetrics.addCounter("RowsRead", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 8553457));
        uniqueMetrics.addCounter("RemainingRowsAfterShortKeyFilter", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 153182454));
        uniqueMetrics.addCounter("DictDecode", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 479337));
        uniqueMetrics.addCounter("DictDecodeCount", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 8553457));
        uniqueMetrics.addCounter("ChunkCopy", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 40600));

        // I/O Metrics
        uniqueMetrics.addCounter("IOTime", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 16403));
        uniqueMetrics.addCounter("BytesRead", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 37 * 1024 * 1024));
        uniqueMetrics.addCounter("CompressedBytesRead", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 37 * 1024 * 1024));
        uniqueMetrics.addCounter("UncompressedBytesRead", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 308 * 1024));
        uniqueMetrics.addCounter("ReadPagesNum", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 13475));
        uniqueMetrics.addCounter("CachedPagesNum", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 12781));
        uniqueMetrics.addCounter("BlockFetch", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 5603000));
        uniqueMetrics.addCounter("BlockFetchCount", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 13480));
        uniqueMetrics.addCounter("BlockSeek", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 182919));
        uniqueMetrics.addCounter("BlockSeekCount", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 10847));
        uniqueMetrics.addCounter("DecompressTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 19813));

        // Segment Processing
        uniqueMetrics.addCounter("TabletCount", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 13));
        uniqueMetrics.addCounter("SegmentsReadCount", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 1921));
        uniqueMetrics.addCounter("RowsetsReadCount", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 627));
        uniqueMetrics.addCounter("TotalColumnsDataPageCount", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 187867));
        uniqueMetrics.addCounter("ColumnIteratorInit", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 967830));
        uniqueMetrics.addCounter("BitmapIndexIteratorInit", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 17860));
        uniqueMetrics.addCounter("FlatJsonInit", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 2412));
        uniqueMetrics.addCounter("FlatJsonMerge", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 69318000));

        // Task Management
        uniqueMetrics.addCounter("IOTaskExecTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 11372000));
        uniqueMetrics.addCounter("IOTaskWaitTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 101996));
        uniqueMetrics.addCounter("SubmitTaskCount", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 527));
        uniqueMetrics.addCounter("SubmitTaskTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 137910));
        uniqueMetrics.addCounter("PrepareChunkSourceTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 6270000));
        uniqueMetrics.addCounter("MorselsCount", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 527));
        uniqueMetrics.addCounter("PeakIOTasks", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 3));
        uniqueMetrics.addCounter("PeakScanTaskQueueSize", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 8));

        // Memory Usage
        uniqueMetrics.addCounter("PeakChunkBufferMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 12 * 1024 * 1024));
        uniqueMetrics.addCounter("PeakChunkBufferSize", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 42));
        uniqueMetrics.addCounter("ChunkBufferCapacity", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 1024));
        uniqueMetrics.addCounter("DefaultChunkBufferCapacity", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 1024));

        // Other Metrics
        uniqueMetrics.addCounter("CreateSegmentIter", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 102503));
        uniqueMetrics.addCounter("GetDelVec", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 0));
        uniqueMetrics.addCounter("GetDeltaColumnGroup", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 13908));
        uniqueMetrics.addCounter("GetRowsets", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 10708));
        uniqueMetrics.addCounter("ReadPKIndex", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 0));
        uniqueMetrics.addCounter("GetVectorRowRangesTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 0));
        uniqueMetrics.addCounter("ProcessVectorDistanceAndIdTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 0));
        uniqueMetrics.addCounter("VectorSearchTime", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 0));
        uniqueMetrics.addCounter("PushdownAccessPaths", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 0));
        uniqueMetrics.addCounter("PushdownPredicates", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 3));

        olapScanProfile.addChild(uniqueMetrics);

        // Create pipeline profile structure that ProfileNodeParser expects
        RuntimeProfile pipelineProfile = new RuntimeProfile("Pipeline 0");
        pipelineProfile.addChild(olapScanProfile);
        fragmentProfile.addChild(pipelineProfile);

        // Create mock plan and seed minimal topology so ExplainAnalyzer can bind nodes
        mockPlan = new ProfilingExecPlan();
        Field level = ProfilingExecPlan.class.getDeclaredField("profileLevel");
        level.setAccessible(true);
        level.setInt(mockPlan, 1);

        Field fragmentsField = ProfilingExecPlan.class.getDeclaredField("fragments");
        fragmentsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<ProfilingExecPlan.ProfilingFragment> fragments =
                (List<ProfilingExecPlan.ProfilingFragment>) fragmentsField.get(mockPlan);
        ProfilingExecPlan.ProfilingElement root =
                new ProfilingExecPlan.ProfilingElement(0, ScanNode.class);
        ProfilingExecPlan.ProfilingElement sink =
                new ProfilingExecPlan.ProfilingElement(-1, ResultSink.class);
        fragments.add(new ProfilingExecPlan.ProfilingFragment(sink, root));
    }

    @AfterEach
    public void tearDown() {
        mockProfile = null;
        mockPlan = null;
    }

    @Test
    public void testScanOperator() {
        String result = ExplainAnalyzer.analyze(mockPlan, mockProfile, List.of(0), false);

        assertTrue(result.contains("ScanFilters"), result);
        assertTrue(result.contains("RowProcessing"), result);
        assertTrue(result.contains("IOMetrics"), result);
        assertTrue(result.contains("SegmentProcessing"), result);
        assertTrue(result.contains("IOTask"), result);
        assertTrue(result.contains("IOBuffer"), result);
        assertTrue(result.contains("IOBuffer"), result);
        assertTrue(result.contains("Others"), result);

        assertTrue(result.contains("ZoneMapFilter: "), result);
        assertTrue(result.contains("PredFilter: "), result);
        assertTrue(result.contains("ShortKeyFilter:"), result);
    }

}
