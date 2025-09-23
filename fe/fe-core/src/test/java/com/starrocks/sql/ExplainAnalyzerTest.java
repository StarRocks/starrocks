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
import com.starrocks.planner.AggregationNode;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SortNode;
import com.starrocks.thrift.TUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExplainAnalyzerTest {
    private RuntimeProfile mockProfile;

    @BeforeEach
    public void setUp() {
        // Only initialize basic profile structure, each test will create its own operator profiles
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
    }

    @AfterEach
    public void tearDown() {
        mockProfile = null;
    }

    @Test
    public void testScanOperator() throws NoSuchFieldException, IllegalAccessException {
        // Create OLAP_SCAN operator profile
        RuntimeProfile olapScanProfile = new RuntimeProfile("OLAP_SCAN (plan_node_id=0)");
        olapScanProfile.addCounter("TotalTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 228000000));
        olapScanProfile.addCounter("CPUTime", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 31000000));
        olapScanProfile.addCounter("ScanTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 197000000));
        olapScanProfile.addCounter("OutputRows", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 8553457));

        // Create CommonMetrics profile
        RuntimeProfile commonMetrics = new RuntimeProfile("CommonMetrics");
        commonMetrics.addCounter("OperatorTotalTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 228000000));
        commonMetrics.addCounter("PullRowNum", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 8553457));
        commonMetrics.addCounter("OperatorPeakMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 1024 * 1024));
        commonMetrics.addCounter("OperatorAllocatedMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 512 * 1024));
        olapScanProfile.addChild(commonMetrics);

        // Create UniqueMetrics profile with scan-specific metrics
        RuntimeProfile uniqueMetrics = new RuntimeProfile("UniqueMetrics");
        uniqueMetrics.addCounter("ZoneMapFilter", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 28208));
        uniqueMetrics.addCounter("ZoneMapFilterRows", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 37558774));
        uniqueMetrics.addCounter("PredFilter", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 184998));
        uniqueMetrics.addCounter("PredFilterRows", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 45617439));
        uniqueMetrics.addCounter("ShortKeyFilter", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 3916));
        uniqueMetrics.addCounter("ShortKeyFilterRows", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, -51182470));
        uniqueMetrics.addCounter("RawRowsRead", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 54170896));
        uniqueMetrics.addCounter("RowsRead", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 8553457));
        uniqueMetrics.addCounter("IOTime", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 16403));
        uniqueMetrics.addCounter("BytesRead", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 37 * 1024 * 1024));
        uniqueMetrics.addCounter("TabletCount", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 13));
        uniqueMetrics.addCounter("SegmentsReadCount", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 1921));
        uniqueMetrics.addCounter("IOTaskExecTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 11372000));
        uniqueMetrics.addCounter("PeakChunkBufferMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 12 * 1024 * 1024));
        uniqueMetrics.addCounter("Unknown", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 12 * 1024 * 1024));
        olapScanProfile.addChild(uniqueMetrics);

        // Create pipeline profile structure
        RuntimeProfile pipelineProfile = new RuntimeProfile("Pipeline 0");
        pipelineProfile.addChild(olapScanProfile);
        mockProfile.getChild("Execution").getChild("Fragment 0").addChild(pipelineProfile);

        // Create mock plan with ScanNode
        ProfilingExecPlan scanPlan = new ProfilingExecPlan();
        Field level = ProfilingExecPlan.class.getDeclaredField("profileLevel");
        level.setAccessible(true);
        level.setInt(scanPlan, 1);

        Field fragmentsField = ProfilingExecPlan.class.getDeclaredField("fragments");
        fragmentsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<ProfilingExecPlan.ProfilingFragment> fragments =
                (List<ProfilingExecPlan.ProfilingFragment>) fragmentsField.get(scanPlan);
        ProfilingExecPlan.ProfilingElement root =
                new ProfilingExecPlan.ProfilingElement(0, ScanNode.class);
        ProfilingExecPlan.ProfilingElement sink =
                new ProfilingExecPlan.ProfilingElement(-1, ResultSink.class);
        fragments.add(new ProfilingExecPlan.ProfilingFragment(sink, root));

        String result = ExplainAnalyzer.analyze(scanPlan, mockProfile, List.of(0), false);

        assertTrue(result.contains("ScanFilters"), result);
        assertTrue(result.contains("RowProcessing"), result);
        assertTrue(result.contains("IOMetrics"), result);
        assertTrue(result.contains("SegmentProcessing"), result);
        assertTrue(result.contains("IOTask"), result);
        assertTrue(result.contains("IOBuffer"), result);
        assertTrue(result.contains("ZoneMapFilter: "), result);
        assertTrue(result.contains("PredFilter: "), result);
        assertTrue(result.contains("ShortKeyFilter:"), result);
        assertTrue(result.contains("Others"), result);
    }

    @Test
    public void testAggregationOperator() throws NoSuchFieldException, IllegalAccessException {
        // Create mock profile for aggregation operator
        RuntimeProfile aggProfile = new RuntimeProfile("HASH_AGG (plan_node_id=1)");
        aggProfile.addCounter("TotalTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 150000000));
        aggProfile.addCounter("CPUTime", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 120000000));
        aggProfile.addCounter("OutputRows", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 100000));

        // Create CommonMetrics profile
        RuntimeProfile commonMetrics = new RuntimeProfile("CommonMetrics");
        commonMetrics.addCounter("OperatorTotalTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 150000000));
        commonMetrics.addCounter("PullRowNum", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 1000000));
        commonMetrics.addCounter("OperatorPeakMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 64 * 1024 * 1024));
        commonMetrics.addCounter("OperatorAllocatedMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 32 * 1024 * 1024));
        aggProfile.addChild(commonMetrics);

        // Create UniqueMetrics profile with aggregation-specific metrics
        RuntimeProfile uniqueMetrics = new RuntimeProfile("UniqueMetrics");

        // Aggregation metrics
        uniqueMetrics.addCounter("AggFuncComputeTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 80000000));
        uniqueMetrics.addCounter("ExprComputeTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 20000000));
        uniqueMetrics.addCounter("ExprReleaseTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 1000000));
        uniqueMetrics.addCounter("HashTableSize", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 10000));
        uniqueMetrics.addCounter("HashTableMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 16 * 1024 * 1024));

        // Memory Management metrics
        uniqueMetrics.addCounter("ChunkBufferPeakMem", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 8 * 1024 * 1024));
        uniqueMetrics.addCounter("ChunkBufferPeakSize", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 100));
        uniqueMetrics.addCounter("StateAllocate", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 5000000));
        uniqueMetrics.addCounter("StateDestroy", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 2000000));

        // Result Processing metrics
        uniqueMetrics.addCounter("GetResultsTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 10000000));
        uniqueMetrics.addCounter("ResultAggAppendTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 5000000));
        uniqueMetrics.addCounter("ResultGroupByAppendTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 3000000));
        uniqueMetrics.addCounter("ResultIteratorTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 2000000));

        // Data Flow metrics
        uniqueMetrics.addCounter("InputRowCount", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 1000000));
        uniqueMetrics.addCounter("PassThroughRowCount", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 50000));
        uniqueMetrics.addCounter("StreamingTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 15000000));
        uniqueMetrics.addCounter("RowsReturned", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 100000));

        aggProfile.addChild(uniqueMetrics);

        // Create pipeline profile structure
        RuntimeProfile pipelineProfile = new RuntimeProfile("Pipeline 1");
        pipelineProfile.addChild(aggProfile);
        mockProfile.getChild("Execution").getChild("Fragment 0").addChild(pipelineProfile);

        // Create mock plan with AggregationNode
        ProfilingExecPlan aggPlan = new ProfilingExecPlan();
        Field level = ProfilingExecPlan.class.getDeclaredField("profileLevel");
        level.setAccessible(true);
        level.setInt(aggPlan, 1);

        Field fragmentsField = ProfilingExecPlan.class.getDeclaredField("fragments");
        fragmentsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<ProfilingExecPlan.ProfilingFragment> fragments =
                (List<ProfilingExecPlan.ProfilingFragment>) fragmentsField.get(aggPlan);
        ProfilingExecPlan.ProfilingElement root =
                new ProfilingExecPlan.ProfilingElement(1, AggregationNode.class);
        ProfilingExecPlan.ProfilingElement sink =
                new ProfilingExecPlan.ProfilingElement(-1, ResultSink.class);
        fragments.add(new ProfilingExecPlan.ProfilingFragment(sink, root));

        String result = ExplainAnalyzer.analyze(aggPlan, mockProfile, List.of(1), false);

        assertTrue(result.contains("Aggregation:"), result);
        assertTrue(result.contains("Memory Management:"), result);
        assertTrue(result.contains("Result Processing:"), result);
        assertTrue(result.contains("Data Flow:"), result);
        assertTrue(result.contains("AggFuncComputeTime: "), result);
        assertTrue(result.contains("HashTableSize: "), result);
        assertTrue(result.contains("InputRowCount: "), result);
    }

    @Test
    public void testJoinOperator() throws NoSuchFieldException, IllegalAccessException {
        // Create mock profile for join operator
        RuntimeProfile joinProfile = new RuntimeProfile("HASH_JOIN (plan_node_id=2)");
        joinProfile.addCounter("TotalTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 200000000));
        joinProfile.addCounter("CPUTime", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 180000000));
        joinProfile.addCounter("OutputRows", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 500000));

        // Create CommonMetrics profile
        RuntimeProfile commonMetrics = new RuntimeProfile("CommonMetrics");
        commonMetrics.addCounter("OperatorTotalTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 200000000));
        commonMetrics.addCounter("PullRowNum", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 2000000));
        commonMetrics.addCounter("OperatorPeakMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 128 * 1024 * 1024));
        commonMetrics.addCounter("OperatorAllocatedMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 64 * 1024 * 1024));
        joinProfile.addChild(commonMetrics);

        // Create UniqueMetrics profile with join-specific metrics
        RuntimeProfile uniqueMetrics = new RuntimeProfile("UniqueMetrics");

        // HashTable metrics
        uniqueMetrics.addCounter("BuildBuckets", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 100000));
        uniqueMetrics.addCounter("BuildKeysPerBucket%", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 85));
        uniqueMetrics.addCounter("BuildHashTableTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 50000000));
        uniqueMetrics.addCounter("BuildConjunctEvaluateTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 10000000));
        uniqueMetrics.addCounter("HashTableMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 32 * 1024 * 1024));
        uniqueMetrics.addCounter("PartitionNums", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 4));
        uniqueMetrics.addCounter("PartitionProbeOverhead", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 2000000));

        // ProbeSide metrics
        uniqueMetrics.addCounter("SearchHashTableTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 80000000));
        uniqueMetrics.addCounter("probeCount", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 2000000));
        uniqueMetrics.addCounter("ProbeConjunctEvaluateTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 15000000));
        uniqueMetrics.addCounter("CopyRightTableChunkTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 10000000));
        uniqueMetrics.addCounter("OtherJoinConjunctEvaluateTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 5000000));
        uniqueMetrics.addCounter("OutputBuildColumnTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 8000000));
        uniqueMetrics.addCounter("OutputProbeColumnTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 12000000));
        uniqueMetrics.addCounter("WhereConjunctEvaluateTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 3000000));

        // RuntimeFilter metrics
        uniqueMetrics.addCounter("RuntimeFilterBuildTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 5000000));
        uniqueMetrics.addCounter("RuntimeFilterNum", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 2));
        uniqueMetrics.addCounter("PartialRuntimeMembershipFilterBytes", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 1024 * 1024));

        joinProfile.addChild(uniqueMetrics);

        // Create pipeline profile structure
        RuntimeProfile pipelineProfile = new RuntimeProfile("Pipeline 2");
        pipelineProfile.addChild(joinProfile);
        mockProfile.getChild("Execution").getChild("Fragment 0").addChild(pipelineProfile);

        // Create mock plan with JoinNode
        ProfilingExecPlan joinPlan = new ProfilingExecPlan();
        Field level = ProfilingExecPlan.class.getDeclaredField("profileLevel");
        level.setAccessible(true);
        level.setInt(joinPlan, 1);

        Field fragmentsField = ProfilingExecPlan.class.getDeclaredField("fragments");
        fragmentsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<ProfilingExecPlan.ProfilingFragment> fragments =
                (List<ProfilingExecPlan.ProfilingFragment>) fragmentsField.get(joinPlan);
        ProfilingExecPlan.ProfilingElement root =
                new ProfilingExecPlan.ProfilingElement(2, JoinNode.class);
        ProfilingExecPlan.ProfilingElement sink =
                new ProfilingExecPlan.ProfilingElement(-1, ResultSink.class);
        fragments.add(new ProfilingExecPlan.ProfilingFragment(sink, root));

        String result = ExplainAnalyzer.analyze(joinPlan, mockProfile, List.of(2), false);

        assertTrue(result.contains("HashTable:"), result);
        assertTrue(result.contains("ProbeSide:"), result);
        assertTrue(result.contains("RuntimeFilter:"), result);
        assertTrue(result.contains("BuildBuckets: "), result);
        assertTrue(result.contains("SearchHashTableTime: "), result);
        assertTrue(result.contains("RuntimeFilterNum: "), result);
    }

    @Test
    public void testSortOperator() throws NoSuchFieldException, IllegalAccessException {
        // Create mock profile for sort operator
        RuntimeProfile sortProfile = new RuntimeProfile("SORT (plan_node_id=3)");
        sortProfile.addCounter("TotalTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 120000000));
        sortProfile.addCounter("CPUTime", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.TIME_NS, null, 100000000));
        sortProfile.addCounter("OutputRows", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 200000));

        // Create CommonMetrics profile
        RuntimeProfile commonMetrics = new RuntimeProfile("CommonMetrics");
        commonMetrics.addCounter("OperatorTotalTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 120000000));
        commonMetrics.addCounter("PullRowNum", RuntimeProfile.ROOT_COUNTER, new Counter(TUnit.UNIT, null, 2000000));
        commonMetrics.addCounter("OperatorPeakMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 256 * 1024 * 1024));
        commonMetrics.addCounter("OperatorAllocatedMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 128 * 1024 * 1024));
        sortProfile.addChild(commonMetrics);

        // Create UniqueMetrics profile with sort-specific metrics
        RuntimeProfile uniqueMetrics = new RuntimeProfile("UniqueMetrics");

        // Sort-specific metrics
        uniqueMetrics.addCounter("SortTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 80000000));
        uniqueMetrics.addCounter("SortKeys", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 2));
        uniqueMetrics.addCounter("SortRows", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 2000000));
        uniqueMetrics.addCounter("SortMemoryUsage", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 64 * 1024 * 1024));
        uniqueMetrics.addCounter("SortSpillBytes", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.BYTES, null, 32 * 1024 * 1024));
        uniqueMetrics.addCounter("SortSpillTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 10000000));
        uniqueMetrics.addCounter("SortMergeTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 15000000));
        uniqueMetrics.addCounter("SortPartitionTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 5000000));
        uniqueMetrics.addCounter("SortChunkSize", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 4096));
        uniqueMetrics.addCounter("SortChunkCount", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 500));
        uniqueMetrics.addCounter("SortCompareTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 20000000));
        uniqueMetrics.addCounter("SortCopyTime", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.TIME_NS, null, 10000000));
        uniqueMetrics.addCounter("SortLimit", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 100000));
        uniqueMetrics.addCounter("SortOffset", RuntimeProfile.ROOT_COUNTER,
                new Counter(TUnit.UNIT, null, 0));

        sortProfile.addChild(uniqueMetrics);

        // Create pipeline profile structure
        RuntimeProfile pipelineProfile = new RuntimeProfile("Pipeline 3");
        pipelineProfile.addChild(sortProfile);
        mockProfile.getChild("Execution").getChild("Fragment 0").addChild(pipelineProfile);

        // Create mock plan with SortNode
        ProfilingExecPlan sortPlan = new ProfilingExecPlan();
        Field level = ProfilingExecPlan.class.getDeclaredField("profileLevel");
        level.setAccessible(true);
        level.setInt(sortPlan, 1);

        Field fragmentsField = ProfilingExecPlan.class.getDeclaredField("fragments");
        fragmentsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<ProfilingExecPlan.ProfilingFragment> fragments =
                (List<ProfilingExecPlan.ProfilingFragment>) fragmentsField.get(sortPlan);
        ProfilingExecPlan.ProfilingElement root =
                new ProfilingExecPlan.ProfilingElement(3, SortNode.class);
        ProfilingExecPlan.ProfilingElement sink =
                new ProfilingExecPlan.ProfilingElement(-1, ResultSink.class);
        fragments.add(new ProfilingExecPlan.ProfilingFragment(sink, root));

        String result = ExplainAnalyzer.analyze(sortPlan, mockProfile, List.of(3), false);

        assertTrue(result.contains("Others:"), result);
        assertTrue(result.contains("SortTime: "), result);
        assertTrue(result.contains("SortKeys: "), result);
        assertTrue(result.contains("SortRows: "), result);
        assertTrue(result.contains("SortMemoryUsage: "), result);
        assertTrue(result.contains("SortSpillBytes: "), result);
        assertTrue(result.contains("SortMergeTime: "), result);
    }

}
