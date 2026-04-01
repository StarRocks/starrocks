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

package com.starrocks.common.util;

import com.starrocks.thrift.TUnit;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@link ProfileSerializer}.
 *
 * <p>Includes a size-comparison test that measures the serialized footprint of a
 * representative query profile under two encodings:
 * <ol>
 *   <li><b>ProfileSerializer (new)</b> — custom binary format with a global
 *       static dictionary ({@link ProfileKeyDictionary}) and a per-tree local
 *       dictionary, wrapped in gzip.</li>
 *   <li><b>Gzipped text (baseline)</b> — {@link RuntimeProfile#toString()} fed
 *       through a {@link GZIPOutputStream}; the simplest possible storage
 *       approach.</li>
 * </ol>
 */
public class ProfileSerializerTest {

    // -------------------------------------------------------------------------
    // Roundtrip correctness
    // -------------------------------------------------------------------------

    @Test
    public void roundTripRestoresProfileNameAndCounterValues() throws IOException {
        RuntimeProfile root = buildRepresentativeProfile();
        byte[] bytes = ProfileSerializer.serialize(root);

        RuntimeProfile restored = ProfileSerializer.deserialize(bytes);

        assertNotNull(restored);
        assertEquals(root.getName(), restored.getName());

        // Root TotalTime counter must survive roundtrip.
        Counter originalTt = root.getCounter(RuntimeProfile.TOTAL_TIME_COUNTER);
        Counter restoredTt = restored.getCounter(RuntimeProfile.TOTAL_TIME_COUNTER);
        assertNotNull(restoredTt, "TotalTime counter must be present after deserialization");
        assertEquals(originalTt.getValue(), restoredTt.getValue());
        assertEquals(originalTt.getType(), restoredTt.getType());
    }

    @Test
    public void roundTripRestoresInfoStrings() throws IOException {
        RuntimeProfile root = buildRepresentativeProfile();
        byte[] bytes = ProfileSerializer.serialize(root);

        RuntimeProfile restored = ProfileSerializer.deserialize(bytes);
        // Summary child carries the query metadata info strings.
        RuntimeProfile summary = restored.getChildList().get(0).first;

        assertEquals("query-roundtrip-001", summary.getInfoString(ProfileManager.QUERY_ID));
        assertEquals("SELECT 1", summary.getInfoString(ProfileManager.SQL_STATEMENT));
    }

    @Test
    public void roundTripRestoresAllChildNodes() throws IOException {
        RuntimeProfile root = buildRepresentativeProfile();
        int originalDepth = countNodes(root);
        byte[] bytes = ProfileSerializer.serialize(root);

        RuntimeProfile restored = ProfileSerializer.deserialize(bytes);
        int restoredDepth = countNodes(restored);

        assertEquals(originalDepth, restoredDepth,
                "Deserialized tree must have the same number of nodes");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Builds a profile tree that resembles a real two-fragment query, with
     * repeated counter names across many nodes — the scenario that most benefits
     * from dictionary compression.
     *
     * <p>Tree structure (abbreviated):
     * <pre>
     * "" (query root)
     *   Summary
     *   Fragment 0
     *     Instance 0-0
     *       Pipeline 0  (ExchangeSinkOperator → AggregateBlockingOperator)
     *       Pipeline 1  (AggregateStreamingOperator → ProjectOperator)
     *   Fragment 1
     *     Instance 1-0
     *       Pipeline 0  (OlapScanOperator → ProjectOperator)
     *     Instance 1-1
     *       Pipeline 0  (OlapScanOperator → ProjectOperator)
     * </pre>
     */
    private static RuntimeProfile buildRepresentativeProfile() {
        RuntimeProfile root = new RuntimeProfile("");
        root.getCounter(RuntimeProfile.TOTAL_TIME_COUNTER).setValue(1_500_000_000L);

        // --- Summary ---
        RuntimeProfile summary = new RuntimeProfile("Summary");
        summary.addInfoString(ProfileManager.QUERY_ID, "query-roundtrip-001");
        summary.addInfoString(ProfileManager.SQL_STATEMENT, "SELECT 1");
        summary.addInfoString(ProfileManager.QUERY_STATE, "EOF");
        summary.addInfoString(ProfileManager.START_TIME, "2024-01-01 00:00:00");
        summary.addInfoString(ProfileManager.END_TIME, "2024-01-01 00:00:01");
        summary.addInfoString(ProfileManager.TOTAL_TIME, "1s500ms");
        summary.addInfoString(ProfileManager.USER, "root");
        summary.addInfoString(ProfileManager.DEFAULT_DB, "tpch");
        root.addChild(summary);

        // --- Fragment 0 (aggregation) ---
        root.addChild(buildFragmentProfile("Fragment 0", 0, new String[] {
                "ExchangeSinkOperator",
                "AggregateBlockingOperator",
                "AggregateStreamingOperator",
                "ProjectOperator",
        }, 1));

        // --- Fragment 1 (scan, 2 instances) ---
        root.addChild(buildFragmentProfile("Fragment 1", 1, new String[] {
                "OlapScanOperator",
                "ProjectOperator",
        }, 2));

        return root;
    }

    /**
     * Builds a fragment profile with {@code instanceCount} instances, each
     * containing one pipeline with {@code operatorNames.length} operators.
     * Every operator gets the same set of counters, which exercises the
     * dictionary's ability to deduplicate repeated counter names.
     */
    private static RuntimeProfile buildFragmentProfile(
            String fragmentName, int fragmentIdx,
            String[] operatorNames, int instanceCount) {

        RuntimeProfile fragment = new RuntimeProfile(fragmentName);
        fragment.getCounter(RuntimeProfile.TOTAL_TIME_COUNTER).setValue(1_000_000_000L);
        fragment.addCounter("BackendNum", TUnit.UNIT, null).setValue(instanceCount);
        fragment.addCounter("InstanceNum", TUnit.UNIT, null).setValue(instanceCount);

        for (int inst = 0; inst < instanceCount; inst++) {
            RuntimeProfile instance = new RuntimeProfile(
                    "Instance " + fragmentIdx + "-" + inst);
            instance.getCounter(RuntimeProfile.TOTAL_TIME_COUNTER)
                    .setValue(900_000_000L + inst * 10_000_000L);
            instance.addInfoString("Address", "127.0.0." + (inst + 1) + ":9060");
            instance.addInfoString("ActiveTime", "900ms");

            RuntimeProfile pipeline = new RuntimeProfile("Pipeline 0");
            pipeline.getCounter(RuntimeProfile.TOTAL_TIME_COUNTER).setValue(880_000_000L);
            pipeline.addCounter("ScheduleTime", TUnit.TIME_NS, null).setValue(5_000_000L);
            pipeline.addCounter("OutputFullTime", TUnit.TIME_NS, null).setValue(1_000_000L);
            pipeline.addCounter("PendingFinishTime", TUnit.TIME_NS, null).setValue(500_000L);

            for (int opIdx = 0; opIdx < operatorNames.length; opIdx++) {
                pipeline.addChild(buildOperatorProfile(
                        operatorNames[opIdx] + " (id=" + (fragmentIdx * 10 + opIdx) + ")",
                        800_000_000L - opIdx * 50_000_000L));
            }
            instance.addChild(pipeline);
            fragment.addChild(instance);
        }
        return fragment;
    }

    /**
     * Builds an operator-level profile with a realistic set of counters.
     * These counter names are identical across all operators, making them
     * the primary beneficiaries of dictionary compression.
     */
    private static RuntimeProfile buildOperatorProfile(String name, long totalTimeNs) {
        RuntimeProfile op = new RuntimeProfile(name);
        op.getCounter(RuntimeProfile.TOTAL_TIME_COUNTER).setValue(totalTimeNs);

        op.addCounter("OperatorTotalTime", TUnit.TIME_NS, null).setValue(totalTimeNs - 1_000_000L);
        op.addCounter("NetworkTime", TUnit.TIME_NS, null).setValue(2_000_000L);
        op.addCounter("ScanTime", TUnit.TIME_NS, null).setValue(3_000_000L);

        // Child counters under OperatorTotalTime
        op.addCounter("WaitTime", TUnit.TIME_NS, null, "OperatorTotalTime")
                .setValue(500_000L);
        op.addCounter("OutputTime", TUnit.TIME_NS, null, "OperatorTotalTime")
                .setValue(1_500_000L);

        op.addCounter("RowsProduced", TUnit.UNIT, null).setValue(100_000L);
        op.addCounter("RowsConsumed", TUnit.UNIT, null).setValue(200_000L);
        op.addCounter("PeakMemoryBytes", TUnit.BYTES, null).setValue(4 * 1024 * 1024L);

        op.addInfoString("DegreeOfParallelism", "4");
        op.addInfoString("ColocatedNumber", "1");

        return op;
    }

    /**
     * Returns the total number of nodes in the profile tree (inclusive).
     */
    private static int countNodes(RuntimeProfile profile) {
        int count = 1;
        for (com.starrocks.common.Pair<RuntimeProfile, Boolean> child : profile.getChildList()) {
            count += countNodes(child.first);
        }
        return count;
    }

}
