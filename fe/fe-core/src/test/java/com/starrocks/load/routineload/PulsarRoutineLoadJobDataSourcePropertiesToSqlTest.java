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

package com.starrocks.load.routineload;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Tests for {@link PulsarRoutineLoadJob#dataSourcePropertiesToSql()}.
 *
 * Covers the three review-comment fixes:
 * 1. Custom partitions with partially committed positions must not produce
 *    a shorter pulsar_initial_positions list — instead positions are omitted.
 * 2. Auto-discovery jobs must not emit pulsar_partitions so replay preserves
 *    partition auto-discovery.
 * 3. Custom property values containing double-quotes must be escaped.
 */
public class PulsarRoutineLoadJobDataSourcePropertiesToSqlTest {

    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC = "test-topic";
    private static final String SUBSCRIPTION = "test-sub";

    /**
     * Helper to build a PulsarRoutineLoadJob with the given partitions and positions.
     */
    private PulsarRoutineLoadJob buildJob(List<String> customPartitions,
                                          List<String> currentPartitions,
                                          List<Pair<String, Long>> initialPositions,
                                          Map<String, String> customProperties) {
        PulsarRoutineLoadJob job = new PulsarRoutineLoadJob(
                1L, "test_job", 1L, 1L, SERVICE_URL, TOPIC, SUBSCRIPTION);

        if (customPartitions != null && !customPartitions.isEmpty()) {
            Deencapsulation.setField(job, "customPulsarPartitions", customPartitions);
        }
        if (currentPartitions != null && !currentPartitions.isEmpty()) {
            Deencapsulation.setField(job, "currentPulsarPartitions", currentPartitions);
        }
        if (initialPositions != null) {
            PulsarProgress progress = (PulsarProgress) Deencapsulation.getField(job, "progress");
            for (Pair<String, Long> pos : initialPositions) {
                progress.addPartitionToInitialPosition(pos);
            }
        }
        if (customProperties != null && !customProperties.isEmpty()) {
            Deencapsulation.setField(job, "customProperties", customProperties);
        }
        return job;
    }

    // -------------------------------------------------------------------------
    // Review comment 1: initial positions with partial commits
    // -------------------------------------------------------------------------

    @Test
    public void testCustomPartitions_allPositionsSet_emitsPositions() {
        // All partitions have explicit initial positions → should emit pulsar_initial_positions
        List<String> partitions = Lists.newArrayList("p0", "p1", "p2");
        List<Pair<String, Long>> positions = Lists.newArrayList(
                Pair.create("p0", PulsarRoutineLoadJob.POSITION_EARLIEST_VAL),
                Pair.create("p1", PulsarRoutineLoadJob.POSITION_LATEST_VAL),
                Pair.create("p2", PulsarRoutineLoadJob.POSITION_EARLIEST_VAL));
        PulsarRoutineLoadJob job = buildJob(partitions, partitions, positions, null);

        String sql = job.dataSourcePropertiesToSql();
        Assertions.assertTrue(sql.contains(CreateRoutineLoadStmt.PULSAR_PARTITIONS_PROPERTY),
                "Should contain pulsar_partitions");
        Assertions.assertTrue(sql.contains(CreateRoutineLoadStmt.PULSAR_INITIAL_POSITIONS_PROPERTY),
                "Should contain pulsar_initial_positions when all partitions have positions");
        // Verify positions are in sorted partition order (p0, p1, p2)
        Assertions.assertTrue(sql.contains("POSITION_EARLIEST,POSITION_LATEST,POSITION_EARLIEST"),
                "Positions should be listed in sorted partition order");
    }

    @Test
    public void testCustomPartitions_somePositionsCommitted_omitsPositions() {
        // Simulates partial commit: p0 and p1 have initial positions, but p2 was committed
        // (PulsarProgress.update() removed p2's initial position, so getInitialPosition returns -1).
        // SHOW CREATE must NOT emit pulsar_initial_positions at all (omit rather than mismatch).
        List<String> partitions = Lists.newArrayList("p0", "p1", "p2");
        List<Pair<String, Long>> positions = Lists.newArrayList(
                Pair.create("p0", PulsarRoutineLoadJob.POSITION_EARLIEST_VAL),
                Pair.create("p1", PulsarRoutineLoadJob.POSITION_LATEST_VAL));
        // Note: p2 has no initial position (simulates committed partition)
        PulsarRoutineLoadJob job = buildJob(partitions, partitions, positions, null);

        String sql = job.dataSourcePropertiesToSql();
        Assertions.assertTrue(sql.contains(CreateRoutineLoadStmt.PULSAR_PARTITIONS_PROPERTY),
                "Should still contain pulsar_partitions");
        Assertions.assertFalse(sql.contains(CreateRoutineLoadStmt.PULSAR_INITIAL_POSITIONS_PROPERTY),
                "Must NOT contain pulsar_initial_positions when some partitions have no position");
    }

    @Test
    public void testCustomPartitions_noPositionsSet_omitsPositions() {
        // Job created with pulsar_partitions but without pulsar_initial_positions.
        // All partitions return -1 from getInitialPosition.
        List<String> partitions = Lists.newArrayList("p0", "p1");
        PulsarRoutineLoadJob job = buildJob(partitions, partitions, null, null);

        String sql = job.dataSourcePropertiesToSql();
        Assertions.assertTrue(sql.contains(CreateRoutineLoadStmt.PULSAR_PARTITIONS_PROPERTY),
                "Should contain pulsar_partitions");
        Assertions.assertFalse(sql.contains(CreateRoutineLoadStmt.PULSAR_INITIAL_POSITIONS_PROPERTY),
                "Must NOT contain pulsar_initial_positions when no positions were set");
    }

    @Test
    public void testCustomPartitions_allPositionsCommitted_omitsPositions() {
        // All partitions were committed (all return -1).
        List<String> partitions = Lists.newArrayList("p0", "p1");
        // Set positions then simulate commit by updating progress
        List<Pair<String, Long>> positions = Lists.newArrayList(
                Pair.create("p0", PulsarRoutineLoadJob.POSITION_EARLIEST_VAL),
                Pair.create("p1", PulsarRoutineLoadJob.POSITION_EARLIEST_VAL));
        PulsarRoutineLoadJob job = buildJob(partitions, partitions, positions, null);

        // Simulate PulsarProgress.update() which removes initial positions for committed partitions
        PulsarProgress progress = (PulsarProgress) Deencapsulation.getField(job, "progress");
        PulsarProgress commitProgress = new PulsarProgress();
        Deencapsulation.setField(commitProgress, "partitionToBacklogNum",
                Maps.newHashMap(Map.of("p0", 0L, "p1", 0L)));
        progress.update(commitProgress);

        String sql = job.dataSourcePropertiesToSql();
        Assertions.assertTrue(sql.contains(CreateRoutineLoadStmt.PULSAR_PARTITIONS_PROPERTY));
        Assertions.assertFalse(sql.contains(CreateRoutineLoadStmt.PULSAR_INITIAL_POSITIONS_PROPERTY),
                "Must NOT contain pulsar_initial_positions after all partitions committed");
    }

    // -------------------------------------------------------------------------
    // Review comment 2: auto-discovery jobs must not emit partitions
    // -------------------------------------------------------------------------

    @Test
    public void testAutoDiscoveryJob_noCustomPartitions_omitsPartitions() {
        // Job created without pulsar_partitions — auto-discovers from broker.
        // currentPulsarPartitions may be populated from the broker, but SHOW CREATE
        // must NOT emit them to preserve auto-discovery on replay.
        List<String> currentPartitions = Lists.newArrayList("broker-p0", "broker-p1", "broker-p2");
        PulsarRoutineLoadJob job = buildJob(null, currentPartitions, null, null);

        String sql = job.dataSourcePropertiesToSql();
        Assertions.assertFalse(sql.contains(CreateRoutineLoadStmt.PULSAR_PARTITIONS_PROPERTY),
                "Must NOT contain pulsar_partitions for auto-discovery jobs");
        Assertions.assertFalse(sql.contains("broker-p0"),
                "Must NOT emit current broker partitions");
        // Verify basic properties are still present
        Assertions.assertTrue(sql.contains(SERVICE_URL));
        Assertions.assertTrue(sql.contains(TOPIC));
        Assertions.assertTrue(sql.contains(SUBSCRIPTION));
    }

    @Test
    public void testAutoDiscoveryJob_emptyEverything() {
        // No custom partitions, no current partitions
        PulsarRoutineLoadJob job = buildJob(null, null, null, null);

        String sql = job.dataSourcePropertiesToSql();
        Assertions.assertFalse(sql.contains(CreateRoutineLoadStmt.PULSAR_PARTITIONS_PROPERTY),
                "Must NOT contain pulsar_partitions");
        Assertions.assertTrue(sql.contains(SERVICE_URL));
        Assertions.assertTrue(sql.contains(TOPIC));
        Assertions.assertTrue(sql.contains(SUBSCRIPTION));
    }

    // -------------------------------------------------------------------------
    // Review comment 3: custom property value escaping
    // -------------------------------------------------------------------------

    @Test
    public void testCustomProperties_jsonValueWithDoubleQuotes_isEscaped() {
        // OAuth authParams-like value with embedded double quotes
        Map<String, String> props = Maps.newLinkedHashMap();
        props.put("authParams", "{\"privateKey\":\"file:///path/key.json\",\"audience\":\"urn:sn:pulsar:test\"}");
        PulsarRoutineLoadJob job = buildJob(null, null, null, props);

        String sql = job.dataSourcePropertiesToSql();
        // The value should have escaped double-quotes
        Assertions.assertTrue(sql.contains("\\\"privateKey\\\""),
                "Double quotes in property values must be escaped: " + sql);
        Assertions.assertTrue(sql.contains("\\\"audience\\\""),
                "Double quotes in property values must be escaped: " + sql);
        // The property key itself should not be escaped
        Assertions.assertTrue(sql.contains("\"property.authParams\"=\""),
                "Property key format should be preserved: " + sql);
    }

    @Test
    public void testCustomProperties_valueWithBackslashAndQuotes_isEscaped() {
        Map<String, String> props = Maps.newLinkedHashMap();
        props.put("testProp", "value with \\backslash and \"quotes\"");
        PulsarRoutineLoadJob job = buildJob(null, null, null, props);

        String sql = job.dataSourcePropertiesToSql();
        // Backslash should be escaped first, then quotes
        Assertions.assertTrue(sql.contains("\\\\backslash"),
                "Backslashes in property values must be escaped: " + sql);
        Assertions.assertTrue(sql.contains("\\\"quotes\\\""),
                "Double quotes in property values must be escaped: " + sql);
    }

    @Test
    public void testCustomProperties_sensitivePropertyIsMasked() {
        Map<String, String> props = Maps.newLinkedHashMap();
        props.put("auth.password", "my-secret-password");
        props.put("auth.token", "my-secret-token");
        props.put("client.secret", "my-client-secret");
        props.put("normalProp", "normalValue");
        PulsarRoutineLoadJob job = buildJob(null, null, null, props);

        String sql = job.dataSourcePropertiesToSql();
        Assertions.assertTrue(sql.contains("******"),
                "Sensitive properties should be masked");
        Assertions.assertFalse(sql.contains("my-secret-password"),
                "Password value must not appear in output");
        Assertions.assertFalse(sql.contains("my-secret-token"),
                "Token value must not appear in output");
        Assertions.assertFalse(sql.contains("my-client-secret"),
                "Secret value must not appear in output");
        Assertions.assertTrue(sql.contains("normalValue"),
                "Non-sensitive properties should appear unmasked");
    }

    @Test
    public void testCustomProperties_plainValue_noExtraEscaping() {
        Map<String, String> props = Maps.newLinkedHashMap();
        props.put("simpleProp", "simpleValue");
        PulsarRoutineLoadJob job = buildJob(null, null, null, props);

        String sql = job.dataSourcePropertiesToSql();
        Assertions.assertTrue(sql.contains("\"property.simpleProp\"=\"simpleValue\""),
                "Plain values should pass through without modification: " + sql);
    }

    // -------------------------------------------------------------------------
    // Combined scenario tests
    // -------------------------------------------------------------------------

    @Test
    public void testCustomPartitionsWithPropertiesAndPositions() {
        // Full scenario: custom partitions + initial positions + custom properties with JSON
        List<String> partitions = Lists.newArrayList("p1", "p0");
        List<Pair<String, Long>> positions = Lists.newArrayList(
                Pair.create("p0", PulsarRoutineLoadJob.POSITION_EARLIEST_VAL),
                Pair.create("p1", PulsarRoutineLoadJob.POSITION_LATEST_VAL));
        Map<String, String> props = Maps.newLinkedHashMap();
        props.put("authPlugin", "org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2");
        props.put("authParams", "{\"key\":\"val\"}");

        PulsarRoutineLoadJob job = buildJob(partitions, partitions, positions, props);

        String sql = job.dataSourcePropertiesToSql();
        // Partitions should be sorted
        Assertions.assertTrue(sql.contains("\"pulsar_partitions\"=\"p0,p1\""),
                "Partitions should be sorted: " + sql);
        // Positions should be emitted (all present)
        Assertions.assertTrue(sql.contains(CreateRoutineLoadStmt.PULSAR_INITIAL_POSITIONS_PROPERTY));
        // JSON property value should be escaped
        Assertions.assertTrue(sql.contains("\\\"key\\\""),
                "JSON in custom properties should be escaped: " + sql);
    }

    @Test
    public void testPartitionsSortedCorrectly() {
        // Verify partition sorting
        List<String> partitions = Lists.newArrayList("partition-c", "partition-a", "partition-b");
        List<Pair<String, Long>> positions = Lists.newArrayList(
                Pair.create("partition-a", PulsarRoutineLoadJob.POSITION_EARLIEST_VAL),
                Pair.create("partition-b", PulsarRoutineLoadJob.POSITION_LATEST_VAL),
                Pair.create("partition-c", PulsarRoutineLoadJob.POSITION_EARLIEST_VAL));
        PulsarRoutineLoadJob job = buildJob(partitions, partitions, positions, null);

        String sql = job.dataSourcePropertiesToSql();
        Assertions.assertTrue(sql.contains("partition-a,partition-b,partition-c"),
                "Partitions must be sorted alphabetically: " + sql);
        Assertions.assertTrue(
                sql.contains("POSITION_EARLIEST,POSITION_LATEST,POSITION_EARLIEST"),
                "Positions must follow sorted partition order: " + sql);
    }
}
