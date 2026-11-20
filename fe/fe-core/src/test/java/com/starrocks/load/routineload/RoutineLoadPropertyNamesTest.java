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

import com.google.common.collect.Maps;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.sql.ast.AlterRoutineLoadStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.LabelName;
import com.starrocks.sql.ast.RoutineLoadDataSourceProperties;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class RoutineLoadPropertyNamesTest {

    @Test
    public void testResolveConvertsToLegacyNames() throws AnalysisException {
        Map<String, String> resolved = resolve(Map.of(
                RoutineLoadPropertyNames.LOAD_INTERVAL, "6500ms",
                RoutineLoadPropertyNames.MAX_CONSUME_DURATION, "5s",
                RoutineLoadPropertyNames.LOAD_TIMEOUT, "20000ms",
                RoutineLoadPropertyNames.MAX_CONCURRENCY, "3",
                RoutineLoadPropertyNames.MAX_ERROR_ROWS, "0",
                RoutineLoadPropertyNames.ERROR_WINDOW_ROWS, "2000005"));

        // legacy units: whole seconds, rounded up, and the error window divided by 10, rounded up
        Assertions.assertEquals("7", resolved.get(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY));
        Assertions.assertEquals("5", resolved.get(CreateRoutineLoadStmt.TASK_CONSUME_SECOND));
        Assertions.assertEquals("20", resolved.get(CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND));
        Assertions.assertEquals("3", resolved.get(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY));
        Assertions.assertEquals("0", resolved.get(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY));
        Assertions.assertEquals("200001", resolved.get(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY));

        // the descriptive values are normalized
        Assertions.assertEquals("6500ms", resolved.get(RoutineLoadPropertyNames.LOAD_INTERVAL));
        Assertions.assertEquals("20s", resolved.get(RoutineLoadPropertyNames.LOAD_TIMEOUT));
        Assertions.assertEquals(6, RoutineLoadPropertyNames.descriptiveValues(resolved).size());
    }

    @Test
    public void testResolveKeepsLegacyNamesUntouched() throws AnalysisException {
        Map<String, String> properties = Map.of(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY, "10");
        Map<String, String> resolved = resolve(properties);
        Assertions.assertEquals(properties, resolved);
        Assertions.assertTrue(RoutineLoadPropertyNames.descriptiveValues(resolved).isEmpty());
    }

    @Test
    public void testBothNamesOfOnePropertyAreRejected() {
        Map<String, String> properties = Map.of(
                RoutineLoadPropertyNames.MAX_CONCURRENCY, "3",
                CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "3");
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> resolve(properties));
        Assertions.assertTrue(e.getMessage().contains("cannot be set together"), e.getMessage());
    }

    @Test
    public void testDurationNeedsAUnit() {
        assertRejected(RoutineLoadPropertyNames.LOAD_INTERVAL, "10", "with a unit");
        assertRejected(RoutineLoadPropertyNames.LOAD_INTERVAL, "10min", "with a unit");
        assertRejected(RoutineLoadPropertyNames.LOAD_TIMEOUT, "-1s", "with a unit");
        assertRejected(RoutineLoadPropertyNames.MAX_CONSUME_DURATION, "0s", "larger than 0");
    }

    @Test
    public void testLowerBounds() {
        assertRejected(RoutineLoadPropertyNames.LOAD_INTERVAL, "4999ms", "at least 5s");
        assertRejected(RoutineLoadPropertyNames.ERROR_WINDOW_ROWS, "1999999", "at least 2000000");
        assertRejected(RoutineLoadPropertyNames.MAX_CONCURRENCY, "0", "between 1 and");
        assertRejected(RoutineLoadPropertyNames.MAX_CONCURRENCY, "2147483648", "between 1 and");
        assertRejected(RoutineLoadPropertyNames.MAX_ERROR_ROWS, "-1", "at least 0");
        assertRejected(RoutineLoadPropertyNames.MAX_ERROR_ROWS, "ten", "an integer");
    }

    @Test
    public void testTimeoutMustExceedConsumeDurationInWholeSeconds() throws AnalysisException {
        // 1500ms and 1800ms both round up to 2s, which a task cannot run with
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> resolve(Map.of(
                        RoutineLoadPropertyNames.MAX_CONSUME_DURATION, "1500ms",
                        RoutineLoadPropertyNames.LOAD_TIMEOUT, "1800ms")));
        Assertions.assertTrue(e.getMessage().contains(RoutineLoadPropertyNames.LOAD_TIMEOUT), e.getMessage());

        resolve(Map.of(
                RoutineLoadPropertyNames.MAX_CONSUME_DURATION, "1500ms",
                RoutineLoadPropertyNames.LOAD_TIMEOUT, "2001ms"));

        // with one of the two under its legacy name, the error names the properties as given: 4500ms rounds up to 5s
        e = Assertions.assertThrows(AnalysisException.class,
                () -> resolve(Map.of(
                        RoutineLoadPropertyNames.MAX_CONSUME_DURATION, "4500ms",
                        CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND, "5")));
        Assertions.assertTrue(e.getMessage().startsWith(
                "task_timeout_second must be larger than max_consume_duration"), e.getMessage());

        // both under the legacy names: left to the legacy check
        resolve(Map.of(
                CreateRoutineLoadStmt.TASK_CONSUME_SECOND, "5",
                CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND, "5"));
    }

    @Test
    public void testMatchingDescriptiveValue() {
        Map<String, String> jobProperties = Map.of(RoutineLoadPropertyNames.LOAD_INTERVAL, "6500ms");
        Assertions.assertEquals("6500ms", RoutineLoadPropertyNames.matchingDescriptiveValue(jobProperties,
                RoutineLoadPropertyNames.LOAD_INTERVAL, 7));
        // changed through the legacy name by an FE that does not know the descriptive name
        Assertions.assertNull(RoutineLoadPropertyNames.matchingDescriptiveValue(jobProperties,
                RoutineLoadPropertyNames.LOAD_INTERVAL, 10));
        Assertions.assertNull(RoutineLoadPropertyNames.matchingDescriptiveValue(jobProperties,
                RoutineLoadPropertyNames.MAX_CONCURRENCY, 3));
    }

    @Test
    public void testForgetReplacedDescriptiveValues() {
        Map<String, String> jobProperties = Maps.newHashMap(Map.of(
                RoutineLoadPropertyNames.LOAD_INTERVAL, "6500ms",
                RoutineLoadPropertyNames.MAX_CONCURRENCY, "3"));

        // changed through the descriptive name: the change carries both names, nothing is forgotten
        RoutineLoadPropertyNames.forgetReplacedDescriptiveValues(Map.of(
                RoutineLoadPropertyNames.MAX_CONCURRENCY, "4",
                CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "4"), jobProperties);
        Assertions.assertTrue(jobProperties.containsKey(RoutineLoadPropertyNames.MAX_CONCURRENCY));

        // changed through the legacy name only
        RoutineLoadPropertyNames.forgetReplacedDescriptiveValues(Map.of(
                CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY, "10"), jobProperties);
        Assertions.assertFalse(jobProperties.containsKey(RoutineLoadPropertyNames.LOAD_INTERVAL));
        Assertions.assertTrue(jobProperties.containsKey(RoutineLoadPropertyNames.MAX_CONCURRENCY));
    }

    @Test
    public void testShowCreateUsesTheNameTheUserGave() {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "testJob", 10L, 20L, "127.0.0.1:9092", "topic1");
        Deencapsulation.setField(job, "taskSchedIntervalS", 7L);
        Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
        jobProperties.put(RoutineLoadPropertyNames.LOAD_INTERVAL, "6500ms");

        String propertiesSql = job.jobPropertiesToSql();
        Assertions.assertTrue(propertiesSql.contains("\"load_interval\"=\"6500ms\""), propertiesSql);
        Assertions.assertFalse(propertiesSql.contains(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY),
                propertiesSql);
        // settings given under legacy names are still emitted under them
        Assertions.assertTrue(propertiesSql.contains(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY),
                propertiesSql);
    }

    @Test
    public void testShowCreateRoundTrip() throws StarRocksException {
        List<String> propertyLists = List.of(
                "(\"max_concurrency\"=\"2\", \"max_error_rows\"=\"5\", \"load_interval\"=\"6500ms\", "
                        + "\"error_window_rows\"=\"2000005\", \"max_consume_duration\"=\"3s\", \"load_timeout\"=\"10s\")",
                // the timeout is derived from the consume duration and comes back under its legacy name
                "(\"max_consume_duration\"=\"1500ms\")",
                "(\"task_consume_second\"=\"3\", \"load_timeout\"=\"10s\")",
                "(\"desired_concurrent_number\"=\"2\", \"max_batch_interval\"=\"6\")");
        for (String properties : propertyLists) {
            CreateRoutineLoadStmt created = analyzedCreate(properties);
            String propertiesSql = jobFrom(created).jobPropertiesToSql();
            CreateRoutineLoadStmt recreated = analyzedCreate(propertiesSql);

            Assertions.assertEquals(created.getDesiredConcurrentNum(), recreated.getDesiredConcurrentNum(), propertiesSql);
            Assertions.assertEquals(created.getMaxErrorNum(), recreated.getMaxErrorNum(), propertiesSql);
            Assertions.assertEquals(created.getMaxBatchIntervalS(), recreated.getMaxBatchIntervalS(), propertiesSql);
            Assertions.assertEquals(created.getMaxBatchRows(), recreated.getMaxBatchRows(), propertiesSql);
            Assertions.assertEquals(created.getTaskConsumeSecond(), recreated.getTaskConsumeSecond(), propertiesSql);
            Assertions.assertEquals(created.getTaskTimeoutSecond(), recreated.getTaskTimeoutSecond(), propertiesSql);
            Assertions.assertEquals(created.getDescriptivePropertyValues(), recreated.getDescriptivePropertyValues(),
                    propertiesSql);
        }
    }

    @Test
    public void testShowCreateFallsBackToTheLegacyNameWhenTheDescriptiveValueIsStale() {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "testJob", 10L, 20L, "127.0.0.1:9092", "topic1");
        // an older FE changed the interval through max_batch_interval and kept the descriptive entry
        Deencapsulation.setField(job, "taskSchedIntervalS", 10L);
        Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
        jobProperties.put(RoutineLoadPropertyNames.LOAD_INTERVAL, "6500ms");

        String propertiesSql = job.jobPropertiesToSql();
        Assertions.assertTrue(propertiesSql.contains("\"max_batch_interval\"=\"10\""), propertiesSql);
        Assertions.assertFalse(propertiesSql.contains(RoutineLoadPropertyNames.LOAD_INTERVAL), propertiesSql);
    }

    @Test
    public void testShowReportsTheSettingsInEffect() {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "testJob", 10L, 20L, "127.0.0.1:9092", "topic1");
        Deencapsulation.setField(job, "desireTaskConcurrentNum", 2);
        Deencapsulation.setField(job, "maxErrorNum", 5L);
        Deencapsulation.setField(job, "taskSchedIntervalS", 7L);
        Deencapsulation.setField(job, "maxBatchRows", 200001L);
        Deencapsulation.setField(job, "taskConsumeSecond", 2L);
        Deencapsulation.setField(job, "taskTimeoutSecond", 10L);
        Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
        jobProperties.putAll(Map.of(
                RoutineLoadPropertyNames.MAX_CONCURRENCY, "2",
                RoutineLoadPropertyNames.MAX_ERROR_ROWS, "5",
                RoutineLoadPropertyNames.LOAD_INTERVAL, "6500ms",
                RoutineLoadPropertyNames.ERROR_WINDOW_ROWS, "2000005",
                RoutineLoadPropertyNames.MAX_CONSUME_DURATION, "1500ms",
                RoutineLoadPropertyNames.LOAD_TIMEOUT, "10s"));

        String json = Deencapsulation.invoke(job, "jobPropertiesToJsonString");
        // the rounded values the job runs with, beside the legacy keys
        for (String entry : List.of("\"maxConcurrency\":\"2\"", "\"maxErrorRows\":\"5\"", "\"loadInterval\":\"7s\"",
                "\"errorWindowRows\":\"2000010\"", "\"maxConsumeDuration\":\"2s\"", "\"loadTimeout\":\"10s\"",
                "\"maxBatchIntervalS\":\"7\"")) {
            Assertions.assertTrue(json.contains(entry), json);
        }
        // the stored descriptive values are not reported under their own names
        for (String name : RoutineLoadPropertyNames.LEGACY_NAMES.keySet()) {
            Assertions.assertFalse(json.contains("\"" + name + "\""), json);
        }
    }

    @Test
    public void testShowKeepsTheOutputOfAJobSetByLegacyNames() {
        List<String> descriptiveKeys = List.of("loadInterval", "maxConsumeDuration", "loadTimeout", "maxConcurrency",
                "maxErrorRows", "errorWindowRows");
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "testJob", 10L, 20L, "127.0.0.1:9092", "topic1");
        Deencapsulation.setField(job, "taskSchedIntervalS", 7L);
        String json = Deencapsulation.invoke(job, "jobPropertiesToJsonString");
        Assertions.assertTrue(json.contains("\"maxBatchIntervalS\":\"7\""), json);
        for (String key : descriptiveKeys) {
            Assertions.assertFalse(json.contains("\"" + key + "\""), json);
        }

        // one setting given under its descriptive name brings in all of them
        Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
        jobProperties.put(RoutineLoadPropertyNames.MAX_CONCURRENCY, "3");
        json = Deencapsulation.invoke(job, "jobPropertiesToJsonString");
        for (String key : descriptiveKeys) {
            Assertions.assertTrue(json.contains("\"" + key + "\""), json);
        }
    }

    @Test
    public void testAlterThroughEitherName() throws StarRocksException {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "testJob", 10L, 20L, "127.0.0.1:9092", "topic1");
        Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");

        Deencapsulation.invoke(job, "modifyCommonJobProperties",
                analyzedAlter(Map.of(RoutineLoadPropertyNames.LOAD_INTERVAL, "8s")));
        Assertions.assertEquals(8L, (long) Deencapsulation.getField(job, "taskSchedIntervalS"));
        Assertions.assertEquals("8s", jobProperties.get(RoutineLoadPropertyNames.LOAD_INTERVAL));

        // altering through the legacy name switches the reported name back to it
        Deencapsulation.invoke(job, "modifyCommonJobProperties",
                analyzedAlter(Map.of(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY, "9")));
        Assertions.assertEquals(9L, (long) Deencapsulation.getField(job, "taskSchedIntervalS"));
        Assertions.assertFalse(jobProperties.containsKey(RoutineLoadPropertyNames.LOAD_INTERVAL));
    }

    @Test
    public void testAlterDerivingTheTimeoutForgetsItsDescriptiveValue() throws StarRocksException {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "testJob", 10L, 20L, "127.0.0.1:9092", "topic1");
        Deencapsulation.setField(job, "taskTimeoutSecond", 40L);
        Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
        jobProperties.put(RoutineLoadPropertyNames.LOAD_TIMEOUT, "40s");

        // the timeout is derived from the new consume duration, as it is for task_consume_second
        Deencapsulation.invoke(job, "modifyCommonJobProperties",
                analyzedAlter(Map.of(RoutineLoadPropertyNames.MAX_CONSUME_DURATION, "5s")));
        Assertions.assertEquals(20L, (long) Deencapsulation.getField(job, "taskTimeoutSecond"));
        Assertions.assertFalse(jobProperties.containsKey(RoutineLoadPropertyNames.LOAD_TIMEOUT));

        String propertiesSql = job.jobPropertiesToSql();
        Assertions.assertTrue(propertiesSql.contains("\"max_consume_duration\"=\"5s\""), propertiesSql);
        Assertions.assertTrue(propertiesSql.contains("\"task_timeout_second\"=\"20\""), propertiesSql);
    }

    private static CreateRoutineLoadStmt analyzedCreate(String propertiesClause) throws StarRocksException {
        String ddl = "CREATE ROUTINE LOAD testDb.testJob ON testTbl PROPERTIES " + propertiesClause
                + " FROM KAFKA (\"kafka_broker_list\"=\"127.0.0.1:9092\",\"kafka_topic\"=\"topic1\")";
        CreateRoutineLoadStmt stmt = (CreateRoutineLoadStmt) SqlParser.parse(ddl, 32).get(0);
        stmt.checkJobProperties();
        return stmt;
    }

    // a job with the settings RoutineLoadJob.setOptional takes from an analyzed statement
    private static KafkaRoutineLoadJob jobFrom(CreateRoutineLoadStmt stmt) {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "testJob", 10L, 20L, "127.0.0.1:9092", "topic1");
        Deencapsulation.setField(job, "desireTaskConcurrentNum", stmt.getDesiredConcurrentNum());
        Deencapsulation.setField(job, "maxErrorNum", stmt.getMaxErrorNum());
        Deencapsulation.setField(job, "taskSchedIntervalS", stmt.getMaxBatchIntervalS());
        Deencapsulation.setField(job, "maxBatchRows", stmt.getMaxBatchRows());
        Deencapsulation.setField(job, "taskConsumeSecond", stmt.getTaskConsumeSecond());
        Deencapsulation.setField(job, "taskTimeoutSecond", stmt.getTaskTimeoutSecond());
        Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
        jobProperties.putAll(stmt.getDescriptivePropertyValues());
        return job;
    }

    // the change that ALTER ROUTINE LOAD ... PROPERTIES (...) applies to the job
    private static Map<String, String> analyzedAlter(Map<String, String> properties) throws StarRocksException {
        AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"), null,
                Maps.newHashMap(properties), new RoutineLoadDataSourceProperties());
        stmt.checkJobProperties();
        return Maps.newHashMap(stmt.getAnalyzedJobProperties());
    }

    private static Map<String, String> resolve(Map<String, String> properties) throws AnalysisException {
        return RoutineLoadPropertyNames.resolve(properties);
    }

    private static void assertRejected(String name, String value, String expectedMessage) {
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> resolve(Map.of(name, value)));
        Assertions.assertTrue(e.getMessage().contains(expectedMessage), e.getMessage());
    }
}
