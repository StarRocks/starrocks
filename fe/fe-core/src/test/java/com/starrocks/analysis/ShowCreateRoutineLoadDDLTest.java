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

package com.starrocks.analysis;

import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.load.routineload.KafkaRoutineLoadJob;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

// The DDL generated for SHOW CREATE ROUTINE LOAD must be runnable: user-controlled property
// values (jsonpaths, json_root, merge_condition) can contain double quotes and backslashes,
// which must be escaped so the emitted statement parses back to the original values.
public class ShowCreateRoutineLoadDDLTest {

    private KafkaRoutineLoadJob jobWithProperties(Map<String, String> extraProperties) {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "testJob", 10L, 20L, "127.0.0.1:9092", "topic1");
        Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
        jobProperties.putAll(extraProperties);
        return job;
    }

    // Embed jobPropertiesToSql() output in a full CREATE ROUTINE LOAD statement, parse it back,
    // and return the raw job properties the parser recovered.
    private Map<String, String> roundTrip(KafkaRoutineLoadJob job) {
        String ddl = "CREATE ROUTINE LOAD testDb.testJob ON testTbl PROPERTIES " + job.jobPropertiesToSql() +
                "FROM KAFKA (\"kafka_broker_list\"=\"127.0.0.1:9092\",\"kafka_topic\"=\"topic1\")";
        List<StatementBase> stmts = SqlParser.parse(ddl, 32);
        Assertions.assertEquals(1, stmts.size(), ddl);
        CreateRoutineLoadStmt parsed = (CreateRoutineLoadStmt) stmts.get(0);
        return parsed.getJobProperties();
    }

    @Test
    public void testJsonPathsWithDoubleQuotesRoundTrip() {
        String jsonPaths = "[\"$.id\", \"$.name\"]";
        KafkaRoutineLoadJob job = jobWithProperties(Map.of(
                CreateRoutineLoadStmt.FORMAT, "json",
                CreateRoutineLoadStmt.JSONPATHS, jsonPaths));

        // The emitted value must carry escaped quotes, not raw ones.
        String propertiesSql = job.jobPropertiesToSql();
        Assertions.assertTrue(propertiesSql.contains("\"jsonpaths\"=\"[\\\"$.id\\\", \\\"$.name\\\"]\""),
                propertiesSql);

        Assertions.assertEquals(jsonPaths, roundTrip(job).get(CreateRoutineLoadStmt.JSONPATHS));
    }

    @Test
    public void testNonAsciiJsonPathsRoundTrip() {
        // Non-ASCII path keys must survive unchanged; escapeJava-style \\uXXXX escaping would
        // corrupt them because the parser's escapeBackSlash() cannot decode unicode escapes.
        String jsonPaths = "[\"$.用户名\"]";
        KafkaRoutineLoadJob job = jobWithProperties(Map.of(
                CreateRoutineLoadStmt.FORMAT, "json",
                CreateRoutineLoadStmt.JSONPATHS, jsonPaths));

        Assertions.assertEquals(jsonPaths, roundTrip(job).get(CreateRoutineLoadStmt.JSONPATHS));
    }

    @Test
    public void testJsonPathsWithBackslashRoundTrip() {
        String jsonPaths = "[\"$.a\\b\"]";
        KafkaRoutineLoadJob job = jobWithProperties(Map.of(
                CreateRoutineLoadStmt.FORMAT, "json",
                CreateRoutineLoadStmt.JSONPATHS, jsonPaths));

        Assertions.assertEquals(jsonPaths, roundTrip(job).get(CreateRoutineLoadStmt.JSONPATHS));
    }

    @Test
    public void testAdjacentBackslashQuoteRoundTrip() {
        // A backslash immediately followed by a double quote is the trickiest interaction:
        // the pair must escape to \\\" and decode back to \" rather than collapsing.
        String jsonPaths = "[\"$.a\\\"b\"]";
        KafkaRoutineLoadJob job = jobWithProperties(Map.of(
                CreateRoutineLoadStmt.FORMAT, "json",
                CreateRoutineLoadStmt.JSONPATHS, jsonPaths));

        String propertiesSql = job.jobPropertiesToSql();
        Assertions.assertTrue(propertiesSql.contains("\"jsonpaths\"=\"[\\\"$.a\\\\\\\"b\\\"]\""), propertiesSql);

        Assertions.assertEquals(jsonPaths, roundTrip(job).get(CreateRoutineLoadStmt.JSONPATHS));
    }

    @Test
    public void testJsonRootAndMergeConditionRoundTrip() {
        String jsonRoot = "$.\"da\\ta\"";
        String mergeCondition = "src.op = \"delete\"";
        KafkaRoutineLoadJob job = jobWithProperties(Map.of(
                CreateRoutineLoadStmt.FORMAT, "json",
                CreateRoutineLoadStmt.JSONPATHS, "[\"$.id\"]",
                CreateRoutineLoadStmt.JSONROOT, jsonRoot,
                LoadStmt.MERGE_CONDITION, mergeCondition));

        Map<String, String> parsed = roundTrip(job);
        Assertions.assertEquals(jsonRoot, parsed.get(CreateRoutineLoadStmt.JSONROOT));
        Assertions.assertEquals(mergeCondition, parsed.get(LoadStmt.MERGE_CONDITION));
    }

    @Test
    public void testEmptyJsonPathsRoundTrip() {
        KafkaRoutineLoadJob job = jobWithProperties(Map.of(CreateRoutineLoadStmt.FORMAT, "json"));

        Assertions.assertEquals("", roundTrip(job).get(CreateRoutineLoadStmt.JSONPATHS));
    }
}
