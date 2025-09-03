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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/AlterRoutineLoadStmtTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.collect.Maps;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AlterRoutineLoadAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterRoutineLoadStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.LabelName;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.RoutineLoadDataSourceProperties;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/*
 * Author: Chenmingyu
 * Date: Jul 20, 2020
 */

public class AlterRoutineLoadStmtTest {

    private static ConnectContext connectContext;

    @BeforeEach
    public void setUp() throws IOException {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testParser() {
        String sql = "alter ROUTINE LOAD for testdb.routine_name\n"
                + "WHERE k1 > 1 and k2 like \"%starrocks%\",\n"
                + "COLUMNS(k1, k2, k4 = k1 + k2),\n"
                + "COLUMNS TERMINATED BY \"\\t\",\n"
                + "PARTITION(p1,p2) \n"
                + "PROPERTIES\n"
                + "(\n"
                + "\"max_batch_rows\"=\"200000\",\n"
                + "\"max_error_number\"=\"1\",\n"
                + "\"max_filter_ratio\"=\"0.3\",\n"
                + "\"desired_concurrent_number\"=\"3\",\n"
                + "\"max_batch_interval\" = \"21\",\n"
                + "\"strict_mode\" = \"false\",\n"
                + "\"task_consume_second\" = \"5\",\n"
                + "\"timezone\" = \"Africa/Abidjan\"\n"
                + ")\n"
                + "FROM KAFKA\n"
                + "(\n"
                + "\"kafka_partitions\" = \"0, 1, 2\",\n"
                + "\"kafka_offsets\" = \"100, 200, 100\",\n"
                + "\"property.group.id\" = \"group1\",\n"
                + "\"confluent.schema.registry.url\" = \"https://key:passwrod@addr\"\n"
                + ");";
        List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
        AlterRoutineLoadStmt stmt = (AlterRoutineLoadStmt)stmts.get(0);
        AlterRoutineLoadAnalyzer.analyze(stmt, connectContext);

        Assertions.assertEquals(9, stmt.getAnalyzedJobProperties().size());
        Assertions.assertTrue(
                stmt.getAnalyzedJobProperties().containsKey(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY));
        Assertions.assertTrue(
            stmt.getAnalyzedJobProperties().containsKey(CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY));
        Assertions.assertEquals("0.3", stmt.getAnalyzedJobProperties().get(CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY));
        Assertions.assertTrue(
                stmt.getAnalyzedJobProperties().containsKey(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY));
        Assertions.assertEquals("5", stmt.getAnalyzedJobProperties().get(CreateRoutineLoadStmt.TASK_CONSUME_SECOND));
        Assertions.assertEquals("20", stmt.getAnalyzedJobProperties().get(CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND));
        Assertions.assertTrue(stmt.hasDataSourceProperty());
        Assertions.assertEquals(1, stmt.getDataSourceProperties().getCustomKafkaProperties().size());
        Assertions.assertTrue(stmt.getDataSourceProperties().getCustomKafkaProperties().containsKey("group.id"));
        Assertions.assertEquals(3, stmt.getDataSourceProperties().getKafkaPartitionOffsets().size());
        Assertions.assertEquals("https://key:passwrod@addr", stmt.getDataSourceProperties().getConfluentSchemaRegistryUrl());
    }

    @Test
    public void testLoadPropertiesContexts() {
        String sql = "ALTER ROUTINE LOAD for testdb.routine_name \n"
                + "PROPERTIES\n"
                + "(\n"
                + "\"max_error_number\"=\"1000\"\n"
                + ")\n";
        List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
        AlterRoutineLoadStmt alterRoutineLoadStmt = (AlterRoutineLoadStmt)stmts.get(0);
        AlterRoutineLoadAnalyzer.analyze(alterRoutineLoadStmt, connectContext);
        Assertions.assertNotNull(alterRoutineLoadStmt.getRoutineLoadDesc());
        Assertions.assertEquals(0, alterRoutineLoadStmt.getLoadPropertyList().size());
    }

    @Test
    public void testLoadColumns() {
        String sql = "ALTER ROUTINE LOAD for testdb.routine_name " +
                " COLUMNS(`k1`, `k2`, `k3`, `k4`, `k5`," +
                " `v1` = to_bitmap(`k1`))" +
                " PROPERTIES (\"desired_concurrent_number\"=\"1\")" +
                " FROM KAFKA (" +
                "\"kafka_partitions\" = \"0, 1, 2\",\n" +
                "\"kafka_offsets\" = \"100, 200, 100\",\n" +
                "\"property.group.id\" = \"group1\"\n" +
                ")";
        List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
        AlterRoutineLoadStmt alterRoutineLoadStmt = (AlterRoutineLoadStmt)stmts.get(0);
        AlterRoutineLoadAnalyzer.analyze(alterRoutineLoadStmt, connectContext);
        Assertions.assertEquals(6, alterRoutineLoadStmt.getRoutineLoadDesc().getColumnsInfo().getColumns().size());

        sql = "ALTER ROUTINE LOAD for testdb.routine_name" +
                " COLUMNS(`k1`, `k2`, `k3`, `k4`, `k5`)" +
                " PROPERTIES (\"desired_concurrent_number\"=\"1\")";
        stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
        alterRoutineLoadStmt = (AlterRoutineLoadStmt)stmts.get(0);
        AlterRoutineLoadAnalyzer.analyze(alterRoutineLoadStmt, connectContext);
        Assertions.assertEquals(5, alterRoutineLoadStmt.getRoutineLoadDesc().getColumnsInfo().getColumns().size());

        sql = "ALTER ROUTINE LOAD for testdb.routine_name " +
                " COLUMNS( `v1` = to_bitmap(`k1`)," +
                " `v2` = to_bitmap(`k2`)," +
                " `v3` = to_bitmap(`k3`)," +
                " `v4` = to_bitmap(`k4`)," +
                " `v5` = to_bitmap(`k5`))" +
                " PROPERTIES (\"desired_concurrent_number\"=\"1\")";
        stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
        alterRoutineLoadStmt = (AlterRoutineLoadStmt)stmts.get(0);
        AlterRoutineLoadAnalyzer.analyze(alterRoutineLoadStmt, connectContext);
        Assertions.assertEquals(5, alterRoutineLoadStmt.getRoutineLoadDesc().getColumnsInfo().getColumns().size());

        sql = "ALTER ROUTINE LOAD for testdb.routine_name " +
                " COLUMNS( `v1` = to_bitmap(`k1`)," +
                " `v2` = to_bitmap(`k2`)," +
                " `v3` = to_bitmap(`k3`)," +
                " `v4` = to_bitmap(`k4`)," +
                " `v5` = to_bitmap(`k5`)," +
                " `k1`, `k2`, `k3`, `k4`, `k5` )";
        stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
        alterRoutineLoadStmt = (AlterRoutineLoadStmt)stmts.get(0);
        AlterRoutineLoadAnalyzer.analyze(alterRoutineLoadStmt, connectContext);
        Assertions.assertEquals(10, alterRoutineLoadStmt.getRoutineLoadDesc().getColumnsInfo().getColumns().size());

        sql = "ALTER ROUTINE LOAD for testdb.routine_name " +
                " COLUMNS( `v1` = to_bitmap(`k1`), `k1`," +
                " `v2` = to_bitmap(`k2`), `k2`," +
                " `v3` = to_bitmap(`k3`), `k3`," +
                " `v4` = to_bitmap(`k4`), `k4`," +
                " `v5` = to_bitmap(`k5`), `k5`)" +
                " PROPERTIES (\"desired_concurrent_number\"=\"1\")";
        stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
        alterRoutineLoadStmt = (AlterRoutineLoadStmt)stmts.get(0);
        AlterRoutineLoadAnalyzer.analyze(alterRoutineLoadStmt, connectContext);
        Assertions.assertEquals(10, alterRoutineLoadStmt.getRoutineLoadDesc().getColumnsInfo().getColumns().size());

        sql = "ALTER ROUTINE LOAD for testdb.routine_name " +
                " COLUMNS(`k1`, `k2`, `k3`, `k4`, `k5`," +
                " `v1` = to_bitmap(`k1`)," +
                " `v2` = to_bitmap(`k2`)," +
                " `v3` = to_bitmap(`k3`)," +
                " `v4` = to_bitmap(`k4`)," +
                " `v5` = to_bitmap(`k5`))" +
                " PROPERTIES (\"desired_concurrent_number\"=\"1\")";
        stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
        alterRoutineLoadStmt = (AlterRoutineLoadStmt)stmts.get(0);
        AlterRoutineLoadAnalyzer.analyze(alterRoutineLoadStmt, connectContext);
        Assertions.assertEquals(10, alterRoutineLoadStmt.getRoutineLoadDesc().getColumnsInfo().getColumns().size());
    }

    @Test
    public void testNormal() {
        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "100");
            jobProperties.put(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY, "200000");
            String typeName = "kafka";
            Map<String, String> dataSourceProperties = Maps.newHashMap();
            dataSourceProperties.put("property.client.id", "101");
            dataSourceProperties.put("property.group.id", "mygroup");
            dataSourceProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "1,2,3");
            dataSourceProperties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "10000, 20000, 30000");
            RoutineLoadDataSourceProperties routineLoadDataSourceProperties = new RoutineLoadDataSourceProperties(
                    typeName, dataSourceProperties);
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"),
                    null, jobProperties, routineLoadDataSourceProperties);
            AlterRoutineLoadAnalyzer.analyze(stmt, connectContext);

            Assertions.assertEquals(2, stmt.getAnalyzedJobProperties().size());
            Assertions.assertTrue(
                    stmt.getAnalyzedJobProperties().containsKey(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY));
            Assertions.assertTrue(
                    stmt.getAnalyzedJobProperties().containsKey(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY));
            Assertions.assertTrue(stmt.hasDataSourceProperty());
            Assertions.assertEquals(2, stmt.getDataSourceProperties().getCustomKafkaProperties().size());
            Assertions.assertTrue(stmt.getDataSourceProperties().getCustomKafkaProperties().containsKey("group.id"));
            Assertions.assertTrue(stmt.getDataSourceProperties().getCustomKafkaProperties().containsKey("client.id"));
            Assertions.assertEquals(3, stmt.getDataSourceProperties().getKafkaPartitionOffsets().size());
        }
    }

    @Test
    public void testNoPproperties() {
        AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"), null,
                Maps.newHashMap(), new RoutineLoadDataSourceProperties());
        AlterRoutineLoadAnalyzer.analyze(stmt, connectContext);
    }

    @Test
    public void testUnsupportedProperties() {
        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.FORMAT, "csv");
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"), null,
                    jobProperties, new RoutineLoadDataSourceProperties());
            try {
                AlterRoutineLoadAnalyzer.analyze(stmt, connectContext);
                Assertions.fail();
            } catch (SemanticException e) {
                Assertions.assertTrue(e.getMessage().contains("format is invalid property"));
            }
        }

        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "100");
            String typeName = "kafka";
            Map<String, String> dataSourceProperties = Maps.newHashMap();
            dataSourceProperties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, "new_topic");
            RoutineLoadDataSourceProperties routineLoadDataSourceProperties = new RoutineLoadDataSourceProperties(
                    typeName, dataSourceProperties);
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"), null,
                    jobProperties, routineLoadDataSourceProperties);

            try {
                AlterRoutineLoadAnalyzer.analyze(stmt, connectContext);
                Assertions.fail();
            } catch (SemanticException e) {
                Assertions.assertTrue(e.getMessage().contains("kafka_topic is invalid kafka custom property"));
            }
        }

        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "100");
            String typeName = "kafka";
            Map<String, String> dataSourceProperties = Maps.newHashMap();
            dataSourceProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "1,2,3");
            RoutineLoadDataSourceProperties routineLoadDataSourceProperties = new RoutineLoadDataSourceProperties(
                    typeName, dataSourceProperties);
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"), null,
                    jobProperties, routineLoadDataSourceProperties);
            try {
                AlterRoutineLoadAnalyzer.analyze(stmt, connectContext);
                Assertions.fail();
            } catch (SemanticException e) {
                Assertions.assertTrue(e.getMessage().contains("Partition and offset must be specified at the same time"));
            }
        }

        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "100");
            String typeName = "kafka";
            Map<String, String> dataSourceProperties = Maps.newHashMap();
            dataSourceProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "1,2,3");
            dataSourceProperties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "1000, 2000");
            RoutineLoadDataSourceProperties routineLoadDataSourceProperties = new RoutineLoadDataSourceProperties(
                    typeName, dataSourceProperties);
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"), null,
                    jobProperties, routineLoadDataSourceProperties);
            try {
                AlterRoutineLoadAnalyzer.analyze(stmt, connectContext);
                Assertions.fail();
            } catch (SemanticException e) {
                Assertions.assertTrue(e.getMessage().contains("Partitions number should be equals to offsets number"));
            }
        }

        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "100");
            String typeName = "kafka";
            Map<String, String> dataSourceProperties = Maps.newHashMap();
            dataSourceProperties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "1000, 2000, 3000");
            RoutineLoadDataSourceProperties routineLoadDataSourceProperties = new RoutineLoadDataSourceProperties(
                    typeName, dataSourceProperties);
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"), null,
                    jobProperties, routineLoadDataSourceProperties);
            try {
                AlterRoutineLoadAnalyzer.analyze(stmt, connectContext);
                Assertions.fail();
            } catch (SemanticException e) {
                Assertions.assertTrue(e.getMessage().contains("Missing kafka partition info"));
            }
        }
    }

    @Test
    public void testBackquote() throws SecurityException, IllegalArgumentException {
        String sql = "ALTER ROUTINE LOAD FOR `db_test`.`rl_test` PROPERTIES (\"desired_concurrent_number\" = \"10\")" +
                            "FROM kafka ( \"kafka_partitions\" = \"0, 1, 2\", \"kafka_offsets\" = \"100, 200, 100\"," +  
                            "\"property.group.id\" = \"new_group\" )";

         List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
        AlterRoutineLoadStmt stmt = (AlterRoutineLoadStmt) stmts.get(0);

        Assertions.assertEquals("db_test", stmt.getDbName());
        Assertions.assertEquals("rl_test", stmt.getLabelName().getLabelName());
    }

}