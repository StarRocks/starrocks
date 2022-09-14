// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/CreateRoutineLoadStmtTest.java

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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.load.routineload.KafkaProgress;
import com.starrocks.load.routineload.LoadDataSourceType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateRoutineLoadStmtTest {

    private static final Logger LOG = LogManager.getLogger(CreateRoutineLoadStmtTest.class);

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    @Test
    public void testAnalyzeWithDuplicateProperty(@Injectable Analyzer analyzer) throws UserException {
        String jobName = "job1";
        String dbName = "db1";
        LabelName labelName = new LabelName(dbName, jobName);
        String tableNameString = "table1";
        String topicName = "topic1";
        String serverAddress = "http://127.0.0.1:8080";
        String kafkaPartitionString = "1,2,3";
        List<String> partitionNameString = Lists.newArrayList();
        partitionNameString.add("p1");
        PartitionNames partitionNames = new PartitionNames(false, partitionNameString);
        ColumnSeparator columnSeparator = new ColumnSeparator(",");

        // duplicate load property
        List<ParseNode> loadPropertyList = new ArrayList<>();
        loadPropertyList.add(columnSeparator);
        loadPropertyList.add(columnSeparator);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        String typeName = LoadDataSourceType.KAFKA.name();
        Map<String, String> customProperties = Maps.newHashMap();

        customProperties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, topicName);
        customProperties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, serverAddress);
        customProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, kafkaPartitionString);

        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, tableNameString,
                loadPropertyList, properties,
                typeName, customProperties);

        new MockUp<StatementBase>() {
            @Mock
            public void analyze(Analyzer analyzer1) {
                return;
            }
        };

        try {
            createRoutineLoadStmt.analyze(analyzer);
            Assert.fail();
        } catch (AnalysisException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testAnalyze(@Injectable Analyzer analyzer) throws UserException {
        String jobName = "job1";
        String dbName = "db1";
        LabelName labelName = new LabelName(dbName, jobName);
        String tableNameString = "table1";
        String topicName = "topic1";
        String serverAddress = "127.0.0.1:8080";
        String kafkaPartitionString = "1,2,3";
        String timeZone = "8:00";
        List<String> partitionNameString = Lists.newArrayList();
        partitionNameString.add("p1");
        PartitionNames partitionNames = new PartitionNames(false, partitionNameString);
        ColumnSeparator columnSeparator = new ColumnSeparator(",");

        // duplicate load property
        TableName tableName = new TableName(dbName, tableNameString);
        List<ParseNode> loadPropertyList = new ArrayList<>();
        loadPropertyList.add(columnSeparator);
        loadPropertyList.add(partitionNames);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        properties.put(LoadStmt.TIMEZONE, timeZone);
        String typeName = LoadDataSourceType.KAFKA.name();
        Map<String, String> customProperties = Maps.newHashMap();

        customProperties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, topicName);
        customProperties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, serverAddress);
        customProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, kafkaPartitionString);

        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, tableNameString,
                loadPropertyList, properties,
                typeName, customProperties);
        new MockUp<StatementBase>() {
            @Mock
            public void analyze(Analyzer analyzer1) {
                return;
            }
        };

        createRoutineLoadStmt.analyze(analyzer);

        Assert.assertNotNull(createRoutineLoadStmt.getRoutineLoadDesc());
        Assert.assertEquals(columnSeparator, createRoutineLoadStmt.getRoutineLoadDesc().getColumnSeparator());
        Assert.assertEquals(partitionNames.getPartitionNames(),
                createRoutineLoadStmt.getRoutineLoadDesc().getPartitionNames().getPartitionNames());
        Assert.assertEquals(2, createRoutineLoadStmt.getDesiredConcurrentNum());
        Assert.assertEquals(0, createRoutineLoadStmt.getMaxErrorNum());
        Assert.assertEquals(serverAddress, createRoutineLoadStmt.getKafkaBrokerList());
        Assert.assertEquals(topicName, createRoutineLoadStmt.getKafkaTopic());
        Assert.assertEquals("+08:00", createRoutineLoadStmt.getTimezone());
    }

    @Test
    public void testAnalyzeJsonConfig() throws Exception {
        String createSQL = "CREATE ROUTINE LOAD db0.routine_load_0 ON t1 " +
                "PROPERTIES(\"format\" = \"json\",\"jsonpaths\"=\"[\\\"$.k1\\\",\\\"$.k2.\\\\\\\"k2.1\\\\\\\"\\\"]\") " +
                "FROM KAFKA(\"kafka_broker_list\" = \"xxx.xxx.xxx.xxx:xxx\",\"kafka_topic\" = \"topic_0\");";
        ConnectContext ctx = starRocksAssert.getCtx();
        CreateRoutineLoadStmt createRoutineLoadStmt = (CreateRoutineLoadStmt) UtFrameUtils
                .parseAndAnalyzeStmt(createSQL, ctx);
        Assert.assertEquals(createRoutineLoadStmt.getJsonPaths(), "[\"$.k1\",\"$.k2.\\\"k2.1\\\"\"]");

        String selectSQL = "SELECT \"Pat O\"\"Hanrahan & <Matthew Eldridge]\"\"\";";
        QueryStatement selectStmt = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(selectSQL, ctx);

        Expr expr = ((SelectRelation) (selectStmt.getQueryRelation())).getOutputExpr().get(0);
        Assert.assertTrue(expr instanceof StringLiteral);
        StringLiteral stringLiteral = (StringLiteral) expr;
        Assert.assertEquals(stringLiteral.getValue(), "Pat O\"Hanrahan & <Matthew Eldridge]\"");

    }

    @Test
    public void testKafkaOffset(@Injectable Analyzer analyzer) throws UserException {
        new MockUp<StatementBase>() {
            @Mock
            public void analyze(Analyzer analyzer1) {
                return;
            }
        };

        String jobName = "job1";
        String dbName = "db1";
        String tableNameString = "table1";
        String kafkaDefaultOffsetsKey = "property." + CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS;

        // load property
        List<String> partitionNameString = Lists.newArrayList();
        partitionNameString.add("p1");
        PartitionNames partitionNames = new PartitionNames(false, partitionNameString);
        ColumnSeparator columnSeparator = new ColumnSeparator(",");
        List<ParseNode> loadPropertyList = new ArrayList<>();
        loadPropertyList.add(columnSeparator);
        loadPropertyList.add(partitionNames);

        // 1. kafka_offsets
        // 1 -> OFFSET_BEGINNING, 2 -> OFFSET_END
        Map<String, String> customProperties = getCustomProperties();
        customProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "1,2");
        customProperties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "OFFSET_BEGINNING,OFFSET_END");
        LabelName labelName = new LabelName(dbName, jobName);
        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(
                labelName, tableNameString, loadPropertyList, Maps.newHashMap(),
                LoadDataSourceType.KAFKA.name(), customProperties);
        createRoutineLoadStmt.analyze(analyzer);
        List<Pair<Integer, Long>> partitionOffsets = createRoutineLoadStmt.getKafkaPartitionOffsets();
        Assert.assertEquals(2, partitionOffsets.size());
        Assert.assertEquals(KafkaProgress.OFFSET_BEGINNING_VAL, (long) partitionOffsets.get(0).second);
        Assert.assertEquals(KafkaProgress.OFFSET_END_VAL, (long) partitionOffsets.get(1).second);

        // 2. no kafka_offsets and property.kafka_default_offsets
        // 1,2 -> OFFSET_END
        customProperties = getCustomProperties();
        customProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "1,2");
        labelName = new LabelName(dbName, jobName);
        createRoutineLoadStmt =
                new CreateRoutineLoadStmt(labelName, tableNameString, loadPropertyList, Maps.newHashMap(),
                        LoadDataSourceType.KAFKA.name(), customProperties);
        createRoutineLoadStmt.analyze(analyzer);
        partitionOffsets = createRoutineLoadStmt.getKafkaPartitionOffsets();
        Assert.assertEquals(2, partitionOffsets.size());
        Assert.assertEquals(KafkaProgress.OFFSET_END_VAL, (long) partitionOffsets.get(0).second);
        Assert.assertEquals(KafkaProgress.OFFSET_END_VAL, (long) partitionOffsets.get(1).second);

        // 3. property.kafka_default_offsets
        // 1,2 -> 10
        customProperties = getCustomProperties();
        customProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "1,2");
        customProperties.put(kafkaDefaultOffsetsKey, "10");
        labelName = new LabelName(dbName, jobName);
        createRoutineLoadStmt =
                new CreateRoutineLoadStmt(labelName, tableNameString, loadPropertyList, Maps.newHashMap(),
                        LoadDataSourceType.KAFKA.name(), customProperties);
        createRoutineLoadStmt.analyze(analyzer);
        partitionOffsets = createRoutineLoadStmt.getKafkaPartitionOffsets();
        Assert.assertEquals(2, partitionOffsets.size());
        Assert.assertEquals(10, (long) partitionOffsets.get(0).second);
        Assert.assertEquals(10, (long) partitionOffsets.get(1).second);

        // 4. both kafka_offsets and property.kafka_default_offsets
        // 1 -> OFFSET_BEGINNING, 2 -> OFFSET_END, 3 -> 11
        customProperties = getCustomProperties();
        customProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "1,2,3");
        customProperties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "OFFSET_BEGINNING,OFFSET_END,11");
        customProperties.put(kafkaDefaultOffsetsKey, "10");
        labelName = new LabelName(dbName, jobName);
        createRoutineLoadStmt =
                new CreateRoutineLoadStmt(labelName, tableNameString, loadPropertyList, Maps.newHashMap(),
                        LoadDataSourceType.KAFKA.name(), customProperties);
        createRoutineLoadStmt.analyze(analyzer);
        partitionOffsets = createRoutineLoadStmt.getKafkaPartitionOffsets();
        Assert.assertEquals(3, partitionOffsets.size());
        Assert.assertEquals(KafkaProgress.OFFSET_BEGINNING_VAL, (long) partitionOffsets.get(0).second);
        Assert.assertEquals(KafkaProgress.OFFSET_END_VAL, (long) partitionOffsets.get(1).second);
        Assert.assertEquals(11, (long) partitionOffsets.get(2).second);
    }

    private Map<String, String> getCustomProperties() {
        Map<String, String> customProperties = Maps.newHashMap();
        customProperties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, "topic1");
        customProperties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, "127.0.0.1:8080");
        return customProperties;
    }
}
