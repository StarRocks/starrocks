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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/planner/StreamLoadPlannerTest.java

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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.load.routineload.KafkaRoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.ImportColumnsStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TPartialUpdateMode;
import com.starrocks.thrift.TStreamLoadPutRequest;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_PARTIAL_UPDATE_MODE;

public class StreamLoadPlannerTest {
    @Injectable
    Database db;

    @Injectable
    OlapTable destTable;

    @Mocked
    StreamLoadScanNode scanNode;

    @Mocked
    OlapTableSink sink;

    @Mocked
    Partition partition;

    @BeforeEach
    public void before() {
        UtFrameUtils.mockInitWarehouseEnv();
    }

    @Test
    public void testNormalPlan() throws StarRocksException {
        List<Column> columns = Lists.newArrayList();
        Column c1 = new Column("c1", IntegerType.BIGINT, false);
        columns.add(c1);
        Column c2 = new Column("c2", IntegerType.BIGINT, true);
        columns.add(c2);
        new Expectations() {
            {
                destTable.getBaseSchema();
                minTimes = 0;
                result = columns;
                destTable.getPartitions();
                minTimes = 0;
                result = Arrays.asList(partition);
                scanNode.getChildren();
                minTimes = 0;
                result = Lists.newArrayList();
                scanNode.getId();
                minTimes = 0;
                result = new PlanNodeId(5);
                partition.getId();
                minTimes = 0;
                result = 0;
            }
        };
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setTxnId(1);
        request.setLoadId(new TUniqueId(2, 3));
        request.setFileType(TFileType.FILE_STREAM);
        request.setFormatType(TFileFormatType.FORMAT_CSV_PLAIN);
        request.setLoad_dop(2);
        request.setPayload_compression_type("LZ4_FRAME");
        StreamLoadInfo streamLoadInfo = StreamLoadInfo.fromTStreamLoadPutRequest(request, db);
        StreamLoadPlanner planner = new StreamLoadPlanner(new ConnectContext(), db, destTable, streamLoadInfo);
        planner.plan(streamLoadInfo.getId());
        Assertions.assertEquals(TCompressionType.LZ4_FRAME, streamLoadInfo.getPayloadCompressionType());
    }

    @Test
    public void testPartialUpdatePlan() throws StarRocksException {
        List<Column> columns = Lists.newArrayList();
        Column c1 = new Column("c1", IntegerType.BIGINT, false);
        columns.add(c1);
        Column c2 = new Column("c2", IntegerType.BIGINT, true);
        columns.add(c2);
        new Expectations() {
            {
                destTable.getKeysType();
                minTimes = 0;
                result = KeysType.PRIMARY_KEYS;
                destTable.getBaseSchema();
                minTimes = 0;
                result = columns;
                destTable.getPartitions();
                minTimes = 0;
                result = Arrays.asList(partition);
                scanNode.getChildren();
                minTimes = 0;
                result = Lists.newArrayList();
                scanNode.getId();
                minTimes = 0;
                result = new PlanNodeId(5);
                partition.getId();
                minTimes = 0;
                result = 0;
            }
        };
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setTxnId(1);
        request.setLoadId(new TUniqueId(2, 3));
        request.setFileType(TFileType.FILE_STREAM);
        request.setFormatType(TFileFormatType.FORMAT_CSV_PLAIN);
        request.setPartial_update(true);
        request.setColumns("c1");
        StreamLoadInfo streamLoadInfo = StreamLoadInfo.fromTStreamLoadPutRequest(request, db);
        StreamLoadPlanner planner = new StreamLoadPlanner(new ConnectContext(), db, destTable, streamLoadInfo);
        planner.plan(streamLoadInfo.getId());
    }

    @Test
    public void testPartialUpdateMode() throws StarRocksException {
        StreamLoadKvParams param = new StreamLoadKvParams(
                Collections.singletonMap(HTTP_PARTIAL_UPDATE_MODE, "column"));
        TUniqueId loadId = UUIDUtil.genTUniqueId();
        StreamLoadInfo.fromHttpStreamLoadRequest(loadId, 100, Optional.of(100), param,
                CRAcquireContext.of(WarehouseManager.DEFAULT_WAREHOUSE_NAME));
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        StreamLoadInfo.fromRoutineLoadJob(routineLoadJob);
    }

    // `partial_update_mode=auto` on a JSON partial update arrives with the flexible bit set (the BE sets it for
    // every JSON auto load). This destTable is an unstubbed mock, i.e. NOT cloud-native, so flexible cannot
    // apply -- and `auto` worked on such tables before flexible existed. The plan must therefore succeed as
    // the homogeneous partial update, and the info flag the scan node reads must be cleared so no hidden
    // "__cset__" slot is injected for a payload the writer will not treat as flexible.
    @Test
    public void testAutoFlexibleDegradesOnUnsupportedTable() throws StarRocksException {
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("c1", IntegerType.BIGINT, false));
        columns.add(new Column("c2", IntegerType.BIGINT, true));
        new Expectations() {
            {
                destTable.getKeysType();
                minTimes = 0;
                result = KeysType.PRIMARY_KEYS;
                destTable.getBaseSchema();
                minTimes = 0;
                result = columns;
                destTable.getPartitions();
                minTimes = 0;
                result = Arrays.asList(partition);
                scanNode.getChildren();
                minTimes = 0;
                result = Lists.newArrayList();
                scanNode.getId();
                minTimes = 0;
                result = new PlanNodeId(5);
                partition.getId();
                minTimes = 0;
                result = 0;
            }
        };
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setTxnId(1);
        request.setLoadId(new TUniqueId(2, 3));
        request.setFileType(TFileType.FILE_STREAM);
        request.setFormatType(TFileFormatType.FORMAT_JSON);
        request.setPartial_update(true);
        request.setPartial_update_mode(TPartialUpdateMode.AUTO_MODE);
        request.setFlexible_partial_update(true);
        request.setColumns("c1,c2");
        StreamLoadInfo streamLoadInfo = StreamLoadInfo.fromTStreamLoadPutRequest(request, db);
        Assertions.assertTrue(streamLoadInfo.isFlexiblePartialUpdate());
        StreamLoadPlanner planner = new StreamLoadPlanner(new ConnectContext(), db, destTable, streamLoadInfo);
        planner.plan(streamLoadInfo.getId());
        Assertions.assertFalse(streamLoadInfo.isFlexiblePartialUpdate(),
                "auto must degrade to the homogeneous plan on a table flexible cannot apply to");
    }

    // The same request with an explicit `partial_update_mode=flexible` names the feature, so the unsupported
    // table is an error the caller asked for.
    @Test
    public void testExplicitFlexibleRejectedOnUnsupportedTable() throws StarRocksException {
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("c1", IntegerType.BIGINT, false));
        columns.add(new Column("c2", IntegerType.BIGINT, true));
        new Expectations() {
            {
                destTable.getKeysType();
                minTimes = 0;
                result = KeysType.PRIMARY_KEYS;
                destTable.getBaseSchema();
                minTimes = 0;
                result = columns;
            }
        };
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setTxnId(1);
        request.setLoadId(new TUniqueId(2, 3));
        request.setFileType(TFileType.FILE_STREAM);
        request.setFormatType(TFileFormatType.FORMAT_JSON);
        request.setPartial_update(true);
        request.setPartial_update_mode(TPartialUpdateMode.COLUMN_UPDATE_MODE);
        request.setFlexible_partial_update(true);
        request.setColumns("c1,c2");
        StreamLoadInfo streamLoadInfo = StreamLoadInfo.fromTStreamLoadPutRequest(request, db);
        StreamLoadPlanner planner = new StreamLoadPlanner(new ConnectContext(), db, destTable, streamLoadInfo);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "shared-data",
                () -> planner.plan(streamLoadInfo.getId()));
    }

    @Test
    public void testParseStmt() {
        String sql = "COLUMNS (k1, k2, k3=abc(), k4=default_value())";
        ImportColumnsStmt columnsStmt = com.starrocks.sql.parser.SqlParser.parseImportColumns(sql, 0);
        Assertions.assertEquals(4, columnsStmt.getColumns().size());

        sql = "k1 > 2 and k3 < 4";
        Expr where = com.starrocks.sql.parser.SqlParser.parseSqlToExpr(sql, 0);
        Assertions.assertTrue(where instanceof CompoundPredicate);
    }
}
