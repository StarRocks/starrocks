// This file is made available under Elastic License 2.0.
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
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.ImportColumnsStmt;
import com.starrocks.analysis.ImportWhereStmt;
import com.starrocks.analysis.SqlParser;
import com.starrocks.analysis.SqlScanner;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Type;
import com.starrocks.common.UserException;
import com.starrocks.common.util.SqlParserUtils;
import com.starrocks.task.StreamLoadTask;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TStreamLoadPutRequest;
import com.starrocks.thrift.TUniqueId;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

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

    @Test
    public void testNormalPlan() throws UserException {
        List<Column> columns = Lists.newArrayList();
        Column c1 = new Column("c1", Type.BIGINT, false);
        columns.add(c1);
        Column c2 = new Column("c2", Type.BIGINT, true);
        columns.add(c2);
        new Expectations() {
            {
                destTable.getBaseSchema();
                minTimes = 0;
                result = columns;
                destTable.getPartitions();
                minTimes = 0;
                result = Arrays.asList(partition);
                scanNode.init((Analyzer) any);
                minTimes = 0;
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
        StreamLoadTask streamLoadTask = StreamLoadTask.fromTStreamLoadPutRequest(request, db);
        StreamLoadPlanner planner = new StreamLoadPlanner(db, destTable, streamLoadTask);
        planner.plan(streamLoadTask.getId());
    }

    @Test
    public void testParseStmt() throws Exception {
        String sql = new String("COLUMNS (k1, k2, k3=abc(), k4=default_value())");
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(sql)));
        ImportColumnsStmt columnsStmt = (ImportColumnsStmt) SqlParserUtils.getFirstStmt(parser);
        Assert.assertEquals(4, columnsStmt.getColumns().size());

        sql = new String("WHERE k1 > 2 and k3 < 4");
        parser = new SqlParser(new SqlScanner(new StringReader(sql)));
        ImportWhereStmt whereStmt = (ImportWhereStmt) SqlParserUtils.getFirstStmt(parser);
        Assert.assertTrue(whereStmt.getExpr() instanceof CompoundPredicate);
    }
}
