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

package com.starrocks.planner;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.InsertPlanner;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InsertPlannerTest {

    @Test
    public void testRefreshAllCollectedExternalTables(@Mocked ConnectContext session,
                                        @Mocked GlobalStateMgr gsm,
                                        @Mocked MetadataMgr metadataMgr,
                                        @Mocked QueryStatement qs,
                                        @Mocked SessionVariable sessVar,
                                        @Mocked AnalyzerUtils unusedStatic,
                                        @Mocked Table t1,
                                        @Mocked Table t2) {
        new Expectations() {{
            session.getGlobalStateMgr(); result = gsm; minTimes = 0;
            gsm.getMetadataMgr(); result = metadataMgr; minTimes = 0;

            session.getSessionVariable(); result = sessVar;
            sessVar.isEnableInsertSelectExternalAutoRefresh(); result = true;

            t1.getCatalogName(); result = "c1"; minTimes = 0;
            t1.getCatalogDBName(); result = "db1"; minTimes = 0;
            t1.isExternalTableWithFileSystem(); result = true; minTimes = 0;

            t2.getCatalogName(); result = "c2"; minTimes = 0;
            t2.getCatalogDBName(); result = "db2"; minTimes = 0;
            t2.isExternalTableWithFileSystem(); result = true; minTimes = 0;

            AnalyzerUtils.collectAllTableWithAlias(qs);
            result = new Delegate<Void>() {
                @SuppressWarnings("unused")
                Map<TableName, Table> delegate(QueryStatement qs) {
                        Map<TableName, Table> out = Maps.newHashMap();
                        out.put(new TableName("c1", "db1", "t1"), t1);
                        out.put(new TableName("c2", "db2", "t2"), t2);
                        return out;
                }
            };
        }};

        InsertPlanner target = new InsertPlanner();
        target.refreshExternalTable(qs, session);

        new Verifications() {{
            metadataMgr.refreshTable("c1", "db1", t1, (List<String>) any, false); times = 1;
            metadataMgr.refreshTable("c2", "db2", t2, (List<String>) any, false); times = 1;
        }};
    }

    @Test
    public void testAutoRefreshDisabled(@Mocked ConnectContext session,
                                        @Mocked SessionVariable sessVar,
                                        @Mocked GlobalStateMgr gsm,
                                        @Mocked MetadataMgr metadataMgr,
                                        @Mocked QueryStatement qs,
                                        @Mocked AnalyzerUtils unusedStatic) {
        new Expectations() {{
            session.getSessionVariable(); result = sessVar;
            sessVar.isEnableInsertSelectExternalAutoRefresh(); result = false;
        }};

        InsertPlanner target = new InsertPlanner();
        target.refreshExternalTable(qs, session);

        new Verifications() {{
            AnalyzerUtils.collectAllTableWithAlias(qs); times = 0;
            metadataMgr.refreshTable(anyString, anyString, (Table) any, (List<String>) any, anyBoolean); times = 0;
            session.getGlobalStateMgr(); times = 0; 
        }};
    }

    @Test
    public void testDoNothingWhenNoTableCollected(@Mocked ConnectContext session,
                                        @Mocked GlobalStateMgr gsm,
                                        @Mocked SessionVariable sessVar,
                                        @Mocked MetadataMgr metadataMgr,
                                        @Mocked QueryStatement qs,
                                        @Mocked AnalyzerUtils unusedStatic) {
        new Expectations() {{
                session.getSessionVariable(); result = sessVar;
                sessVar.isEnableInsertSelectExternalAutoRefresh(); result = true;
                AnalyzerUtils.collectAllTableWithAlias(qs);
                result = new Delegate<Void>() {
                    @SuppressWarnings("unused")
                    Map<TableName, Table> delegate(QueryStatement _qs) {
                        return Maps.newHashMap();
                    }
                };
        }};

        InsertPlanner target = new InsertPlanner();
        target.refreshExternalTable(qs, session);

        new Verifications() {{
            metadataMgr.refreshTable(anyString, anyString, (Table) any, (List<String>) any, anyBoolean); times = 0;
        }};
    }
}