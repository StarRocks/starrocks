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

package com.starrocks.sql.analyzer;

import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class InsertHiveMetadataCacheTest extends ConnectorPlanTestBase {

    private static final String INSERT_FROM_HIVE =
            "insert into t0 (v1, v2) select l_orderkey, l_partkey from hive0.tpch.lineitem";

    @BeforeEach
    public void reset() {
        connectContext.setUseConnectorMetadataCache(Optional.empty());
        connectContext.getSessionVariable().setEnableHiveMetadataCacheWithInsert(false);
    }

    @AfterEach
    public void cleanup() {
        reset();
    }

    @Test
    public void testInsertFromHiveDisablesMetadataCacheByDefault() throws Exception {
        UtFrameUtils.parseStmtWithNewParser(INSERT_FROM_HIVE, connectContext);
        Assertions.assertEquals(Optional.of(false), connectContext.getUseConnectorMetadataCache());
    }

    @Test
    public void testInsertFromHiveKeepsMetadataCacheWhenEnabled() throws Exception {
        connectContext.getSessionVariable().setEnableHiveMetadataCacheWithInsert(true);
        UtFrameUtils.parseStmtWithNewParser(INSERT_FROM_HIVE, connectContext);
        Assertions.assertEquals(Optional.empty(), connectContext.getUseConnectorMetadataCache());
    }

    @Test
    public void testInsertFromHiveKeepsMetadataCacheAfterAutoRefresh() throws Exception {
        // The pre-lock pass refreshed the source table, so planning reads the fresh cache instead of bypassing it.
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(INSERT_FROM_HIVE, connectContext);
        new QueryAnalyzer(connectContext).analyzeExternalTablesOnly(stmt, true);
        Analyzer.analyze(stmt, connectContext);
        Assertions.assertEquals(Optional.of(true), connectContext.getUseConnectorMetadataCache());
    }

    @Test
    public void testInsertFromHiveDisablesMetadataCacheWithoutAutoRefresh() throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(INSERT_FROM_HIVE, connectContext);
        new QueryAnalyzer(connectContext).analyzeExternalTablesOnly(stmt, false);
        Analyzer.analyze(stmt, connectContext);
        Assertions.assertEquals(Optional.of(false), connectContext.getUseConnectorMetadataCache());
    }

    @Test
    public void testInsertFromOlapDoesNotTouchMetadataCache() throws Exception {
        UtFrameUtils.parseStmtWithNewParser("insert into t0 select * from t1", connectContext);
        Assertions.assertEquals(Optional.empty(), connectContext.getUseConnectorMetadataCache());
    }
}
