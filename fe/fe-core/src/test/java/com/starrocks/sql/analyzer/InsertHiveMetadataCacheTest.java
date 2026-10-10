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

import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Whether an INSERT reading Hive bypasses the connector metadata cache. With the auto refresh every source is
 * refreshed for the statement, so the cache is current and used; without it the cache is bypassed (#22272) unless
 * enable_hive_metadata_cache_with_insert allows it.
 */
public class InsertHiveMetadataCacheTest extends ConnectorPlanTestBase {

    private static final String INSERT_FROM_HIVE =
            "insert into t0 (v1, v2) select l_orderkey, l_partkey from hive0.tpch.lineitem";

    @BeforeEach
    public void reset() {
        connectContext.setUseConnectorMetadataCache(Optional.empty());
        connectContext.getSessionVariable().setEnableHiveMetadataCacheWithInsert(false);
        connectContext.getSessionVariable().setEnableInsertSelectExternalAutoRefresh(true);
    }

    @AfterEach
    public void cleanup() {
        reset();
    }

    @Test
    public void testAutoRefreshKeepsMetadataCache() throws Exception {
        UtFrameUtils.parseStmtWithNewParser(INSERT_FROM_HIVE, connectContext);
        Assertions.assertEquals(Optional.empty(), connectContext.getUseConnectorMetadataCache());
    }

    @Test
    public void testWithoutAutoRefreshBypassesMetadataCache() throws Exception {
        connectContext.getSessionVariable().setEnableInsertSelectExternalAutoRefresh(false);
        UtFrameUtils.parseStmtWithNewParser(INSERT_FROM_HIVE, connectContext);
        Assertions.assertEquals(Optional.of(false), connectContext.getUseConnectorMetadataCache());
    }

    @Test
    public void testWithoutAutoRefreshKeepsMetadataCacheWhenAllowed() throws Exception {
        connectContext.getSessionVariable().setEnableInsertSelectExternalAutoRefresh(false);
        connectContext.getSessionVariable().setEnableHiveMetadataCacheWithInsert(true);
        UtFrameUtils.parseStmtWithNewParser(INSERT_FROM_HIVE, connectContext);
        Assertions.assertEquals(Optional.empty(), connectContext.getUseConnectorMetadataCache());
    }

    @Test
    public void testExplicitChoiceIsKept() throws Exception {
        // MV refresh decides for itself (PartitionBasedMvRefreshProcessor sets Optional.of(true)).
        connectContext.getSessionVariable().setEnableInsertSelectExternalAutoRefresh(false);
        connectContext.setUseConnectorMetadataCache(Optional.of(true));
        UtFrameUtils.parseStmtWithNewParser(INSERT_FROM_HIVE, connectContext);
        Assertions.assertEquals(Optional.of(true), connectContext.getUseConnectorMetadataCache());
    }

    @Test
    public void testInsertFromOlapDoesNotTouchMetadataCache() throws Exception {
        connectContext.getSessionVariable().setEnableInsertSelectExternalAutoRefresh(false);
        UtFrameUtils.parseStmtWithNewParser("insert into t0 select * from t1", connectContext);
        Assertions.assertEquals(Optional.empty(), connectContext.getUseConnectorMetadataCache());
    }
}
