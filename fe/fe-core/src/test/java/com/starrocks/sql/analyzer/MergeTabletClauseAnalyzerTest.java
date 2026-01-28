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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.MergeTabletClause;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.TabletGroupList;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MergeTabletClauseAnalyzerTest {
    private static Database db;
    private static OlapTable table;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        Config.enable_range_distribution = true;

        starRocksAssert.withDatabase("test").useDatabase("test");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String sql = "create table test_table (key1 int, key2 varchar(10))\n" +
                "order by(key1)\n" +
                "properties('replication_num' = '1'); ";
        starRocksAssert.withTable(sql);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "test_table");
        Assertions.assertNotNull(table);
    }

    @Test
    public void testRejectNonCloudNativeTable() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(new IcebergTable());
        MergeTabletClause clause = new MergeTabletClause();
        SemanticException exception = Assertions.assertThrows(SemanticException.class,
                () -> analyzer.analyze(null, clause));
        Assertions.assertTrue(exception.getMessage().contains("Merge tablet only support cloud native tables"));
    }

    @Test
    public void testRejectPartitionAndTablets() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
        PartitionRef partitionRef = new PartitionRef(List.of("p1"), false, NodePosition.ZERO);
        MergeTabletClause clause = new MergeTabletClause(partitionRef,
                new TabletGroupList(List.of(List.of(1L, 2L))), null);
        SemanticException exception = Assertions.assertThrows(SemanticException.class,
                () -> analyzer.analyze(null, clause));
        Assertions.assertTrue(exception.getMessage().contains("Partitions and tablets cannot be specified"));
    }

    @Test
    public void testRejectTempPartition() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
        PartitionRef partitionRef = new PartitionRef(List.of("p1"), true, NodePosition.ZERO);
        MergeTabletClause clause = new MergeTabletClause(partitionRef, null, null);
        SemanticException exception = Assertions.assertThrows(SemanticException.class,
                () -> analyzer.analyze(null, clause));
        Assertions.assertTrue(exception.getMessage().contains("Cannot merge tablet in temp partition"));
    }

    @Test
    public void testRejectEmptyPartitions() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
        PartitionRef partitionRef = new PartitionRef(new ArrayList<>(), false, NodePosition.ZERO);
        MergeTabletClause clause = new MergeTabletClause(partitionRef, null, null);
        SemanticException exception = Assertions.assertThrows(SemanticException.class,
                () -> analyzer.analyze(null, clause));
        Assertions.assertTrue(exception.getMessage().contains("Empty partitions"));
    }

    @Test
    public void testRejectEmptyTabletGroups() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
        MergeTabletClause clause = new MergeTabletClause(null, new TabletGroupList(List.of()), null);
        SemanticException exception = Assertions.assertThrows(SemanticException.class,
                () -> analyzer.analyze(null, clause));
        Assertions.assertTrue(exception.getMessage().contains("Empty tablets"));
    }

    @Test
    public void testRejectEmptyTabletGroup() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
        MergeTabletClause clause = new MergeTabletClause(null, new TabletGroupList(List.of(new ArrayList<>())), null);
        SemanticException exception = Assertions.assertThrows(SemanticException.class,
                () -> analyzer.analyze(null, clause));
        Assertions.assertTrue(exception.getMessage().contains("Empty tablets"));
    }

    @Test
    public void testRejectSingleTabletGroup() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
        MergeTabletClause clause = new MergeTabletClause(null, new TabletGroupList(List.of(List.of(1L))), null);
        SemanticException exception = Assertions.assertThrows(SemanticException.class,
                () -> analyzer.analyze(null, clause));
        Assertions.assertTrue(exception.getMessage().contains("at least 2 tablets"));
    }

    @Test
    public void testRejectInvalidProperty() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
        Map<String, String> properties = Map.of("tablet_reshard_target_size", "abc");
        MergeTabletClause clause = new MergeTabletClause(null,
                new TabletGroupList(List.of(List.of(1L, 2L))), properties);
        Assertions.assertThrows(SemanticException.class, () -> analyzer.analyze(null, clause));
    }

    @Test
    public void testRejectUnknownProperty() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
        Map<String, String> properties = new HashMap<>();
        properties.put("tablet_reshard_target_size", "1024");
        properties.put("unknown_key", "1");
        MergeTabletClause clause = new MergeTabletClause(null,
                new TabletGroupList(List.of(List.of(1L, 2L))), properties);
        SemanticException exception = Assertions.assertThrows(SemanticException.class,
                () -> analyzer.analyze(null, clause));
        Assertions.assertTrue(exception.getMessage().contains("Unknown properties"));
    }

    @Test
    public void testAcceptValidClause() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
        Map<String, String> properties = Map.of("tablet_reshard_target_size", "1024");
        MergeTabletClause clause = new MergeTabletClause(null,
                new TabletGroupList(List.of(List.of(1L, 2L))), properties);
        analyzer.analyze(null, clause);
        Assertions.assertEquals(1024L, clause.getTabletReshardTargetSize());
    }
}
