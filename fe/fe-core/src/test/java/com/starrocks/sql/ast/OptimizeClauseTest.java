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

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class OptimizeClauseTest {

    @Test
    public void testToSqlBasic() {
        OptimizeClause clause = new OptimizeClause(null, null, null, null, null, null);
        Assertions.assertEquals("OPTIMIZE", clause.toSql());
        Assertions.assertEquals("OPTIMIZE", clause.toString());
    }

    @Test
    public void testToSqlWithPartitionNames() {
        PartitionRef partitionRef = new PartitionRef(
                Lists.newArrayList("p1", "p2"), false, NodePosition.ZERO);
        OptimizeClause clause = new OptimizeClause(null, null, null, null, partitionRef, null);
        Assertions.assertEquals("OPTIMIZE PARTITIONS (p1, p2)", clause.toSql());
    }

    @Test
    public void testToSqlWithEmptyPartitionNames() {
        PartitionRef partitionRef = new PartitionRef(
                Lists.newArrayList(), false, NodePosition.ZERO);
        OptimizeClause clause = new OptimizeClause(null, null, null, null, partitionRef, null);
        // empty partition names should not add anything
        Assertions.assertEquals("OPTIMIZE", clause.toSql());
    }

    @Test
    public void testToSqlWithDuplicateKey() {
        KeysDesc keysDesc = new KeysDesc(KeysType.DUP_KEYS, Lists.newArrayList("col1", "col2"));
        OptimizeClause clause = new OptimizeClause(keysDesc, null, null, null, null, null);
        Assertions.assertEquals("OPTIMIZE DUPLICATE KEY(`col1`, `col2`)", clause.toSql());
    }

    @Test
    public void testToSqlWithPrimaryKey() {
        KeysDesc keysDesc = new KeysDesc(KeysType.PRIMARY_KEYS, Lists.newArrayList("id"));
        OptimizeClause clause = new OptimizeClause(keysDesc, null, null, null, null, null);
        Assertions.assertEquals("OPTIMIZE PRIMARY KEY(`id`)", clause.toSql());
    }

    @Test
    public void testToSqlWithAggregateKey() {
        KeysDesc keysDesc = new KeysDesc(KeysType.AGG_KEYS, Lists.newArrayList("k1", "k2"));
        OptimizeClause clause = new OptimizeClause(keysDesc, null, null, null, null, null);
        Assertions.assertEquals("OPTIMIZE AGGREGATE KEY(`k1`, `k2`)", clause.toSql());
    }

    @Test
    public void testToSqlWithUniqueKey() {
        KeysDesc keysDesc = new KeysDesc(KeysType.UNIQUE_KEYS, Lists.newArrayList("uid"));
        OptimizeClause clause = new OptimizeClause(keysDesc, null, null, null, null, null);
        Assertions.assertEquals("OPTIMIZE UNIQUE KEY(`uid`)", clause.toSql());
    }

    @Test
    public void testToSqlWithSortKeys() {
        OptimizeClause clause = new OptimizeClause(null, null, null, null, null, null);
        clause.setSortKeys(Lists.newArrayList("col1", "col2"));
        Assertions.assertEquals("OPTIMIZE ORDER BY `col1`, `col2`", clause.toSql());
    }

    @Test
    public void testToSqlWithEmptySortKeys() {
        OptimizeClause clause = new OptimizeClause(null, null, null, null, null, null);
        clause.setSortKeys(Collections.emptyList());
        Assertions.assertEquals("OPTIMIZE", clause.toSql());
    }

    @Test
    public void testToSqlWithDistributionDesc() {
        HashDistributionDesc distributionDesc = new HashDistributionDesc(10, Lists.newArrayList("col1"));
        OptimizeClause clause = new OptimizeClause(null, null, distributionDesc, null, null, null);
        Assertions.assertEquals("OPTIMIZE DISTRIBUTED BY HASH(`col1`) BUCKETS 10", clause.toSql());
    }

    @Test
    public void testToSqlWithDistributionDescNoBuckets() {
        HashDistributionDesc distributionDesc = new HashDistributionDesc(-1, Lists.newArrayList("col1"));
        OptimizeClause clause = new OptimizeClause(null, null, distributionDesc, null, null, null);
        Assertions.assertEquals("OPTIMIZE DISTRIBUTED BY HASH(`col1`)", clause.toSql());
    }

    @Test
    public void testToSqlWithRandomDistributionDesc() {
        RandomDistributionDesc distributionDesc = new RandomDistributionDesc(10);
        OptimizeClause clause = new OptimizeClause(null, null, distributionDesc, null, null, null);
        Assertions.assertEquals("OPTIMIZE DISTRIBUTED BY RANDOM BUCKETS 10", clause.toSql());
    }

    @Test
    public void testToSqlWithRandomDistributionDescNoBuckets() {
        RandomDistributionDesc distributionDesc = new RandomDistributionDesc(-1);
        OptimizeClause clause = new OptimizeClause(null, null, distributionDesc, null, null, null);
        Assertions.assertEquals("OPTIMIZE DISTRIBUTED BY RANDOM", clause.toSql());
    }

    @Test
    public void testToSqlWithRange() {
        OptimizeRange range = new OptimizeRange(
                new StringLiteral("2024-01-01"),
                new StringLiteral("2024-12-31"),
                NodePosition.ZERO);
        OptimizeClause clause = new OptimizeClause(null, null, null, null, null, range);
        Assertions.assertEquals("OPTIMIZE BETWEEN 2024-01-01 AND 2024-12-31", clause.toSql());
    }

    @Test
    public void testToSqlWithRangeNullStart() {
        OptimizeRange range = new OptimizeRange(null, new StringLiteral("2024-12-31"), NodePosition.ZERO);
        OptimizeClause clause = new OptimizeClause(null, null, null, null, null, range);
        Assertions.assertEquals("OPTIMIZE BETWEEN  AND 2024-12-31", clause.toSql());
    }

    @Test
    public void testToSqlWithRangeNullEnd() {
        OptimizeRange range = new OptimizeRange(new StringLiteral("2024-01-01"), null, NodePosition.ZERO);
        OptimizeClause clause = new OptimizeClause(null, null, null, null, null, range);
        Assertions.assertEquals("OPTIMIZE BETWEEN 2024-01-01 AND ", clause.toSql());
    }

    @Test
    public void testToSqlWithKeysAndSortKeys() {
        KeysDesc keysDesc = new KeysDesc(KeysType.DUP_KEYS, Lists.newArrayList("col1"));
        OptimizeClause clause = new OptimizeClause(keysDesc, null, null, null, null, null);
        clause.setSortKeys(Lists.newArrayList("col2", "col3"));
        Assertions.assertEquals("OPTIMIZE DUPLICATE KEY(`col1`) ORDER BY `col2`, `col3`", clause.toSql());
    }

    @Test
    public void testToSqlWithKeysAndDistribution() {
        KeysDesc keysDesc = new KeysDesc(KeysType.DUP_KEYS, Lists.newArrayList("col1"));
        HashDistributionDesc distributionDesc = new HashDistributionDesc(10, Lists.newArrayList("col1"));
        OptimizeClause clause = new OptimizeClause(keysDesc, null, distributionDesc, null, null, null);
        Assertions.assertEquals(
                "OPTIMIZE DUPLICATE KEY(`col1`) DISTRIBUTED BY HASH(`col1`) BUCKETS 10",
                clause.toSql());
    }

    @Test
    public void testToSqlWithPartitionDesc() {
        RangePartitionDesc partitionDesc = new RangePartitionDesc(
                Lists.newArrayList("date_col"), Lists.newArrayList());
        OptimizeClause clause = new OptimizeClause(null, partitionDesc, null, null, null, null);
        String result = clause.toSql();
        // Verify partitionDesc output is included and starts with "PARTITION BY RANGE"
        Assertions.assertTrue(result.startsWith("OPTIMIZE PARTITION BY RANGE"),
                "Expected to start with 'OPTIMIZE PARTITION BY RANGE', but was: " + result);
    }

    @Test
    public void testToSqlWithAllFields() {
        PartitionRef partitionRef = new PartitionRef(
                Lists.newArrayList("p1"), false, NodePosition.ZERO);
        KeysDesc keysDesc = new KeysDesc(KeysType.DUP_KEYS, Lists.newArrayList("col1"));
        HashDistributionDesc distributionDesc = new HashDistributionDesc(10, Lists.newArrayList("col1"));
        OptimizeRange range = new OptimizeRange(
                new StringLiteral("2024-01-01"),
                new StringLiteral("2024-12-31"),
                NodePosition.ZERO);

        OptimizeClause clause = new OptimizeClause(
                keysDesc, null, distributionDesc, null, partitionRef, range);
        clause.setSortKeys(Lists.newArrayList("col2"));

        Assertions.assertEquals(
                "OPTIMIZE PARTITIONS (p1) DUPLICATE KEY(`col1`) ORDER BY `col2` " +
                        "DISTRIBUTED BY HASH(`col1`) BUCKETS 10 BETWEEN 2024-01-01 AND 2024-12-31",
                clause.toSql());
    }
}
