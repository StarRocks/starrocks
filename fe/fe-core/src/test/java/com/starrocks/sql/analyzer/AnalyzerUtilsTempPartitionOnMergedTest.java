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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.TableProperty;
import com.starrocks.persist.ColumnIdExpr;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.DateType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Automatic partitions created at load time must not contradict the partitions the table already has.
 *
 * <p>A table can hold partitions coarser than its own partition expression: {@code ALTER TABLE t
 * PARTITION BY date_trunc('month', dt) BETWEEN ...} merges a range of daily partitions into one
 * monthly partition and — by design, since the merge is scoped by BETWEEN — leaves the table's
 * partition expression at {@code date_trunc('day', dt)}. That state is reachable only through the
 * merge job (a table with automatic partitioning rejects a manual ADD PARTITION), so these tests
 * build it directly on the metadata.
 *
 * <p>Ordinary loads are unaffected: the sink resolves a row against the existing ranges and only
 * asks the FE for a partition when no range covers it. Dynamic overwrite is different — it creates
 * a *temporary* partition per written partition at load time and swaps it in afterwards, so a
 * temporary partition computed from the expression granularity instead of the covering partition
 * re-introduces the daily partitions the merge had removed.
 */
public class AnalyzerUtilsTempPartitionOnMergedTest {

    private static final long MERGED_PARTITION_ID = 10001L;

    /** A day-partitioned table that already holds one monthly partition, as a merge would leave it. */
    private static OlapTable mergedTable() throws Exception {
        Column dt = new Column("dt", DateType.DATE);
        List<Column> schema = Lists.newArrayList(dt);

        Expr dayExpr = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(new StringLiteral("day"), new SlotRef(null, "dt")));
        ExpressionRangePartitionInfo partitionInfo = new ExpressionRangePartitionInfo(
                Lists.newArrayList(ColumnIdExpr.create(dayExpr)), schema, PartitionType.EXPR_RANGE);

        HashDistributionInfo distributionInfo = new HashDistributionInfo(1, schema);
        OlapTable table = new OlapTable(1L, "merged_day_tbl", schema, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
        table.setTableProperty(new TableProperty(ImmutableMap.of("replication_num", "1")));

        Range<PartitionKey> january = Range.closedOpen(
                PartitionKey.createPartitionKey(
                        Collections.singletonList(new PartitionValue("2026-01-01")), schema),
                PartitionKey.createPartitionKey(
                        Collections.singletonList(new PartitionValue("2026-02-01")), schema));
        partitionInfo.setRange(MERGED_PARTITION_ID, false, january);
        partitionInfo.setReplicationNum(MERGED_PARTITION_ID, (short) 1);
        table.addPartition(new Partition(MERGED_PARTITION_ID, "p202601", distributionInfo));
        return table;
    }

    private static List<List<String>> values(String partitionValue) {
        return Collections.singletonList(Collections.singletonList(partitionValue));
    }

    private static PartitionKeyDesc onlyKeyDesc(AddPartitionClause clause) {
        RangePartitionDesc rangeDesc = (RangePartitionDesc) clause.getPartitionDesc();
        List<SingleRangePartitionDesc> descs = rangeDesc.getSingleRangePartitionDescs();
        Assertions.assertEquals(1, descs.size(), "expected exactly one partition desc");
        return descs.get(0).getPartitionKeyDesc();
    }

    /**
     * Trigger: a temporary partition for a value already covered by the merged monthly partition
     * must adopt that partition's bounds, not the daily bounds of the partition expression.
     */
    @Test
    public void tempPartitionAdoptsCoveringPartitionBounds() throws Exception {
        AddPartitionClause clause = AnalyzerUtils.getAddPartitionClauseFromPartitionValues(
                mergedTable(), values("2026-01-05"), true, "txn1");
        PartitionKeyDesc keyDesc = onlyKeyDesc(clause);
        Assertions.assertEquals("2026-01-01",
                keyDesc.getLowerValues().get(0).getStringValue(),
                "temporary partition must start where the covering partition starts");
        Assertions.assertEquals("2026-02-01",
                keyDesc.getUpperValues().get(0).getStringValue(),
                "temporary partition must end where the covering partition ends");
    }

    /**
     * Trigger: two values inside the same covering partition must collapse into one partition desc,
     * otherwise the load would ask for two temporary partitions with identical, overlapping ranges.
     */
    @Test
    public void valuesInsideOneCoveringPartitionCollapse() throws Exception {
        List<List<String>> twoDays = Lists.newArrayList(
                Collections.singletonList("2026-01-05"),
                Collections.singletonList("2026-01-06"));
        AddPartitionClause clause = AnalyzerUtils.getAddPartitionClauseFromPartitionValues(
                mergedTable(), twoDays, true, "txn1");
        RangePartitionDesc rangeDesc = (RangePartitionDesc) clause.getPartitionDesc();
        Assertions.assertEquals(1, rangeDesc.getSingleRangePartitionDescs().size(),
                "both values fall in the same covering partition, so one desc is expected");
    }

    /** Control: a value outside every existing partition still follows the partition expression. */
    @Test
    public void tempPartitionOutsideCoverageKeepsExpressionGranularity() throws Exception {
        AddPartitionClause clause = AnalyzerUtils.getAddPartitionClauseFromPartitionValues(
                mergedTable(), values("2026-03-07"), true, "txn1");
        PartitionKeyDesc keyDesc = onlyKeyDesc(clause);
        Assertions.assertEquals("2026-03-07", keyDesc.getLowerValues().get(0).getStringValue());
        Assertions.assertEquals("2026-03-08", keyDesc.getUpperValues().get(0).getStringValue());
    }

    /** Control: ordinary (non-temporary) automatic partitioning is untouched by this rule. */
    @Test
    public void ordinaryAutoPartitionKeepsExpressionGranularity() throws Exception {
        AddPartitionClause clause = AnalyzerUtils.getAddPartitionClauseFromPartitionValues(
                mergedTable(), values("2026-01-05"), false, null);
        PartitionKeyDesc keyDesc = onlyKeyDesc(clause);
        Assertions.assertEquals("2026-01-05", keyDesc.getLowerValues().get(0).getStringValue());
        Assertions.assertEquals("2026-01-06", keyDesc.getUpperValues().get(0).getStringValue());
    }
}
