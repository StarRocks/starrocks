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

import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.util.Utility;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.DEFAULT_CREATE_TABLE_TEMPLATE;

public class PushDownHeavyExprsTest {
    protected static ConnectContext ctx;
    protected static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase(StatsConstants.STATISTICS_DB_NAME).useDatabase(StatsConstants.STATISTICS_DB_NAME)
                .withTable(DEFAULT_CREATE_TABLE_TEMPLATE);
        starRocksAssert.withDatabase("test").useDatabase("test");
        Utility.getClickBenchCreateTableSqlList().forEach(createTblSql -> {
            try {
                starRocksAssert.withTable(createTblSql);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        String tblSql = "CREATE TABLE IF NOT EXISTS tbl_transaction_001 (\n" +
                "    id              BIGINT          COMMENT 'Primary Key',\n" +
                "    field_date_1    DATE            COMMENT 'Date Field 1',\n" +
                "    field_int_1     INT             COMMENT 'Integer Field 1',\n" +
                "    field_varchar_1 VARCHAR(100)    COMMENT 'Varchar Field 1',\n" +
                "    field_varchar_2 VARCHAR(50)     COMMENT 'Varchar Field 2 (Category)',\n" +
                "    field_varchar_3 VARCHAR(100)    COMMENT 'Varchar Field 3 (Foreign Key)',\n" +
                "    field_text_1    VARCHAR(2000)   COMMENT 'Text Field 1 (JSON Data)'\n" +
                ")\n" +
                "COMMENT 'Test Table: Generic Transaction Table'\n" +
                "PROPERTIES('replication_num'='1');";

        starRocksAssert.withTable(tblSql);
        ctx.getSessionVariable().setEnableGlobalLateMaterialization(false);
    }

    @Test
    public void test() throws Exception {
        String q =
                Utility.getClickBenchQueryList().stream()
                        .filter(p -> p.first.equals("Q29")).findFirst().get().second;
        String plan = UtFrameUtils.getFragmentPlan(ctx, q);
        Assertions.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: hits\n" + "     heavy exprs: \n" +
                "          <slot 106> : regexp_replace(15: Referer, '^https?://(?:www.)?([^/]+)/.*$', '1')"), plan);

        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, q);
        System.out.println(plan);
        Assertions.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     table: hits, rollup: hits\n" +
                "     heavy exprs: \n" +
                "          106 <-> regexp_replace[([15: Referer, VARCHAR(65533), false], " +
                "'^https?://(?:www.)?([^/]+)/.*$', '1'); args: VARCHAR,VARCHAR,VARCHAR; " +
                "result: VARCHAR; args nullable: false; result nullable: true]\n"), plan);
    }

    @Test
    public void testComplexSql() throws Exception {
        String q = "SELECT CASE\n" +
                "        /* Condition A */\n" +
                "        WHEN field_varchar_2 = 'VALUE_ALPHA'\n" +
                "             AND COALESCE(\n" +
                "                    NULLIF(NULLIF(field_varchar_3, ''), '0'),\n" +
                "                    ''\n" +
                "                ) NOT IN ('', '0')\n" +
                "             AND COALESCE(\n" +
                "                    NULLIF(\n" +
                "                        GET_JSON_OBJECT(field_text_1, '$.key_json_1'),\n" +
                "                        '0'\n" +
                "                    ),\n" +
                "                    ''\n" +
                "                ) <> ''\n" +
                "             AND REGEXP_REPLACE(\n" +
                "                    GET_JSON_OBJECT(field_text_1, '$.key_json_2'),\n" +
                "                    '^\\\\[\\\\\"|\\\\\"]$',\n" +
                "                    ''\n" +
                "                 ) LIKE '%pattern_match_1%'\n" +
                "        THEN GET_JSON_OBJECT(field_text_1, '$.key_json_1')\n" +
                "\n" +
                "        /* Condition B */\n" +
                "        WHEN COALESCE(\n" +
                "                    NULLIF(\n" +
                "                        GET_JSON_OBJECT(field_text_1, '$.key_json_3'),\n" +
                "                        '0'\n" +
                "                    ),\n" +
                "                    ''\n" +
                "                ) <> ''\n" +
                "             AND REGEXP_REPLACE(\n" +
                "                    GET_JSON_OBJECT(field_text_1, '$.key_json_2'),\n" +
                "                    '^\\\\[\\\\\"|\\\\\"]$',\n" +
                "                    ''\n" +
                "                 ) LIKE '%pattern_match_2%'\n" +
                "        THEN GET_JSON_OBJECT(field_text_1, '$.key_json_3')\n" +
                "\n" +
                "        /* Default Value */\n" +
                "        ELSE '0'\n" +
                "    END AS result_field_1\n" +
                "FROM tbl_transaction_001\n" +
                "WHERE field_date_1 BETWEEN '2025-12-23' AND '2025-12-25'\n" +
                "  AND field_int_1 = 1\n" +
                "  /* Filter: valid foreign_key and category in specific list */\n" +
                "  AND IF(\n" +
                "        COALESCE(\n" +
                "            NULLIF(NULLIF(field_varchar_3, ''), '0'),\n" +
                "            ''\n" +
                "        ) NOT IN ('', '0')\n" +
                "        AND field_varchar_2 IN ('VALUE_ALPHA', 'VALUE_BETA'),\n" +
                "        1,\n" +
                "        0\n" +
                "    ) = 1;";
        String plan = UtFrameUtils.getFragmentPlan(ctx, q);
        Assertions.assertTrue(plan.contains("1:Project\n" +
                "  |  <slot 8> : CASE WHEN (((5: field_varchar_2 = 'VALUE_ALPHA') AND " +
                "(coalesce(nullif(nullif(6: field_varchar_3, ''), '0'), '') NOT IN ('', '0'))) AND " +
                "(coalesce(nullif(9: get_json_object, '0'), '') != '')) AND " +
                "(12: regexp_replace LIKE '%pattern_match_1%') THEN 9: get_json_object WHEN " +
                "(coalesce(nullif(11: get_json_object, '0'), '') != '') AND " +
                "(12: regexp_replace LIKE '%pattern_match_2%') THEN 11: get_json_object ELSE '0' END\n" +
                "  |  common expressions:\n" +
                "  |  <slot 9> : get_json_object(7: field_text_1, '$.key_json_1')\n" +
                "  |  <slot 10> : get_json_object(7: field_text_1, '$.key_json_2')\n" +
                "  |  <slot 11> : get_json_object(7: field_text_1, '$.key_json_3')\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: tbl_transaction_001\n" +
                "     heavy exprs: \n" +
                "          <slot 12> : regexp_replace(get_json_object(7: field_text_1, '$.key_json_2'), " +
                "'^\\\\[\\\\\"|\\\\\"]$', '')\n"), plan);
    }

    @Test
    public void testTopNFilterProbeOnHeavyExprSlot() throws Exception {
        // ORDER BY a heavy expr + LIMIT puts a TopN runtime filter probe on the heavy-expr
        // slot, which has no backing column. Serializing the scan node used to NPE in
        // assignOrderByHints; only toThrift takes that path, so plan the query to thrift.
        String q = "SELECT regexp_replace(field_varchar_1, 'x', 'y') AS s " +
                "FROM tbl_transaction_001 ORDER BY s LIMIT 3";
        String plan = UtFrameUtils.getPlanThriftString(ctx, q);
        Assertions.assertTrue(plan.contains("SORT_NODE"), plan);
    }

    /**
     * Heavy exprs reach the BE in TPlanNodeCommon.heavy_exprs, a map keyed by slot id, and
     * ScanNode materializes them in ascending slot-id order without honouring dependencies. A
     * heavy expr that references another heavy expr's output slot therefore reads a column the
     * chunk does not carry yet, which surfaces either as an FE "Cannot convert ColumnRefOperator
     * to Expr" or as a BE "Expr evaluate meet error: slot_id N not found", depending on which
     * entry the extraction happens to visit first. Nesting one regexp inside another produces
     * exactly that pair: the inner call becomes a heavy common sub-expression and the outer one
     * references it by slot.
     */
    @Test
    public void testHeavyExprsNeverReferenceEachOther() throws Exception {
        String q = "SELECT regexp_replace(regexp_replace(field_varchar_1, '^www\\\\.', ''), 'a', 'b') AS x, " +
                "regexp_replace(field_varchar_1, '^www\\\\.', '') AS y " +
                "FROM tbl_transaction_001";
        Pair<String, ExecPlan> pair = UtFrameUtils.getPlanAndFragment(ctx, q);
        List<String> violations = new ArrayList<>();
        for (PlanFragment fragment : pair.second.getFragments()) {
            collectHeavyExprViolations(fragment.getPlanRoot(), violations);
        }
        Assertions.assertTrue(violations.isEmpty(), String.valueOf(violations));
    }

    private static void collectHeavyExprViolations(PlanNode node, List<String> violations) {
        if (node instanceof ScanNode) {
            Map<SlotId, Expr> heavyExprs = ((ScanNode) node).getHeavyExprs();
            if (heavyExprs != null && !heavyExprs.isEmpty()) {
                Set<Integer> heavySlots = new HashSet<>();
                heavyExprs.keySet().forEach(id -> heavySlots.add(id.asInt()));
                for (Map.Entry<SlotId, Expr> entry : heavyExprs.entrySet()) {
                    List<SlotRef> refs = new ArrayList<>();
                    entry.getValue().collect(SlotRef.class, refs);
                    for (SlotRef ref : refs) {
                        int referenced = ref.getSlotId().asInt();
                        if (referenced != entry.getKey().asInt() && heavySlots.contains(referenced)) {
                            violations.add("heavy expr on slot " + entry.getKey().asInt()
                                    + " references heavy slot " + referenced);
                        }
                    }
                }
            }
        }
        for (PlanNode child : node.getChildren()) {
            collectHeavyExprViolations(child, violations);
        }
    }
}
