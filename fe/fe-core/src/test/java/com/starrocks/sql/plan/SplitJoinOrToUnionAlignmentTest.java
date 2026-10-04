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

package com.starrocks.sql.plan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SplitJoinORToUnionRule pairs the union's branches by position, so all branches must be listed in
 * one consistent order. The union's output columns used to come from a ColumnRefSet (column-id
 * order) while the non-first branches came from RowOutputInfo, which for a projected operator
 * follows the projection map's iteration order. ColumnRefOperator hashes on its id, so those two
 * orders agree only while the ids stay inside one hash bucket range: ids {15,16,17,18} iterate as
 * 16,17,18,15 and the branches rotate against each other.
 *
 * <p>Sweeping the column-id offset shows the failure recurring with period 16. With mixed column
 * types the type checker rejects the plan ("input cols type not equal with output cols type"); with
 * uniform types nothing notices and the branches silently swap values.
 */
public class SplitJoinOrToUnionAlignmentTest extends PlanTestBase {

    private static final Pattern EXPR_TYPE = Pattern.compile("\\[\\d+:?[^,\\]]*,\\s*([A-Z0-9()]+),");

    private String padColumns(int pad) {
        StringBuilder sb = new StringBuilder();
        for (int p = 0; p < pad; p++) {
            sb.append(", v1 + ").append(p).append(" as p").append(p);
        }
        return sb.toString();
    }

    /** Type sequence of each "child exprs:" row of the first UNION in a verbose plan. */
    private List<List<String>> unionChildTypes(String plan) {
        List<List<String>> rows = new ArrayList<>();
        boolean inChild = false;
        for (String line : plan.split("\n")) {
            if (line.contains("child exprs:")) {
                inChild = true;
                continue;
            }
            if (inChild) {
                List<String> types = new ArrayList<>();
                Matcher m = EXPR_TYPE.matcher(line);
                while (m.find()) {
                    types.add(m.group(1));
                }
                if (types.isEmpty()) {
                    break;
                }
                rows.add(types);
            }
        }
        return rows;
    }

    @Test
    public void unionBranchesStayAlignedAcrossColumnIdOffsets() throws Exception {
        boolean old = connectContext.getSessionVariable().isEnabledRewriteOrToUnionAllJoin();
        connectContext.getSessionVariable().setEnabledRewriteOrToUnionAllJoin(true);
        try {
            for (int pad = 0; pad <= 28; pad++) {
                String sql = "select cast(x.v3 as varchar) c3, t1.v6 c6, x.v1 + 1 c1, cast(t1.v4 as varchar) c4 "
                        + "from (select v1, v2, v3" + padColumns(pad) + " from t0) x "
                        + "join t1 on x.v1 = t1.v4 or x.v2 = t1.v5";
                String plan = getVerboseExplain(sql);
                List<List<String>> rows = unionChildTypes(plan);
                Assertions.assertTrue(rows.size() >= 2, "pad=" + pad + ": expected a union with 2 branches\n" + plan);
                for (int i = 1; i < rows.size(); i++) {
                    Assertions.assertEquals(rows.get(0), rows.get(i),
                            "pad=" + pad + ": union branch " + i + " is not aligned with branch 0\n" + plan);
                }
            }
        } finally {
            connectContext.getSessionVariable().setEnabledRewriteOrToUnionAllJoin(old);
        }
    }

    /**
     * Same sweep with uniform column types, where only this assertion can see the rotation. Each
     * column is a distinct constant offset; the projections say which slot holds which offset, so the
     * union's branches can be compared semantically rather than by type.
     */
    @Test
    public void uniformTypeBranchesKeepTheSameColumnOrder() throws Exception {
        boolean old = connectContext.getSessionVariable().isEnabledRewriteOrToUnionAllJoin();
        connectContext.getSessionVariable().setEnabledRewriteOrToUnionAllJoin(true);
        try {
            for (int pad = 6; pad <= 11; pad++) {
                String sql = "select x.v3 + 1000 c3, t1.v6 + 2000 c6, x.v1 + 3000 c1, t1.v4 + 4000 c4 "
                        + "from (select v1, v2, v3" + padColumns(pad) + " from t0) x "
                        + "join t1 on x.v1 = t1.v4 or x.v2 = t1.v5";
                String plan = getVerboseExplain(sql);

                // slot id -> the constant offset that slot computes, e.g. "27 <-> [18: v1, ...] + 3000"
                Map<String, String> slotToOffset = new HashMap<>();
                Matcher def = Pattern.compile("(\\d+) <-> \\[[^\\]]+\\] \\+ (1000|2000|3000|4000)").matcher(plan);
                while (def.find()) {
                    slotToOffset.put(def.group(1), def.group(2));
                }

                List<List<String>> branches = new ArrayList<>();
                boolean inChild = false;
                for (String line : plan.split("\n")) {
                    if (line.contains("child exprs:")) {
                        inChild = true;
                        continue;
                    }
                    if (inChild) {
                        List<String> offsets = new ArrayList<>();
                        Matcher m = Pattern.compile("\\[(\\d+): expr").matcher(line);
                        while (m.find()) {
                            offsets.add(slotToOffset.getOrDefault(m.group(1), "?" + m.group(1)));
                        }
                        if (offsets.isEmpty()) {
                            break;
                        }
                        branches.add(offsets);
                    }
                }
                Assertions.assertTrue(branches.size() >= 2, "pad=" + pad + ": expected 2 union branches\n" + plan);
                for (int i = 1; i < branches.size(); i++) {
                    Assertions.assertEquals(branches.get(0), branches.get(i),
                            "pad=" + pad + ": union branch " + i + " carries the columns in a different order"
                                    + " -- the branches would swap values\n" + plan);
                }
            }
        } finally {
            connectContext.getSessionVariable().setEnabledRewriteOrToUnionAllJoin(old);
        }
    }
}
