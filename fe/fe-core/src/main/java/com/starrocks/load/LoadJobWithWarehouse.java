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

package com.starrocks.load;

public interface LoadJobWithWarehouse {
    String getCurrentWarehouse();

    boolean isFinal();

    default boolean isInternalJob() {
        return false;
    }

<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/load/LoadJobWithWarehouse.java
    long getFinishTimestampMs();
=======
    @Test
    public void testUnionUnGather() {
        runFileUnitTest("scheduler/union/union_ungather");
    }

    @Test
    public void test() throws Exception {
        String sql = "select met1.* from (\n" +
                "  with denominator as (select 1 as num),\n" +
                "  numerator as (select 1 as num)\n" +
                "  SELECT ROUND((SELECT COUNT(*) * 100 FROM numerator)\n" +
                "       / (SELECT COUNT(*)  FROM denominator), 2)\n" +
                "       AS measure_score\n" +
                "       , (SELECT COUNT(*) FROM numerator) AS numerator_count\n" +
                "       , (SELECT COUNT(*)  FROM denominator) AS denominator_count\n" +
                ") as met1\n" +
                "union all\n" +
                "select met2.* from (\n" +
                "  with denominator as (select 1 as num),\n" +
                "  numerator as (select 1 as num)\n" +
                "  SELECT ROUND((SELECT COUNT(*) * 100 FROM numerator)\n" +
                "       / (SELECT COUNT(*)  FROM denominator), 2)\n" +
                "       AS measure_score\n" +
                "       , (SELECT COUNT(*) FROM numerator) AS numerator_count\n" +
                "       , (SELECT COUNT(*)  FROM denominator) AS denominator_count\n" +
                ") as met2\n" +
                "union all\n" +
                "select met3.* from (\n" +
                "  with denominator as (select 1 as num),\n" +
                "  numerator as (select 1 as num)\n" +
                "  SELECT ROUND((SELECT COUNT(*) * 100 FROM numerator)\n" +
                "       / (SELECT COUNT(*)  FROM denominator), 2)\n" +
                "       AS measure_score\n" +
                "       , (SELECT COUNT(*) FROM numerator) AS numerator_count\n" +
                "       , (SELECT COUNT(*)  FROM denominator) AS denominator_count\n" +
                ") as met3\n" +
                "union all\n" +
                "select met4.* from (\n" +
                "  with denominator as (select 1 as num),\n" +
                "  numerator as (select 1 as num)\n" +
                "  SELECT ROUND((SELECT COUNT(*) * 100 FROM numerator)\n" +
                "       / (SELECT COUNT(*)  FROM denominator), 2)\n" +
                "       AS measure_score\n" +
                "       , (SELECT COUNT(*) FROM numerator) AS numerator_count\n" +
                "       , (SELECT COUNT(*)  FROM denominator) AS denominator_count\n" +
                ") as met4\n" +
                "union all\n" +
                "select met5.* from (\n" +
                "  with denominator as (select 1 as num),\n" +
                "  numerator as (select 1 as num)\n" +
                "  SELECT ROUND((SELECT COUNT(*) * 100 FROM numerator)\n" +
                "       / (SELECT COUNT(*)  FROM denominator), 2)\n" +
                "       AS measure_score\n" +
                "       , (SELECT COUNT(*) FROM numerator) AS numerator_count\n" +
                "       , (SELECT COUNT(*)  FROM denominator) AS denominator_count\n" +
                ") as met5\n" +
                "union all\n" +
                "select met6.* from (\n" +
                "  with denominator as (select 1 as num),\n" +
                "  numerator as (select 1 as num)\n" +
                "  SELECT ROUND((SELECT COUNT(*) * 100 FROM numerator)\n" +
                "       / (SELECT COUNT(*)  FROM denominator), 2)\n" +
                "       AS measure_score\n" +
                "       , (SELECT COUNT(*) FROM numerator) AS numerator_count\n" +
                "       , (SELECT COUNT(*)  FROM denominator) AS denominator_count\n" +
                ") as met6\n" +
                "union all\n" +
                "select met7.* from (\n" +
                "  with denominator as (select 1 as num),\n" +
                "  numerator as (select 1 as num)\n" +
                "  SELECT ROUND((SELECT COUNT(*) * 100 FROM numerator)\n" +
                "       / (SELECT COUNT(*)  FROM denominator), 2)\n" +
                "       AS measure_score\n" +
                "       , (SELECT COUNT(*) FROM numerator) AS numerator_count\n" +
                "       , (SELECT COUNT(*)  FROM denominator) AS denominator_count\n" +
                ") as met7";
        getFragmentPlan(sql);
    }
>>>>>>> 4ff80a2e09 ([BugFix] Fix NPE when cte forced inlined (#40550)):fe/fe-core/src/test/java/com/starrocks/qe/scheduler/plan/UnionTest.java
}
