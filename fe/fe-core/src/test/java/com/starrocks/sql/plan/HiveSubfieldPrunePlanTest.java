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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class HiveSubfieldPrunePlanTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @Test
    public void testMapReturningExprKeepsOtherColumnsPrunable() throws Exception {
        // The lambda inside map_filter, and a CASE WHEN whose branches return MAP, are expressions the
        // subfield visitor cannot reason about. They make col_map be read whole, which is expected, but
        // they must not stop col_struct from being pruned down to the single field the query reads.
        String sql = "select map_filter((k, v) -> k = 1, col_map), col_struct.c0 "
                + "from hive0.subfield_db.subfield_map";
        assertContains(getVerboseExplain(sql), "[col_struct] <-> [struct<`c0` int(11)>]");

        sql = "select case when col_int = 1 then map_filter((k, v) -> k = 1, col_map) else col_map end, "
                + "col_struct.c0 from hive0.subfield_db.subfield_map";
        assertContains(getVerboseExplain(sql), "[col_struct] <-> [struct<`c0` int(11)>]");

        // map_values pushes MAP_VALUE before descending into the unknown expression. That path belongs
        // to map_filter's result, not to col_map, so col_map must keep both its keys and its values.
        sql = "select map_values(map_filter((k, v) -> k = 1, col_map)), col_struct.c0 "
                + "from hive0.subfield_db.subfield_map";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "[col_struct] <-> [struct<`c0` int(11)>]");
        assertContains(plan, "[col_map] <-> [MAP<INT,INT>]");
    }

    @Test
    public void testUnknownExprDoesNotInheritDownstreamPath() throws Exception {
        // A lower operator defines m = map_filter(lambda, col_map) and an upper one reads map_values(m).
        // The MAP_VALUE path belongs to map_filter's result, not to col_map: attributing it would drop
        // col_map's keys, which the lambda evaluates. Here the join keeps the two in separate operators,
        // so the path arrives through the visited access group rather than through the local path stack.
        String sql = "select map_values(t.m) from "
                + "(select col_int, map_filter((k, v) -> k = 1, col_map) as m from hive0.subfield_db.subfield_map) t "
                + "join (select col_int as ci from hive0.subfield_db.subfield_map) s on t.col_int = s.ci";
        assertContains(getVerboseExplain(sql), "[col_map] <-> [MAP<INT,INT>]");

        // Same shape across an aggregate, and with only map_keys downstream.
        sql = "with t as (select col_int, map_filter((k, v) -> k = 1, col_map) as m "
                + "from hive0.subfield_db.subfield_map) "
                + "select map_keys(t.m), s.col_int from t join (select col_int from hive0.subfield_db.subfield_map) s "
                + "on t.col_int = s.col_int";
        assertContains(getVerboseExplain(sql), "[col_map] <-> [MAP<INT,INT>]");
    }

    @Test
    public void testArrayReturningExprIsAPruneBoundary() throws Exception {
        // An outer subfield access describes array_map's result, not col_arr: select
        // array_filter(x -> x.user = 'official', name)[1].family must not read name as struct<family>, or the
        // BE fails evaluating x.user. col_arr is narrowed to what the lambda reads (x.c1) and nothing else.
        String sql = "select array_map((x) -> named_struct('c0', x.c1), col_arr)[1].c0 "
                + "from hive0.subfield_db.subfield_map";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "[col_arr] <-> [ARRAY<struct<`c1` int(11)>>]");

        // Same shape with the subfield access in a different operator, so the path arrives through the
        // visited access group instead of the local path stack.
        sql = "select t.a[1].c0 from "
                + "(select array_map((x) -> named_struct('c0', x.c1), col_arr) as a, col_int "
                + "from hive0.subfield_db.subfield_map) t "
                + "join (select col_int as ci from hive0.subfield_db.subfield_map) s on t.col_int = s.ci";
        plan = getVerboseExplain(sql);
        assertContains(plan, "[col_arr] <-> [ARRAY<struct<`c1` int(11)>>]");
    }

    @Test
    public void testArrayMapLambdaSubfieldsPruneTheArray() throws Exception {
        // The lambda argument is bound to the array's elements, so its subfield reads are the array's.
        String sql = "select array_sum(array_map(x -> x.c1, col_arr)) from hive0.subfield_db.subfield_map";
        assertContains(getVerboseExplain(sql), "[col_arr] <-> [ARRAY<struct<`c1` int(11)>>]");

        // The element used whole, or the array also read directly, keeps every subfield.
        String full = "[col_arr] <-> [ARRAY<struct<`c0` int(11), `c1` int(11)>>]";
        // (x -> x alone is folded to the array itself, which [1].c0 then prunes legitimately.)
        sql = "select array_map(x -> named_struct('s', x), col_arr)[1].s.c0 from hive0.subfield_db.subfield_map";
        assertContains(getVerboseExplain(sql), full);
        sql = "select array_sum(array_map(x -> x.c1, col_arr)), col_arr from hive0.subfield_db.subfield_map";
        assertContains(getVerboseExplain(sql), full);

        // Nested lambdas: the inner lambda's paths reach the outer array through x.items, and the inner
        // lambda's reads of the outer argument count too.
        sql = "select array_map(x -> array_sum(array_map(y -> y.a, x.items)), col_nested) "
                + "from hive0.subfield_db.subfield_map";
        assertContains(getVerboseExplain(sql),
                "[col_nested] <-> [ARRAY<struct<`items` array<struct<`a` int(11)>>>>]");
        sql = "select array_map(x -> array_sum(array_map(y -> y.a + x.c0, x.items)), col_nested) "
                + "from hive0.subfield_db.subfield_map";
        assertContains(getVerboseExplain(sql),
                "[col_nested] <-> [ARRAY<struct<`c0` int(11), `items` array<struct<`a` int(11)>>>>]");

        // An array produced by a function may reshape its elements: map_entries yields struct<key, value>
        // elements whose paths are not paths of col_map_struct, and array_filter's lambda reads x.c0. Such
        // arrays keep their columns whole, and planning must not fail.
        String fullMap = "[col_map_struct] <-> [MAP<INT,struct<`a` int(11), `b` int(11)>>]";
        sql = "select array_map(e -> e.value.a, map_entries(col_map_struct)) from hive0.subfield_db.subfield_map";
        assertContains(getVerboseExplain(sql), fullMap);
        sql = "select array_map(x -> x.c1, array_filter(x -> x.c0 > 0, col_arr)) from hive0.subfield_db.subfield_map";
        assertContains(getVerboseExplain(sql), full);

        // Two reads of the same array union their subfields.
        sql = "select array_sum(array_map(x -> x.c1, col_arr)), array_sum(array_map(x -> x.c0, col_arr)) "
                + "from hive0.subfield_db.subfield_map";
        assertContains(getVerboseExplain(sql), full);
    }

    @Test
    public void testArrayMapLambdaPruneAcrossProjections() throws Exception {
        // The scan column itself, separated by a join, still prunes: it is the same column everywhere.
        String sql = "select array_sum(array_map(x -> x.c1, t.a)) from "
                + "(select col_arr as a, col_int from hive0.subfield_db.subfield_map) t "
                + "join (select col_int as ci from hive0.subfield_db.subfield_map) s on t.col_int = s.ci";
        assertContains(getVerboseExplain(sql), "[col_arr] <-> [ARRAY<struct<`c1` int(11)>>]");

        // A struct subfield computed by a lower projection is a new column that keeps its full declared type,
        // and it crosses the shuffle exchange: narrowing the scan column under it would make the two disagree.
        sql = "select array_map(x -> x.a, t.arr) from (select col_int as ci from hive0.subfield_db.subfield_map) s "
                + "join [shuffle] (select col_int, col_struct_arr.items as arr from hive0.subfield_db.subfield_map) t "
                + "on t.col_int = s.ci";
        assertContains(getVerboseExplain(sql),
                "[col_struct_arr] <-> [struct<`items` array<struct<`a` int(11), `b` int(11)>>>]");
        // Taken in the same projection, the subfield chain is on the scan column and prunes.
        sql = "select array_map(x -> x.a, col_struct_arr.items) from hive0.subfield_db.subfield_map";
        assertContains(getVerboseExplain(sql), "[col_struct_arr] <-> [struct<`items` array<struct<`a` int(11)>>>]");

        // The lower projection computes the array with a function: the column looks like a plain column ref
        // here, but its element paths are not paths of the scan column, so it is read in full.
        sql = "select array_map(e -> e.value.a, t.entries) from "
                + "(select col_int, map_entries(col_map_struct) as entries from hive0.subfield_db.subfield_map) t "
                + "join (select col_int as ci from hive0.subfield_db.subfield_map) s on t.col_int = s.ci";
        assertContains(getVerboseExplain(sql), "[col_map_struct] <-> [MAP<INT,struct<`a` int(11), `b` int(11)>>]");
        sql = "select array_map(x -> x.c1, t.a) from "
                + "(select col_int, array_filter(x -> x.c0 > 0, col_arr) as a from hive0.subfield_db.subfield_map) t "
                + "join (select col_int as ci from hive0.subfield_db.subfield_map) s on t.col_int = s.ci";
        assertContains(getVerboseExplain(sql), "[col_arr] <-> [ARRAY<struct<`c0` int(11), `c1` int(11)>>]");
    }

    @Test
    public void testCollectionIndexDoesNotTakeTheElementPath() throws Exception {
        // `.a` leads into the element of items, not into the index expression's col.other; planning used to fail
        // looking up `a` in other's element type.
        String sql = "select col_two_arrs[1].items[array_length(col_two_arrs[1].other)].a "
                + "from hive0.subfield_db.subfield_map";
        assertContains(getVerboseExplain(sql), "[col_two_arrs] <-> [ARRAY<struct<`items` array<struct<`a` int(11)>>, "
                + "`other` array<struct<`c` int(11), `d` int(11)>>>>]");
        // same inside an array_map lambda, whose paths now reach the scan column
        sql = "select array_map(x -> x.items[array_length(x.other)].a, col_two_arrs) "
                + "from hive0.subfield_db.subfield_map";
        assertContains(getVerboseExplain(sql), "[col_two_arrs] <-> [ARRAY<struct<`items` array<struct<`a` int(11)>>, "
                + "`other` array<struct<`c` int(11), `d` int(11)>>>>]");
    }

    @Test
    public void testMapEntriesElementPathReadsTheWholeMap() throws Exception {
        // A path into map_entries' struct<key, value> elements is not a path into the map; planning used to
        // fail looking up `value` in the map's value struct.
        String fullMap = "[col_map_struct] <-> [MAP<INT,struct<`a` int(11), `b` int(11)>>]";
        String sql = "select map_entries(col_map_struct)[1].value.a from hive0.subfield_db.subfield_map";
        assertContains(getVerboseExplain(sql), fullMap);
        // Same with the element path in an upper operator.
        sql = "select t.e[1].value.a from "
                + "(select col_int, map_entries(col_map_struct) as e from hive0.subfield_db.subfield_map) t "
                + "join (select col_int as ci from hive0.subfield_db.subfield_map) s on t.col_int = s.ci";
        assertContains(getVerboseExplain(sql), fullMap);
    }

    @Test
    public void testAggCTE() throws Exception {
        String sql = "with stream as (select array_agg(col_struct.c0) as t1 from hive0.subfield_db.subfield group by " +
                "col_int) select t1 from stream";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "Pruned type: 2 [col_struct] <-> [struct<`c0` int(11)>]");
    }
}
