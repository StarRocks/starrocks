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

package com.starrocks.analysis;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

import static com.starrocks.common.InvertedIndexParams.CommonIndexParamKey.IMP_LIB;

import com.starrocks.analysis.IndexDef.IndexType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableIndexes;
import com.starrocks.catalog.Type;
import com.starrocks.common.InvertedIndexParams.IndexParamsKey;
import com.starrocks.common.InvertedIndexParams.InvertedIndexImpType;
import com.starrocks.common.util.IndexUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class GINIndexTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();

        starRocksAssert.withTable("CREATE TABLE `test_index_tbl` (\n" +
                "  `f1` int NOT NULL COMMENT \"\",\n" +
                "  `f2` string NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`f1`)\n" +
                "DISTRIBUTED BY HASH(`f1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("test_index_tbl");
    }

    @Test
    public void testCreateIndex() {
        IndexDef indexDef = new IndexDef("inverted_index", Collections.singletonList("f1"), IndexType.GIN, "",
                new HashMap<String, String>() {{
                    put("tmp_test_param", "1");
                }});
        Table table = connectContext.getGlobalStateMgr().getDb("test").getTable("test_index_tbl");
        Index indexFromDef = IndexFactory.createIndexFromDef(table, indexDef);
        Assertions.assertEquals(indexFromDef.getIndexId(), 0);
        Assertions.assertEquals(indexFromDef.getIndexName(), indexDef.getIndexName());
        Assertions.assertEquals(indexFromDef.getIndexType(), indexDef.getIndexType());
        Assertions.assertEquals(indexFromDef.getColumns(), indexDef.getColumns());
        Assertions.assertEquals(indexFromDef.getProperties(), indexDef.getProperties());
        Assertions.assertEquals(indexFromDef.getComment(), indexDef.getComment());
    }

    @Test
    public void testCheckInvertedIndex() {
        Column c1 = new Column("f1", Type.ARRAY_FLOAT, true);

        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexUtil.checkInvertedIndexValid(c1, null, KeysType.PRIMARY_KEYS),
                "The inverted index can only be build on DUPLICATE table.");

        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexUtil.checkInvertedIndexValid(c1, null, KeysType.DUP_KEYS),
                "The inverted index can only be build on column with type of scalar type.");

        Column c2 = new Column("f2", Type.STRING, true);
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexUtil.checkInvertedIndexValid(c2, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), "???");
                }}, KeysType.DUP_KEYS),
                "Only support clucene implement for now");

        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexUtil.checkInvertedIndexValid(c2, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
                    put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, "french");
                }}, KeysType.DUP_KEYS));

        Column c3 = new Column("f3", Type.FLOAT, true);
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexUtil.checkInvertedIndexValid(c3, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
                    put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, InvertedIndexUtil.INVERTED_INDEX_PARSER_CHINESE);
                }}, KeysType.DUP_KEYS));

        Assertions.assertDoesNotThrow(
                () -> IndexUtil.checkInvertedIndexValid(c2, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
                    put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, InvertedIndexUtil.INVERTED_INDEX_PARSER_CHINESE);
                    put(IndexParamsKey.OMIT_TERM_FREQ_AND_POSITION.name().toLowerCase(Locale.ROOT), "true");
                    put(IndexParamsKey.COMPOUND_FORMAT.name().toLowerCase(Locale.ROOT), "false");
                }}, KeysType.DUP_KEYS));
    }

    @Test
    public void testCreateIndexFromStmt() {
        Column c1 = new Column("f1", Type.STRING, true);
        Column c2 = new Column("f2", Type.INT, true);

        Index index1 = new Index("idx1", Collections.singletonList(c1.getName()), IndexType.GIN, "", Collections.emptyMap());
        Index index2 = new Index("idx2", Collections.singletonList(c2.getName()), IndexType.GIN, "", Collections.emptyMap());

        OlapTable table = (OlapTable) connectContext.getGlobalStateMgr().getDb("test").getTable("test_index_tbl");

        Assertions.assertTrue(IndexFactory.createIndexesFromCreateStmt(Collections.emptyList(), table).getIndexes().isEmpty());

        index1.setIndexId(100);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> IndexFactory.createIndexesFromCreateStmt(Arrays.asList(index1, index2), null),
                "All status of index indexId should be consistent");
        index1.setIndexId(-1);


        TableIndexes tableIndexes =
                Assertions.assertDoesNotThrow(() -> IndexFactory.createIndexesFromCreateStmt(Arrays.asList(index1, index2), null));
        Assertions.assertTrue(tableIndexes.getIndexes().stream().allMatch(index -> index.getIndexId() >= 0));
        index1.setIndexId(-1);
        index2.setIndexId(-1);

        table.setMaxIndexId(10);
        IndexFactory.createIndexesFromCreateStmt(Arrays.asList(index1, index2), table);
        Assertions.assertTrue(tableIndexes.getIndexes().stream().allMatch(index -> index.getIndexId() >= 10));
        table.setMaxIndexId(-1);
    }


}
