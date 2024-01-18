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

import com.starrocks.analysis.IndexDef.IndexType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.Type;
import com.starrocks.common.InvertedIndexParams;
import com.starrocks.common.InvertedIndexParams.CommonIndexParamKey;
import com.starrocks.common.InvertedIndexParams.IndexParamsKey;
import com.starrocks.common.InvertedIndexParams.InvertedIndexImpType;
import com.starrocks.common.InvertedIndexParams.SearchParamsKey;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TIndexType;
import com.starrocks.thrift.TOlapTableIndex;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.starrocks.common.InvertedIndexParams.CommonIndexParamKey.IMP_LIB;

public class GINIndexTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
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
    }

    @Test
    public void testCheckInvertedIndex() {
        Column c1 = new Column("f1", Type.ARRAY_FLOAT, true);

        Assertions.assertThrows(
                SemanticException.class,
                () -> InvertedIndexUtil.checkInvertedIndexValid(c1, null, KeysType.PRIMARY_KEYS),
                "The inverted index can only be build on DUPLICATE table.");

        Assertions.assertThrows(
                SemanticException.class,
                () -> InvertedIndexUtil.checkInvertedIndexValid(c1, null, KeysType.DUP_KEYS),
                "The inverted index can only be build on column with type of scalar type.");

        Column c2 = new Column("f2", Type.STRING, true);
        Assertions.assertThrows(
                SemanticException.class,
                () -> InvertedIndexUtil.checkInvertedIndexValid(c2, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), "???");
                }}, KeysType.DUP_KEYS),
                "Only support clucene implement for now");

        Assertions.assertThrows(
                SemanticException.class,
                () -> InvertedIndexUtil.checkInvertedIndexValid(c2, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
                    put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, "french");
                }}, KeysType.DUP_KEYS));

        Column c3 = new Column("f3", Type.FLOAT, true);
        Assertions.assertThrows(
                SemanticException.class,
                () -> InvertedIndexUtil.checkInvertedIndexValid(c3, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
                    put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, InvertedIndexUtil.INVERTED_INDEX_PARSER_CHINESE);
                }}, KeysType.DUP_KEYS));

        Assertions.assertThrows(
                SemanticException.class,
                () -> InvertedIndexUtil.checkInvertedIndexValid(c2, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
                    put("xxx", "yyy");
                }}, KeysType.DUP_KEYS));

        Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexValid(c2, new HashMap<String, String>() {{
                    put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
                    put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, InvertedIndexUtil.INVERTED_INDEX_PARSER_CHINESE);
                    put(IndexParamsKey.OMIT_TERM_FREQ_AND_POSITION.name().toLowerCase(Locale.ROOT), "true");
                    put(SearchParamsKey.IS_SEARCH_ANALYZED.name().toLowerCase(Locale.ROOT), "false");
                    put(SearchParamsKey.DEFAULT_SEARCH_ANALYZER.name().toLowerCase(Locale.ROOT), "english");
                    put(SearchParamsKey.RERANK.name().toLowerCase(Locale.ROOT), "false");
                }}, KeysType.DUP_KEYS));
    }

    @Test
    public void testIndexPropertiesWithDefault() {
        Map<String, String> properties = new HashMap<>();
        // empty set default
        InvertedIndexParams.setDefaultParamsValue(properties, CommonIndexParamKey.values());
        Assertions.assertEquals(properties.size(),
                Arrays.stream(CommonIndexParamKey.values()).map(CommonIndexParamKey::needDefault).count());

        // set values, so do not set default
        properties.put(IMP_LIB.name(), "other");
        InvertedIndexParams.setDefaultParamsValue(properties, CommonIndexParamKey.values());
        Assertions.assertEquals(properties.get(IMP_LIB.name()), "other");
    }

    @Test
    public void testIndexToThrift() {
        int indexId = 0;
        String indexName = "test_index";
        List<String> columns = Collections.singletonList("f1");

        Index index = new Index(indexId, indexName, columns, IndexType.GIN, "", new HashMap<>() {{
            put(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name());
            put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, InvertedIndexUtil.INVERTED_INDEX_PARSER_CHINESE);
            put(IndexParamsKey.OMIT_TERM_FREQ_AND_POSITION.name().toLowerCase(Locale.ROOT), "true");
            put(SearchParamsKey.IS_SEARCH_ANALYZED.name().toLowerCase(Locale.ROOT), "false");
            put(SearchParamsKey.DEFAULT_SEARCH_ANALYZER.name().toLowerCase(Locale.ROOT), "english");
            put(SearchParamsKey.RERANK.name().toLowerCase(Locale.ROOT), "false");
        }});

        TOlapTableIndex olapIndex = index.toThrift();
        Assertions.assertEquals(indexId, olapIndex.getIndex_id());
        Assertions.assertEquals(indexName, olapIndex.getIndex_name());
        Assertions.assertEquals(TIndexType.GIN, olapIndex.getIndex_type());
        Assertions.assertEquals(columns, olapIndex.getColumns());
        Assertions.assertEquals(
                Collections.singletonMap(IMP_LIB.name().toLowerCase(Locale.ROOT), InvertedIndexImpType.CLUCENE.name()),
                olapIndex.getCommon_properties());

        Assertions.assertEquals(new HashMap<String, String>(){{
            put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, InvertedIndexUtil.INVERTED_INDEX_PARSER_CHINESE);
            put(IndexParamsKey.OMIT_TERM_FREQ_AND_POSITION.name().toLowerCase(Locale.ROOT), "true");
        }}, olapIndex.getIndex_properties());

        Assertions.assertEquals(new HashMap<String, String>(){{
            put(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY, InvertedIndexUtil.INVERTED_INDEX_PARSER_CHINESE);
            put(IndexParamsKey.OMIT_TERM_FREQ_AND_POSITION.name().toLowerCase(Locale.ROOT), "true");
        }}, olapIndex.getIndex_properties());

        Assertions.assertEquals(new HashMap<String, String>(){{
            put(SearchParamsKey.IS_SEARCH_ANALYZED.name().toLowerCase(Locale.ROOT), "false");
            put(SearchParamsKey.DEFAULT_SEARCH_ANALYZER.name().toLowerCase(Locale.ROOT), "english");
            put(SearchParamsKey.RERANK.name().toLowerCase(Locale.ROOT), "false");
        }}, olapIndex.getSearch_properties());
    }
}
