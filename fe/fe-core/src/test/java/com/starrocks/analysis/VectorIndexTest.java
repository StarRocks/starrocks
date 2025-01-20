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

import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.IndexParams.IndexParamItem;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.VectorIndexParams;
import com.starrocks.common.VectorIndexParams.CommonIndexParamKey;
import com.starrocks.common.VectorIndexParams.IndexParamsKey;
import com.starrocks.common.VectorIndexParams.MetricsType;
import com.starrocks.common.VectorIndexParams.VectorIndexType;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.IndexDef.IndexType;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TIndexType;
import com.starrocks.thrift.TOlapTableIndex;
import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class VectorIndexTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.enable_experimental_vector = true;
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE `test_index_tbl` (\n" +
                "  `f1` int NOT NULL COMMENT \"\",\n" +
                "  `f2` ARRAY<float> NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`f1`)\n" +
                "DISTRIBUTED BY HASH(`f1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @Test
    public void testCheckVectorIndex() {
        Column c1 = new Column("f1", Type.INT, false, AggregateType.MAX, "", "");

        Assertions.assertThrows(
                SemanticException.class,
                () -> VectorIndexUtil.checkVectorIndexValid(c1, null, KeysType.AGG_KEYS),
                "The vector index can only build on DUPLICATE or PRIMARY table");

        Column c2 = new Column("f2", Type.VARCHAR, false);

        Assertions.assertThrows(
                SemanticException.class,
                () -> VectorIndexUtil.checkVectorIndexValid(c2, null, KeysType.DUP_KEYS),
                "The vector index can only be build on column with type of array<float>.");

        Column c3 = new Column("f3", Type.ARRAY_FLOAT, true);
        Assertions.assertThrows(
                SemanticException.class,
                () -> VectorIndexUtil.checkVectorIndexValid(c3, Collections.emptyMap(), KeysType.DUP_KEYS),
                "You should set index_type at least to add a vector index.");

        Column c4 = new Column("f4", Type.ARRAY_FLOAT, false);
        Assertions.assertThrows(
                SemanticException.class,
                () -> VectorIndexUtil.checkVectorIndexValid(c4, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), "???");
                }}, KeysType.DUP_KEYS));

        // INDEX_TYPE error
        Assertions.assertThrows(
                SemanticException.class,
                () -> VectorIndexUtil.checkVectorIndexValid(c4, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), "???");
                    put(CommonIndexParamKey.DIM.name(), "1024");
                }}, KeysType.DUP_KEYS));

        // missing METRICS_TYPE
        Assertions.assertThrows(
                SemanticException.class,
                () -> VectorIndexUtil.checkVectorIndexValid(c4, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
                    put(CommonIndexParamKey.DIM.name(), "1024");
                }}, KeysType.DUP_KEYS));

        // wrong NBITS index params with HNSW
        Assertions.assertThrows(
                SemanticException.class,
                () -> VectorIndexUtil.checkVectorIndexValid(c4, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
                    put(CommonIndexParamKey.DIM.name(), "1024");
                    put(CommonIndexParamKey.METRIC_TYPE.name(), MetricsType.L2_DISTANCE.name());

                    put(VectorIndexParams.IndexParamsKey.M.name(), "10");
                    put(VectorIndexParams.IndexParamsKey.NBITS.name(), "10");
                }}, KeysType.DUP_KEYS),
                "Params HNSW should not define with NBITS"
                );

        // wrong NPROBE search params with HNSW
        Assertions.assertThrows(
                SemanticException.class,
                () -> VectorIndexUtil.checkVectorIndexValid(c4, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
                    put(CommonIndexParamKey.DIM.name(), "1024");
                    put(CommonIndexParamKey.METRIC_TYPE.name(), MetricsType.L2_DISTANCE.name());

                    put(VectorIndexParams.IndexParamsKey.M.name(), "10");

                    put(VectorIndexParams.SearchParamsKey.NPROBE.name(), "10");
                }}, KeysType.DUP_KEYS),
                "Params HNSW should not define with NBITS"
        );

        Assertions.assertDoesNotThrow(
                () -> VectorIndexUtil.checkVectorIndexValid(c4, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
                    put(CommonIndexParamKey.DIM.name(), "1024");
                    put(CommonIndexParamKey.METRIC_TYPE.name(), MetricsType.L2_DISTANCE.name());
                    put(CommonIndexParamKey.IS_VECTOR_NORMED.name(), "false");

                    put(VectorIndexParams.IndexParamsKey.M.name(), "10");
                    put(VectorIndexParams.IndexParamsKey.EFCONSTRUCTION.name(), "10");
                    put(VectorIndexParams.SearchParamsKey.EFSEARCH.name(), "10");
                }}, KeysType.DUP_KEYS)
        );

        Map<String, String> paramItemMap = new HashMap<>(){{
            put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
            put(CommonIndexParamKey.DIM.name(), "1024");
            put(CommonIndexParamKey.METRIC_TYPE.name(), MetricsType.L2_DISTANCE.name());
            put(CommonIndexParamKey.IS_VECTOR_NORMED.name(), "false");
        }};

        // Add default properties
        IndexParamItem m = IndexParamsKey.M.getIndexParamItem();
        IndexParamItem efConstruction = VectorIndexParams.IndexParamsKey.EFCONSTRUCTION.getIndexParamItem();
        Assertions.assertDoesNotThrow(() -> VectorIndexUtil.checkVectorIndexValid(c4, paramItemMap, KeysType.DUP_KEYS));
        Assertions.assertTrue(m.getDefaultValue()
                .equalsIgnoreCase(paramItemMap.get(m.getParamKey().name().toLowerCase(Locale.ROOT))));
        Assertions.assertTrue(efConstruction.getDefaultValue()
                .equalsIgnoreCase(paramItemMap.get(efConstruction.getParamKey().name().toLowerCase(Locale.ROOT))));

        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };
        Assertions.assertThrows(
                SemanticException.class,
                () -> VectorIndexUtil.checkVectorIndexValid(c4, null, KeysType.DUP_KEYS),
                "The vector index does not support shared data mode");
    }

    @Test
    public void testIndexToThrift() {
        int indexId = 0;
        String indexName = "vector_index";
        List<ColumnId> columns = Collections.singletonList(ColumnId.create("f1"));

        Index index = new Index(indexId, indexName, columns, IndexType.VECTOR, "", new HashMap<>() {{
            put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
            put(CommonIndexParamKey.DIM.name(), "1024");
            put(CommonIndexParamKey.METRIC_TYPE.name(), MetricsType.L2_DISTANCE.name());
            put(CommonIndexParamKey.IS_VECTOR_NORMED.name(), "false");

            put(VectorIndexParams.IndexParamsKey.M.name(), "10");
            put(VectorIndexParams.IndexParamsKey.EFCONSTRUCTION.name(), "10");
            put(VectorIndexParams.SearchParamsKey.EFSEARCH.name(), "10");
        }});

        index.hashCode();

        TOlapTableIndex vectorIndex = index.toThrift();
        Assertions.assertEquals(indexId, vectorIndex.getIndex_id());
        Assertions.assertEquals(indexName, vectorIndex.getIndex_name());
        Assertions.assertEquals(TIndexType.VECTOR, vectorIndex.getIndex_type());
        Assertions.assertEquals(columns.stream().map(ColumnId::getId).collect(Collectors.toList()), vectorIndex.getColumns());
        Assertions.assertEquals(
                new HashMap<>(){{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
                    put(CommonIndexParamKey.DIM.name(), "1024");
                    put(CommonIndexParamKey.METRIC_TYPE.name(), MetricsType.L2_DISTANCE.name());
                    put(CommonIndexParamKey.IS_VECTOR_NORMED.name(), "false");
                }},
                vectorIndex.getCommon_properties());

        Assertions.assertEquals(new HashMap<String, String>(){{
            put(VectorIndexParams.IndexParamsKey.M.name(), "10");
            put(VectorIndexParams.IndexParamsKey.EFCONSTRUCTION.name(), "10");
        }}, vectorIndex.getIndex_properties());

        Assertions.assertEquals(new HashMap<String, String>(){{
            put(VectorIndexParams.SearchParamsKey.EFSEARCH.name(), "10");
        }}, vectorIndex.getSearch_properties());
    }
}
