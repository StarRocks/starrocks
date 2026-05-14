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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.IndexParams.IndexParamItem;
import com.starrocks.common.Config;
import com.starrocks.common.VectorIndexParams;
import com.starrocks.common.VectorIndexParams.CommonIndexParamKey;
import com.starrocks.common.VectorIndexParams.IndexParamsKey;
import com.starrocks.common.VectorIndexParams.MetricsType;
import com.starrocks.common.VectorIndexParams.QuantizerType;
import com.starrocks.common.VectorIndexParams.VectorIndexType;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.IndexAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.IndexDef.IndexType;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TIndexType;
import com.starrocks.thrift.TOlapTableIndex;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class VectorIndexTest extends PlanTestBase {

    @BeforeAll
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
        Column c1 = new Column("f1", IntegerType.INT, false, AggregateType.MAX, "", "");

        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkVectorIndexValid(c1, null, KeysType.AGG_KEYS),
                "The vector index can only build on DUPLICATE or PRIMARY table");

        Column c2 = new Column("f2", VarcharType.VARCHAR, false);

        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkVectorIndexValid(c2, null, KeysType.DUP_KEYS),
                "The vector index can only be build on column with type of array<float>.");

        Column c3 = new Column("f3", ArrayType.ARRAY_FLOAT, true);
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkVectorIndexValid(c3, Collections.emptyMap(), KeysType.DUP_KEYS),
                "You should set index_type at least to add a vector index.");

        Column c4 = new Column("f4", ArrayType.ARRAY_FLOAT, false);
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkVectorIndexValid(c4, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), "???");
                }}, KeysType.DUP_KEYS));

        // INDEX_TYPE error
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkVectorIndexValid(c4, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), "???");
                    put(CommonIndexParamKey.DIM.name(), "1024");
                }}, KeysType.DUP_KEYS));

        // missing METRICS_TYPE
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkVectorIndexValid(c4, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
                    put(CommonIndexParamKey.DIM.name(), "1024");
                }}, KeysType.DUP_KEYS));

        // wrong NBITS index params with HNSW
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkVectorIndexValid(c4, new HashMap<>() {{
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
                () -> IndexAnalyzer.checkVectorIndexValid(c4, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
                    put(CommonIndexParamKey.DIM.name(), "1024");
                    put(CommonIndexParamKey.METRIC_TYPE.name(), MetricsType.L2_DISTANCE.name());

                    put(VectorIndexParams.IndexParamsKey.M.name(), "10");

                    put(VectorIndexParams.SearchParamsKey.NPROBE.name(), "10");
                }}, KeysType.DUP_KEYS),
                "Params HNSW should not define with NBITS"
        );

        Assertions.assertDoesNotThrow(
                () -> IndexAnalyzer.checkVectorIndexValid(c4, new HashMap<>() {{
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
        Assertions.assertDoesNotThrow(() -> IndexAnalyzer.checkVectorIndexValid(c4, paramItemMap, KeysType.DUP_KEYS));
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
                () -> IndexAnalyzer.checkVectorIndexValid(c4, null, KeysType.DUP_KEYS),
                "The vector index does not support shared data mode");
    }

    @Test
    public void testQuantizerPropertyRegistration() {
        // Covers C1 wiring: the quantizer/m_pq/nbits_pq keys are registered in
        // IndexParams and their single-field check() is dispatched by the
        // analyzer. Backward compatibility (no property) is the key invariant
        // since existing indexes must keep parsing.
        Column vecCol = new Column("f2", ArrayType.ARRAY_FLOAT, false);

        // Quantizer omitted -> backward-compat flat behaviour.
        Assertions.assertDoesNotThrow(
                () -> IndexAnalyzer.checkVectorIndexValid(vecCol, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
                    put(CommonIndexParamKey.DIM.name(), "128");
                    put(CommonIndexParamKey.METRIC_TYPE.name(), MetricsType.L2_DISTANCE.name());
                    put(CommonIndexParamKey.IS_VECTOR_NORMED.name(), "false");
                    put(IndexParamsKey.M.name(), "16");
                    put(IndexParamsKey.EFCONSTRUCTION.name(), "40");
                }}, KeysType.DUP_KEYS));

        // Quantizer=sq8 with L2 metric: the new key is accepted.
        Assertions.assertDoesNotThrow(
                () -> IndexAnalyzer.checkVectorIndexValid(vecCol, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
                    put(CommonIndexParamKey.DIM.name(), "128");
                    put(CommonIndexParamKey.METRIC_TYPE.name(), MetricsType.L2_DISTANCE.name());
                    put(CommonIndexParamKey.IS_VECTOR_NORMED.name(), "false");
                    put(IndexParamsKey.M.name(), "16");
                    put(IndexParamsKey.EFCONSTRUCTION.name(), "40");
                    put(IndexParamsKey.QUANTIZER.name(), QuantizerType.SQ8.name());
                }}, KeysType.DUP_KEYS));

        // Unknown quantizer name is rejected by the per-key check().
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkVectorIndexValid(vecCol, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
                    put(CommonIndexParamKey.DIM.name(), "128");
                    put(CommonIndexParamKey.METRIC_TYPE.name(), MetricsType.L2_DISTANCE.name());
                    put(CommonIndexParamKey.IS_VECTOR_NORMED.name(), "false");
                    put(IndexParamsKey.M.name(), "16");
                    put(IndexParamsKey.EFCONSTRUCTION.name(), "40");
                    put(IndexParamsKey.QUANTIZER.name(), "bogus");
                }}, KeysType.DUP_KEYS));

        // NBITS_PQ range (single-field) is enforced via the enum's check().
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkVectorIndexValid(vecCol, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
                    put(CommonIndexParamKey.DIM.name(), "128");
                    put(CommonIndexParamKey.METRIC_TYPE.name(), MetricsType.L2_DISTANCE.name());
                    put(CommonIndexParamKey.IS_VECTOR_NORMED.name(), "false");
                    put(IndexParamsKey.M.name(), "16");
                    put(IndexParamsKey.EFCONSTRUCTION.name(), "40");
                    put(IndexParamsKey.QUANTIZER.name(), QuantizerType.PQ.name());
                    put(IndexParamsKey.M_PQ.name(), "16");
                    put(IndexParamsKey.NBITS_PQ.name(), "2");
                }}, KeysType.DUP_KEYS),
                "Value of `NBITS_PQ` must be in [4, 16]");
    }

    @Test
    public void testQuantizerCrossFieldValidation() {
        // Covers C2 cross-field rules in IndexAnalyzer. Single-field wiring is
        // already covered by testQuantizerPropertyRegistration.
        Column vecCol = new Column("f2", ArrayType.ARRAY_FLOAT, false);

        // PQ happy path: m_pq divides dim, nbits_pq in range.
        Assertions.assertDoesNotThrow(
                () -> IndexAnalyzer.checkVectorIndexValid(vecCol, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
                    put(CommonIndexParamKey.DIM.name(), "128");
                    put(CommonIndexParamKey.METRIC_TYPE.name(), MetricsType.L2_DISTANCE.name());
                    put(CommonIndexParamKey.IS_VECTOR_NORMED.name(), "false");
                    put(IndexParamsKey.M.name(), "16");
                    put(IndexParamsKey.EFCONSTRUCTION.name(), "40");
                    put(IndexParamsKey.QUANTIZER.name(), QuantizerType.PQ.name());
                    put(IndexParamsKey.M_PQ.name(), "16");
                    put(IndexParamsKey.NBITS_PQ.name(), "8");
                }}, KeysType.DUP_KEYS));

        // V1 rejects quantizer != flat combined with non-L2 metric. Users who
        // want SQ/PQ on cosine should upgrade once we validate recall.
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkVectorIndexValid(vecCol, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
                    put(CommonIndexParamKey.DIM.name(), "128");
                    put(CommonIndexParamKey.METRIC_TYPE.name(), MetricsType.COSINE_SIMILARITY.name());
                    put(CommonIndexParamKey.IS_VECTOR_NORMED.name(), "false");
                    put(IndexParamsKey.M.name(), "16");
                    put(IndexParamsKey.EFCONSTRUCTION.name(), "40");
                    put(IndexParamsKey.QUANTIZER.name(), QuantizerType.SQ8.name());
                }}, KeysType.DUP_KEYS),
                "Quantized HNSW index only supports metric_type = l2_distance in V1");

        // PQ requires m_pq — tenann/faiss cannot infer it.
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkVectorIndexValid(vecCol, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
                    put(CommonIndexParamKey.DIM.name(), "128");
                    put(CommonIndexParamKey.METRIC_TYPE.name(), MetricsType.L2_DISTANCE.name());
                    put(CommonIndexParamKey.IS_VECTOR_NORMED.name(), "false");
                    put(IndexParamsKey.M.name(), "16");
                    put(IndexParamsKey.EFCONSTRUCTION.name(), "40");
                    put(IndexParamsKey.QUANTIZER.name(), QuantizerType.PQ.name());
                }}, KeysType.DUP_KEYS),
                "`M_PQ` is required when QUANTIZER = pq");

        // dim must be divisible by m_pq — otherwise faiss index_factory throws at BE.
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkVectorIndexValid(vecCol, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
                    put(CommonIndexParamKey.DIM.name(), "10");
                    put(CommonIndexParamKey.METRIC_TYPE.name(), MetricsType.L2_DISTANCE.name());
                    put(CommonIndexParamKey.IS_VECTOR_NORMED.name(), "false");
                    put(IndexParamsKey.M.name(), "16");
                    put(IndexParamsKey.EFCONSTRUCTION.name(), "40");
                    put(IndexParamsKey.QUANTIZER.name(), QuantizerType.PQ.name());
                    put(IndexParamsKey.M_PQ.name(), "3");
                }}, KeysType.DUP_KEYS),
                "`DIM` should be a multiple of `M_PQ` for PQ-quantized HNSW index");

        // M_PQ supplied with a non-PQ quantizer is rejected (BE ignores it; failing
        // loudly avoids silently accepting an ineffective config).
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkVectorIndexValid(vecCol, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
                    put(CommonIndexParamKey.DIM.name(), "128");
                    put(CommonIndexParamKey.METRIC_TYPE.name(), MetricsType.L2_DISTANCE.name());
                    put(CommonIndexParamKey.IS_VECTOR_NORMED.name(), "false");
                    put(IndexParamsKey.M.name(), "16");
                    put(IndexParamsKey.EFCONSTRUCTION.name(), "40");
                    put(IndexParamsKey.QUANTIZER.name(), QuantizerType.SQ8.name());
                    put(IndexParamsKey.M_PQ.name(), "16");
                }}, KeysType.DUP_KEYS),
                "`M_PQ` is only allowed when QUANTIZER = pq");

        // NBITS_PQ supplied without QUANTIZER (defaults to flat) is rejected.
        Assertions.assertThrows(
                SemanticException.class,
                () -> IndexAnalyzer.checkVectorIndexValid(vecCol, new HashMap<>() {{
                    put(CommonIndexParamKey.INDEX_TYPE.name(), VectorIndexType.HNSW.name());
                    put(CommonIndexParamKey.DIM.name(), "128");
                    put(CommonIndexParamKey.METRIC_TYPE.name(), MetricsType.L2_DISTANCE.name());
                    put(CommonIndexParamKey.IS_VECTOR_NORMED.name(), "false");
                    put(IndexParamsKey.M.name(), "16");
                    put(IndexParamsKey.EFCONSTRUCTION.name(), "40");
                    put(IndexParamsKey.NBITS_PQ.name(), "8");
                }}, KeysType.DUP_KEYS),
                "`NBITS_PQ` is only allowed when QUANTIZER = pq");
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
