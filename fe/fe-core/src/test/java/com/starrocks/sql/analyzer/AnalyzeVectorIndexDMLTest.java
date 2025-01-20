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

import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static org.assertj.core.api.Assertions.assertThat;

public class AnalyzeVectorIndexDMLTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        connectContext = AnalyzeTestUtil.getConnectContext();

        Config.enable_experimental_vector = true;
    }

    @Test
    public void testValidateParamsForCreateTable() {
        String sql;

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'IVFPQ', \n" +
                "        'dim'='4', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'Nbits' = '8',\n" +
                "        'Nlist' = '16', \n" +
                "        'M_IVFPQ' = '2'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeSuccess(sql);

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'HNSW', \n" +
                "        'dim'='5', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'M' = '2', \n" +
                "        'efconstruction' = '1'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeSuccess(sql);

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'invalid-index-type', \n" +
                "        'dim'='5', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'M' = '16', \n" +
                "        'efconstruction' = '40'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeFail(sql, "Value of `index_type` must in (IVFPQ,HNSW)");

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'HNSW', \n" +
                "        'dim'='invalid-dim', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'M' = '16', \n" +
                "        'efconstruction' = '40'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeFail(sql, "Value of `DIM` must be a integer");
        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'HNSW', \n" +
                "        'dim'='0', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'M' = '16', \n" +
                "        'efconstruction' = '40'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeFail(sql, "Value of `DIM` must be >= 1");

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'HNSW', \n" +
                "        'dim'='5', \n" +
                "        'metric_type' = 'invalid-metric-type', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'M' = '16', \n" +
                "        'efconstruction' = '40'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeFail(sql, "Value of `METRIC_TYPE` must be in [l2_distance, cosine_similarity]");

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'HNSW', \n" +
                "        'dim'='5', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'invalid-is-vector-normed', \n" +
                "        'M' = '16', \n" +
                "        'efconstruction' = '40'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeFail(sql, "Value of `IS_VECTOR_NORMED` must be `true` or `false`");

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'HNSW', \n" +
                "        'dim'='5', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'M' = 'invalid-M', \n" +
                "        'efconstruction' = '40'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeFail(sql, "Value of `M` must be a integer");

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'HNSW', \n" +
                "        'dim'='5', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'M' = '1', \n" +
                "        'efconstruction' = '40'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeFail(sql, "Value of `M` must be >= 2");

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'HNSW', \n" +
                "        'dim'='5', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'M' = '1', \n" +
                "        'efconstruction' = 'invalid-efconstruction'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeFail(sql, "Value of `EFCONSTRUCTION` must be a integer");

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'HNSW', \n" +
                "        'dim'='5', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'M' = '1', \n" +
                "        'efconstruction' = '0'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeFail(sql, "Value of `EFCONSTRUCTION` must be >= 1");

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'HNSW', \n" +
                "        'dim'='5', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'M' = '2', \n" +
                "        'efconstruction' = '1',\n" +
                "        'Nbits' = '8'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeFail(sql, "Index params [NBITS] should not define with HNSW");

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'IVFPQ', \n" +
                "        'dim'='4', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'Nbits' = 'invalid-Nbits',\n" +
                "        'Nlist' = '16', \n" +
                "        'M_IVFPQ' = '2'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeFail(sql, "Value of `NBITS` must be a integer");

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'IVFPQ', \n" +
                "        'dim'='4', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'Nbits' = '2',\n" +
                "        'Nlist' = '16', \n" +
                "        'M_IVFPQ' = '2'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeFail(sql, "Value of `NBITS` must be 8");

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'IVFPQ', \n" +
                "        'dim'='4', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'Nbits' = '8',\n" +
                "        'Nlist' = 'invalid-Nlist', \n" +
                "        'M_IVFPQ' = '2'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeFail(sql, "Value of `NLIST` must be a integer");

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'IVFPQ', \n" +
                "        'dim'='4', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'Nbits' = '8',\n" +
                "        'Nlist' = '0',\n" +
                "        'M_IVFPQ' = '2'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeFail(sql, "Value of `NLIST` must be >= 1");

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'IVFPQ', \n" +
                "        'dim'='4', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'Nbits' = '8',\n" +
                "        'Nlist' = '16', 'invalid-key'='10', \n" +
                "        'M_IVFPQ' = '2'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeFail(sql, "Unknown index param: `INVALID-KEY`");
    }

    @Test
    public void testValidateParamsForAlterTable() throws Exception {
        AnalyzeTestUtil.getStarRocksAssert().withTable("CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    v1 ARRAY<FLOAT> NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES ('replication_num'='1');");
        String sql;

        try {
            sql = "ALTER TABLE vector_t1 ADD INDEX index_vector1 (v1) USING VECTOR (\n" +
                    "        'index_type' = 'IVFPQ', \n" +
                    "        'dim'='4', \n" +
                    "        'metric_type' = 'l2_distance', \n" +
                    "        'is_vector_normed' = 'false', \n" +
                    "        'Nbits' = '8',\n" +
                    "        'Nlist' = '16', \n" +
                    "        'M_IVFPQ' = '2'\n" +
                    "    )\n";
            analyzeSuccess(sql);

            sql = "ALTER TABLE vector_t1 ADD INDEX index_vector1 (v1) USING VECTOR (\n" +
                    "        'index_type' = 'HNSW', \n" +
                    "        'dim'='5', \n" +
                    "        'metric_type' = 'l2_distance', \n" +
                    "        'is_vector_normed' = 'false', \n" +
                    "        'M' = '2', \n" +
                    "        'efconstruction' = '1'\n" +
                    "    )\n";
            analyzeSuccess(sql);

            sql = "ALTER TABLE vector_t1 ADD INDEX index_vector1 (v1) USING VECTOR (\n" +
                    "        'index_type' = 'aIVFPQ', \n" +
                    "        'dim'='5', \n" +
                    "        'metric_type' = 'l2_distance', \n" +
                    "        'is_vector_normed' = 'false', \n" +
                    "        'Nbits' = '8',\n" +
                    "        'Nlist' = '16' \n" +
                    "    )\n";
            analyzeSuccess(sql);

            StatementBase statement = SqlParser.parseSingleStatement(sql, connectContext.getSessionVariable().getSqlMode());
            StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statement);
            stmtExecutor.execute();
            assertThat(connectContext.getState().getErrType()).isEqualTo(QueryState.ErrType.INTERNAL_ERR);
            assertThat(connectContext.getState().getErrorMessage()).contains("Value of `index_type` must in (IVFPQ,HNSW)");

        } finally {
            AnalyzeTestUtil.getStarRocksAssert().dropTables(List.of("vector_t1"));
        }
    }

    @Test
    public void testOnlyOneVectorIndexForCreateTable() {
        String sql;

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    v1 ARRAY<FLOAT> NOT NULL,\n" +
                "    v2 ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_v1 (vector) USING VECTOR (\n" +
                "        'index_type' = 'IVFPQ', \n" +
                "        'dim'='4', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'Nbits' = '8',\n" +
                "        'Nlist' = '16', \n" +
                "        'M_IVFPQ' = '2'\n" +
                "    ),\n" +
                "    INDEX index_v2 (vector) USING VECTOR (\n" +
                "        'index_type' = 'IVFPQ', \n" +
                "        'dim'='4', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'Nbits' = '8',\n" +
                "        'Nlist' = '16', \n" +
                "        'M_IVFPQ' = '2'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeFail(sql, "At most one vector index is allowed for a table, but 2 were found: [index_v1, index_v2]");
    }

    @Test
    public void testOnlyOneVectorIndexForAlterTableSuccess() throws Exception {
        AnalyzeTestUtil.getStarRocksAssert().withTable("CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    v1 ARRAY<FLOAT> NOT NULL,\n" +
                "    v2 ARRAY<FLOAT> NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES ('replication_num'='1');");
        String sql;

        try {
            sql = "ALTER TABLE vector_t1 ADD INDEX index_v1 (v1) USING VECTOR (\n" +
                    "        'index_type' = 'IVFPQ', \n" +
                    "        'dim'='4', \n" +
                    "        'metric_type' = 'l2_distance', \n" +
                    "        'is_vector_normed' = 'false', \n" +
                    "        'Nbits' = '8',\n" +
                    "        'Nlist' = '16', \n" +
                    "        'M_IVFPQ' = '2'\n" +
                    "    )\n";
            StatementBase statement = SqlParser.parseSingleStatement(sql, connectContext.getSessionVariable().getSqlMode());
            StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statement);
            stmtExecutor.execute();
            assertThat(connectContext.getState().isError()).isFalse();
        } finally {
            AnalyzeTestUtil.getStarRocksAssert().dropTables(List.of("vector_t1"));
        }
    }

    @Test
    public void testOnlyOneVectorIndexForAlterTableFail() throws Exception {
        AnalyzeTestUtil.getStarRocksAssert().withTable("CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    v1 ARRAY<FLOAT> NOT NULL,\n" +
                "    v2 ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_v1 (v1) USING VECTOR (\n" +
                "        'index_type' = 'IVFPQ', \n" +
                "        'dim'='4', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'Nbits' = '8',\n" +
                "        'Nlist' = '16',\n" +
                "        'M_IVFPQ' = '2'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES ('replication_num'='1');");
        String sql;

        try {
            {
                sql = "ALTER TABLE vector_t1 ADD INDEX index_v2 (v2) USING VECTOR (\n" +
                        "        'index_type' = 'IVFPQ', \n" +
                        "        'dim'='4', \n" +
                        "        'metric_type' = 'l2_distance', \n" +
                        "        'is_vector_normed' = 'false', \n" +
                        "        'Nbits' = '8',\n" +
                        "        'Nlist' = '16', \n" +
                        "        'M_IVFPQ' = '2'\n" +
                        "    )\n";
                StatementBase statement = SqlParser.parseSingleStatement(sql, connectContext.getSessionVariable().getSqlMode());
                StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statement);
                stmtExecutor.execute();
                assertThat(connectContext.getState().getErrorMessage()).contains(
                        "At most one vector index is allowed for a table, but there is already a vector index [index_v1]");
            }
        } finally {
            AnalyzeTestUtil.getStarRocksAssert().dropTables(List.of("vector_t1"));
        }
    }

    @Test
    public void testCreateOnNullableColumn() {
        String sql;

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'IVFPQ', \n" +
                "        'dim'='5', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'Nbits' = '8',\n" +
                "        'Nlist' = '16', \n" +
                "        'M_IVFPQ' = '2'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1";
        analyzeFail(sql, "The vector index can only build on non-nullable column");
    }

    @Test
    public void testIVFPQ() {
        String sql;

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    v1 ARRAY<FLOAT> NOT NULL,\n" +
                "    v2 ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_v1 (v1) USING VECTOR (\n" +
                "        'index_type' = 'IVFPQ', \n" +
                "        'dim'='4', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'Nbits' = '8',\n" +
                "        'Nlist' = '16'" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES ('replication_num'='1');";
        analyzeFail(sql, "`M_IVFPQ` is required for IVFPQ index");

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    v1 ARRAY<FLOAT> NOT NULL,\n" +
                "    v2 ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_v1 (v1) USING VECTOR (\n" +
                "        'index_type' = 'IVFPQ', \n" +
                "        'dim'='10', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'Nbits' = '8',\n" +
                "        'Nlist' = '16', \n" +
                "        'M_IVFPQ' = '3' \n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES ('replication_num'='1');";
        analyzeFail(sql, "`DIM` should be a multiple of `M_IVFPQ` for IVFPQ index");

        sql = "CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    v1 ARRAY<FLOAT> NOT NULL,\n" +
                "    v2 ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_v1 (v1) USING VECTOR (\n" +
                "        'index_type' = 'IVFPQ', \n" +
                "        'dim'='10', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'Nbits' = '8',\n" +
                "        'Nlist' = '16', \n" +
                "        'M_IVFPQ' = '2' \n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES ('replication_num'='1');";
        analyzeSuccess(sql);
    }

    @Test
    public void testShowIndex() throws Exception {
        String sql;
        String show;

        AnalyzeTestUtil.getStarRocksAssert().withTable("CREATE TABLE vector_t1 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    v1 ARRAY<FLOAT> NOT NULL,\n" +
                "    v2 ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_v1 (v1) USING VECTOR (\n" +
                "        'index_type' = 'IVFPQ', \n" +
                "        'dim'='10', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'Nbits' = '8',\n" +
                "        'Nlist' = '16',\n" +
                "        'M_IVFPQ' = '2'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES ('replication_num'='1');");
        show = AnalyzeTestUtil.getStarRocksAssert().showCreateTable("show create table vector_t1");
        assertThat(show).contains(
                "INDEX index_v1 (`v1`) USING VECTOR(\"dim\" = \"10\", \"index_type\" = \"ivfpq\", " +
                        "\"is_vector_normed\" = \"false\", \"m_ivfpq\" = \"2\", \"metric_type\" = \"l2_distance\", " +
                        "\"nbits\" = \"8\", \"nlist\" = \"16\")");

        sql = "CREATE TABLE vector_t2 (\n" +
                "    id bigint(20) NOT NULL,\n" +
                "    vector ARRAY<FLOAT> NOT NULL,\n" +
                "    INDEX index_vector (vector) USING VECTOR (\n" +
                "        'index_type' = 'HNSW', \n" +
                "        'dim'='5', \n" +
                "        'metric_type' = 'l2_distance', \n" +
                "        'is_vector_normed' = 'false', \n" +
                "        'M' = '2', \n" +
                "        'efconstruction' = '1'\n" +
                "    )\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES ('replication_num'='1');";
        AnalyzeTestUtil.getStarRocksAssert().withTable(sql);
        show = AnalyzeTestUtil.getStarRocksAssert().showCreateTable("show create table vector_t2");
        assertThat(show).contains(
                "INDEX index_vector (`vector`) USING VECTOR(\"dim\" = \"5\", \"efconstruction\" = \"1\", " +
                        "\"index_type\" = \"hnsw\", \"is_vector_normed\" = \"false\", \"m\" = \"2\", " +
                        "\"metric_type\" = \"l2_distance\")");

        AnalyzeTestUtil.getStarRocksAssert().dropTables(List.of("vector_t1", "vector_t2"));
    }
}
