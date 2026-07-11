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

package com.starrocks.alter;

import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.utframe.TestWithFeService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Drives real ADD/DROP/MODIFY COLUMN statements through SchemaChangeHandler.analyzeAndCreateJob
 * and asserts the alter_table_column_op_total counter increments per op_type. The counter fires at
 * analysis/submission time, so the test does not need to wait for any async job to finish. The
 * type-change MODIFY is issued last (and nothing follows it) because it is not eligible for fast
 * schema evolution and leaves the table in an asynchronous SCHEMA_CHANGE state.
 */
public class AlterColumnOpMetricTest extends TestWithFeService {

    private static final String DB = "test_alter_op_metric";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(DB);
        useDatabase(DB);
        createTable("CREATE TABLE " + DB + ".t (\n"
                + "  k1 INT NOT NULL,\n"
                + "  v1 INT\n"
                + ") DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num'='1','fast_schema_evolution'='true');");
    }

    private void runAlter(String sql) throws Exception {
        connectContext.setThreadLocalInfo();
        AlterTableStmt stmt = (AlterTableStmt) parseAndAnalyzeStmt(sql, connectContext);
        DDLStmtExecutor.execute(stmt, connectContext);
    }

    private long count(String opType) {
        return MetricRepo.COUNTER_ALTER_TABLE_COLUMN_OP.getMetric(opType).getValue();
    }

    @Test
    public void countsAddDropModify() throws Exception {
        boolean saved = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            long add0 = count("add");
            runAlter("ALTER TABLE " + DB + ".t ADD COLUMN v2 INT");
            Assertions.assertEquals(add0 + 1, count("add"), "ADD COLUMN must bump op_type=add");

            long drop0 = count("drop");
            runAlter("ALTER TABLE " + DB + ".t DROP COLUMN v2");
            Assertions.assertEquals(drop0 + 1, count("drop"), "DROP COLUMN must bump op_type=drop");

            // MODIFY COLUMN v1 INT->BIGINT is a type change, so it is NOT fast-schema-evolution-eligible:
            // it creates an asynchronous AlterJobV2 and leaves the table in SCHEMA_CHANGE state. It must
            // run last so no subsequent statement on this table is rejected with
            // InvalidOlapTableStateException while the async job is still in progress.
            long mod0 = count("modify");
            runAlter("ALTER TABLE " + DB + ".t MODIFY COLUMN v1 BIGINT");
            Assertions.assertEquals(mod0 + 1, count("modify"), "MODIFY COLUMN must bump op_type=modify");
        } finally {
            MetricRepo.hasInit = saved;
        }
    }
}
