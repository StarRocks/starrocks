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

import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.planner.IcebergDeleteSink;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the format-version guard on the Iceberg row-level DELETE plan path: row-level
 * DELETE writes position-delete files, which only format version 2 supports, so planning
 * must reject it on non-v2 tables. The guard sits after the metadata (whole-file) delete
 * branch in DeletePlanner, which is format-version-agnostic and must stay reachable.
 */
public class IcebergDeleteFormatVersionTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
    }

    private static ExecPlan getExecPlanOf(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        StatementBase statementBase =
                com.starrocks.sql.parser.SqlParser.parse(sql, connectContext.getSessionVariable().getSqlMode())
                        .get(0);
        connectContext.getDumpInfo().setOriginStmt(sql);
        return StatementPlanner.plan(statementBase, connectContext);
    }

    @Test
    public void testRowLevelDeleteOnNonV2TableRejected() {
        // t0 is format version 3; its predicate does not qualify for metadata delete
        // (MockIcebergMetadata keeps the ConnectorMetadata default canDeleteUsingMetadata=false),
        // so the planner takes the row-level path and must reject the non-v2 table.
        SemanticException exception = assertThrows(SemanticException.class,
                () -> getExecPlanOf("DELETE FROM iceberg0.unpartitioned_db.t0 WHERE id = 1"));
        assertTrue(exception.getMessage().contains("V2 tables"),
                "Expected the format-version error, got: " + exception.getMessage());
    }

    @Test
    public void testRowLevelDeleteOnV2TablePlanned() throws Exception {
        // The same row-level path on a format version 2 table must keep planning normally.
        ExecPlan execPlan = getExecPlanOf("DELETE FROM iceberg0.unpartitioned_db.t0_v2 WHERE id = 1");
        assertNotNull(execPlan);
        assertInstanceOf(IcebergDeleteSink.class, execPlan.getFragments().get(0).getSink());
    }
}
