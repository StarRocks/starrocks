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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/Column.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.starrocks.persist.OriginStatementInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateSyncMVStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.AggStateDesc;
import com.starrocks.type.DecimalType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.VarbinaryType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;

public class SyncMaterializedViewDefinitionTest {
    private static ConnectContext context;
    private static OlapTable base;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        context = UtFrameUtils.createDefaultCtx();
        StarRocksAssert fixtures = new StarRocksAssert(context);
        fixtures.withDatabase("sync_definition").useDatabase("sync_definition")
                .withTable("CREATE TABLE source (k INT, v BIGINT, d DECIMAL(18,4), mv_real INT) " +
                        "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        base = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb("sync_definition").getTable("source");
    }

    @Test
    public void testRestoreAnalyzedPerRowDefinitions() throws Exception {
        String[] projections = {
                "abs(k) AS computed_key, count(v)",
                "k, bitmap_agg(v)",
                "k, hll_union(hll_hash(cast(v AS VARCHAR))) AS hll_value",
                "k, avg_union(avg_state(v)) AS avg_value",
                "k, avg_union(avg_state(d + cast(1.20 AS DECIMAL(18,4)))) AS avg_value"
        };
        for (String projection : projections) {
            String group = projection.startsWith("abs") ? "abs(k)" : "k";
            String sql = "CREATE MATERIALIZED VIEW definition_mv AS SELECT " + projection +
                    " FROM source WHERE v > 10 GROUP BY " + group;
            CreateSyncMVStmt stmt = (CreateSyncMVStmt) UtFrameUtils.parseStmtWithNewParser(sql, context);
            List<Column> schema = new ArrayList<>();
            stmt.getMVColumnItemList().forEach(item -> schema.add(item.toMVColumn(base)));
            List<Type> creationTypes = new ArrayList<>();
            List<String> originalSql = new ArrayList<>();
            for (Column column : schema) {
                Expr expr = column.getDefineExpr();
                creationTypes.add(expr == null ? null : expr.getType());
                originalSql.add(expr == null ? null : AstToSQLBuilder.toSQL(expr));
            }
            MaterializedIndexMeta meta = meta(schema);
            meta.setDefineStmt(new OriginStatementInfo(sql, 0));
            meta.setDbId(123);
            meta.setSchemaId(987);
            meta.setSyncMvRequiredTxnId(456);
            meta.setSyncMvDefinition(SyncMaterializedViewDefinition.capture(schema, stmt.getWhereClause()));
            for (int i = 0; i < schema.size(); i++) {
                Assertions.assertEquals(originalSql.get(i), schema.get(i).getDefineExpr() == null ? null :
                        AstToSQLBuilder.toSQL(schema.get(i).getDefineExpr()), "capture must not mutate the AST");
            }
            MaterializedIndexMeta restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(meta), MaterializedIndexMeta.class);
            base.analyzeRollupIndexMeta(restored);
            Assertions.assertNotNull(restored.getWhereClause());
            Assertions.assertTrue(restored.getWhereClause().getType().isBoolean());
            Assertions.assertEquals(sql, restored.getOriginStmt());
            Assertions.assertEquals(987, restored.getSchemaId());
            Assertions.assertEquals(456, restored.getSyncMvRequiredTxnId());
            for (int i = 0; i < schema.size(); i++) {
                Column recoveredColumn = restored.getSchema().get(i);
                Assertions.assertEquals(schema.get(i).getType(), recoveredColumn.getType());
                Assertions.assertEquals(schema.get(i).getUniqueId(), recoveredColumn.getUniqueId());
                Assertions.assertEquals(schema.get(i).getAggStateDesc(), recoveredColumn.getAggStateDesc());
                if (creationTypes.get(i) != null) {
                    Expr recovered = recoveredColumn.getDefineExpr();
                    Assertions.assertNotNull(recovered);
                    Assertions.assertEquals(creationTypes.get(i), recovered.getType(), projection);
                    Assertions.assertEquals(creationTypes.get(i).getAggStateDesc(), recovered.getType().getAggStateDesc());
                }
            }
            base.setIndexMeta(100, "definition_mv", restored.getSchema(), 0, 0, (short) 1,
                    TStorageType.COLUMN, KeysType.AGG_KEYS, null);
            base.getIndexMetaIdToMeta().put(100L, restored);
            for (Partition partition : base.getPartitions()) {
                for (PhysicalPartition physical : partition.getSubPartitions()) {
                    MaterializedIndex index = new MaterializedIndex(100, MaterializedIndex.IndexState.NORMAL);
                    physical.getLatestBaseIndex().getTablets().forEach(tablet -> index.addTablet(tablet, null, false));
                    physical.createRollupIndex(index);
                }
            }
            base.rebuildFullSchema();
            OlapTable recoveredTable = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(base), OlapTable.class);
            recoveredTable.onReload();
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("sync_definition");
            db.dropTable("source");
            db.registerTableUnlocked(recoveredTable);
            Assertions.assertNotNull(UtFrameUtils.getPlanAndFragment(context,
                    "INSERT INTO source VALUES (1, 2, 1.2500, 3)").second, projection);
            base = recoveredTable;
            base.deleteIndexInfo("definition_mv");
            for (Partition partition : base.getPartitions()) {
                for (PhysicalPartition physical : partition.getSubPartitions()) {
                    physical.deleteMaterializedIndexByMetaId(100);
                }
            }
            Expr value = restored.getSchema().get(1).getDefineExpr();
            if (projection.contains("count")) {
                Assertions.assertEquals(IntegerType.BIGINT, value.getType());
            } else if (projection.contains("bitmap_agg")) {
                Assertions.assertTrue(AstToSQLBuilder.toSQL(value).toLowerCase().contains("to_bitmap"));
                Assertions.assertTrue(value.getType().isBitmapType());
            } else if (projection.contains("avg_union")) {
                Assertions.assertNotNull(value.getType().getAggStateDesc());
                Assertions.assertEquals("avg", value.getType().getAggStateDesc().getFunctionName());
            }
        }
    }

    @Test
    public void testPreserveLiteralAndImplicitCastTypes() {
        List<Expr> expressions = Lists.newArrayList(new IntLiteral(1, IntegerType.BIGINT),
                new CastExpr(IntegerType.BIGINT, new SlotRef(new TableName(null, "alias"), "k")));
        DecimalLiteral folded = new DecimalLiteral("1.2");
        folded.setType(new DecimalType(PrimitiveType.DECIMAL64, 18, 4));
        expressions.add(folded);
        NullLiteral typedNull = new NullLiteral();
        typedNull.setType(IntegerType.BIGINT);
        expressions.add(typedNull);
        expressions.add(new NullLiteral());
        for (Expr expr : expressions) {
            Column value = new Column("value", expr.getType());
            value.setDefineExpr(expr);
            List<Column> schema = Lists.newArrayList(value);
            String original = AstToSQLBuilder.toSQL(expr);
            MaterializedIndexMeta restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(
                    definitionMeta(schema)), MaterializedIndexMeta.class);
            base.analyzeRollupIndexMeta(restored);
            Assertions.assertEquals(expr.getType(), restored.getSchema().get(0).getDefineExpr().getType());
            Assertions.assertEquals(original, AstToSQLBuilder.toSQL(expr));
        }
    }

    @ParameterizedTest
    @CsvSource({"v,false", "d,false", "v,true", "d,true"})
    public void testStateTypeSurvivesUnrelatedBinaryColumn(String argument, boolean borrowStateType) throws Exception {
        AggStateDesc previous = VarbinaryType.VARBINARY.getAggStateDesc();
        try {
            CreateSyncMVStmt stmt = (CreateSyncMVStmt) UtFrameUtils.parseStmtWithNewParser(
                    "CREATE MATERIALIZED VIEW state_isolation AS SELECT k, avg_union(avg_state(" + argument +
                            ")) AS avg_value FROM source GROUP BY k", context);
            Column state = stmt.getMVColumnItemList().get(1).toMVColumn(base);
            AggStateDesc expected = state.getAggStateDesc();
            Assertions.assertNotNull(expected);
            // A view over a binary-returning builtin constructs an ordinary column from its shared type.
            new Column("unrelated_binary", borrowStateType ? state.getType() : VarbinaryType.VARBINARY);
            Assertions.assertEquals(expected, state.getType().getAggStateDesc());
            Assertions.assertNotSame(VarbinaryType.VARBINARY, state.getType());
            Column snapshot = new Column(state);
            Assertions.assertNotSame(state.getType(), snapshot.getType());
            new Column("borrowed_snapshot", snapshot.getType());
            Assertions.assertEquals(expected, snapshot.getType().getAggStateDesc());
            Assertions.assertEquals(expected, state.getType().getAggStateDesc());
        } finally {
            VarbinaryType.VARBINARY.setAggStateDesc(previous);
        }
    }

    @Test
    public void testPreserveOverloadForFoldedStateArguments() {
        Expr[] foldedArguments = {new DecimalLiteral("1.2", new DecimalType(PrimitiveType.DECIMAL64, 18, 4)),
                new IntLiteral(1, IntegerType.BIGINT), NullLiteral.create(IntegerType.BIGINT)};
        // DecimalLiteral's typed constructor narrows its type to the value; an analyzed folded node carries
        // the expression's authoritative type via setType.
        foldedArguments[0].setType(new DecimalType(PrimitiveType.DECIMAL64, 18, 4));
        String[] argumentSql = {"CAST(1.2 AS DECIMAL(18,4))", "CAST(1 AS BIGINT)", "CAST(NULL AS BIGINT)"};
        for (int i = 0; i < foldedArguments.length; i++) {
            Column stateColumn = new Column("state_value", IntegerType.INT);
            stateColumn.setDefineExpr(SqlParser.parseSqlToExpr("avg_state(" + argumentSql[i] + ")",
                    SqlModeHelper.MODE_DEFAULT));
            MaterializedIndexMeta creation = meta(Lists.newArrayList(stateColumn));
            base.analyzeRollupIndexMeta(creation);
            Expr creationExpr = stateColumn.getDefineExpr();
            Type stateType = creationExpr.getType();
            stateColumn.setType(stateType);
            Assertions.assertNotNull(stateType.getAggStateDesc());
            // Model the analyzed constant folder replacing CAST with a typed literal.
            creationExpr.setChild(0, foldedArguments[i]);
            MaterializedIndexMeta captured = definitionMeta(creation.getSchema());
            MaterializedIndexMeta restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(captured),
                    MaterializedIndexMeta.class);
            base.analyzeRollupIndexMeta(restored);
            Expr recovered = restored.getSchema().get(0).getDefineExpr();
            Assertions.assertEquals(stateType, recovered.getType());
            Assertions.assertEquals(stateType.getAggStateDesc(), recovered.getType().getAggStateDesc());
            Assertions.assertEquals(foldedArguments[i].getType(), recovered.getChild(0).getType());
        }
    }

    @Test
    public void testPreserveRealMvPrefixedBaseColumn() {
        Column source = new Column("mv_real", IntegerType.INT);
        MaterializedIndexMeta restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(
                definitionMeta(Lists.newArrayList(source))), MaterializedIndexMeta.class);
        base.analyzeRollupIndexMeta(restored);
        Assertions.assertNull(restored.getSchema().get(0).getDefineExpr());
        Assertions.assertEquals("mv_real", restored.getSchema().get(0).getName());
    }

    @Test
    public void testRejectNonSourceDirectEntryAndInvalidAnalysis() {
        MaterializedIndexMeta nonSource = definitionMeta(Lists.newArrayList(new Column("fiction", IntegerType.INT)));
        Assertions.assertThrows(SemanticException.class, () -> base.analyzeRollupIndexMeta(nonSource));
        Column missingReference = new Column("derived", IntegerType.INT);
        missingReference.setDefineExpr(SqlParser.parseSqlToExpr("fiction + 1", SqlModeHelper.MODE_DEFAULT));
        Assertions.assertThrows(SemanticException.class,
                () -> base.analyzeRollupIndexMeta(definitionMeta(Lists.newArrayList(missingReference))));
    }

    @Test
    public void testRejectAggregateWrappersInExecutableDefinition() {
        Column value = new Column("value", IntegerType.BIGINT);
        value.setDefineExpr(SqlParser.parseSqlToExpr("sum(v)", SqlModeHelper.MODE_DEFAULT));
        MaterializedIndexMeta definition = definitionMeta(Lists.newArrayList(value));
        Assertions.assertThrows(SemanticException.class, () -> base.analyzeRollupIndexMeta(definition));
        MaterializedIndexMeta whereAggregate = definitionMeta(Lists.newArrayList(new Column("k", IntegerType.INT)));
        whereAggregate.setSyncMvDefinition(SyncMaterializedViewDefinition.capture(whereAggregate.getSchema(),
                SqlParser.parseSqlToExpr("sum(v) > 0", SqlModeHelper.MODE_DEFAULT)));
        Assertions.assertThrows(SemanticException.class, () -> base.analyzeRollupIndexMeta(whereAggregate));
    }

    @Test
    public void testAuthoritativeDefinitionSurvivesSchemaReconciliation() {
        Column value = new Column("value", IntegerType.BIGINT);
        value.setDefineExpr(new IntLiteral(1, IntegerType.BIGINT));
        Column dropped = new Column("dropped", IntegerType.INT);
        dropped.setDefineExpr(new IntLiteral(2));
        MaterializedIndexMeta meta = definitionMeta(Lists.newArrayList(value, dropped));
        meta.setWhereClause(SqlParser.parseSqlToExpr("k > 0", SqlModeHelper.MODE_DEFAULT));
        meta.setSyncMvDefinition(SyncMaterializedViewDefinition.capture(meta.getSchema(), meta.getWhereClause()));
        meta.setSyncMvRequiredTxnId(42);
        meta.setDbId(123);
        MaterializedIndexMeta copy = meta.shallowCopy();
        Column shadow = new Column(value);
        shadow.setName("__starrocks_shadow_value");
        shadow.setDefineExpr(new IntLiteral(99));
        copy.setSchema(Lists.newArrayList(shadow, new Column("mv_real", IntegerType.INT)));
        MaterializedIndexMeta restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(copy), MaterializedIndexMeta.class);
        base.analyzeRollupIndexMeta(restored);
        Assertions.assertEquals(42, restored.getSyncMvRequiredTxnId());
        Assertions.assertEquals(123, restored.getDbId());
        Assertions.assertNotNull(restored.getWhereClause());
        Assertions.assertEquals(IntegerType.BIGINT, restored.getSchema().get(0).getDefineExpr().getType());
        Assertions.assertTrue(AstToSQLBuilder.toSQL(restored.getSchema().get(0).getDefineExpr()).contains("1"));
        Assertions.assertNull(restored.getSchema().get(1).getDefineExpr());
        Assertions.assertEquals(2, meta.getSchema().size());
        JsonObject original = GsonUtils.GSON.toJsonTree(meta.getSyncMvDefinition()).getAsJsonObject();
        Assertions.assertTrue(original.getAsJsonObject("columnExprSql").has("dropped"));
    }

    private static MaterializedIndexMeta definitionMeta(List<Column> schema) {
        MaterializedIndexMeta meta = meta(schema);
        meta.setSyncMvDefinition(SyncMaterializedViewDefinition.capture(schema, null));
        return meta;
    }

    private static MaterializedIndexMeta meta(List<Column> schema) {
        return new MaterializedIndexMeta(100, schema, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.AGG_KEYS, null);
    }
}
