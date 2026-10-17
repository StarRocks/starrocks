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

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import com.starrocks.type.IntegerType;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.catalog.IcebergTable.DATA_SEQUENCE_NUMBER;

public class IcebergEqualityDeleteRewriteRuleTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCopiedEqualityDeleteScanIsNotExpandedAgain(boolean duplicatePlan) {
        BaseTable nativeTable = Mockito.mock(BaseTable.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(nativeTable.schema()).thenReturn(new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get())));
        Mockito.when(nativeTable.spec()).thenReturn(PartitionSpec.unpartitioned());
        Mockito.when(nativeTable.operations().current().formatVersion()).thenReturn(2);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(snapshot.summary()).thenReturn(Map.of("total-equality-deletes", "1"));
        Mockito.when(nativeTable.snapshot(1L)).thenReturn(snapshot);

        Column id = new Column("id", IntegerType.INT, true);
        IcebergTable table = new IcebergTable(1, "source", "iceberg", null, "db", "source",
                "", List.of(id), nativeTable, Map.of());
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, true);
        factory.updateColumnRefToColumns(idRef, id, table);
        factory.updateColumnToRelationIds(idRef.getId(), factory.getNextRelationId());
        LogicalIcebergScanOperator original = new LogicalIcebergScanOperator(table,
                Map.of(idRef, id), Map.of(id, idRef), -1, null, TvrTableSnapshot.of(1L));
        OptimizerContext context = Mockito.mock(OptimizerContext.class);
        Mockito.when(context.getColumnRefFactory()).thenReturn(factory);
        Mockito.when(context.getSessionVariable()).thenReturn(new SessionVariable());
        DeleteFile deleteFile = Mockito.mock(DeleteFile.class);
        Mockito.when(deleteFile.equalityFieldIds()).thenReturn(List.of(1));
        Mockito.when(deleteFile.specId()).thenReturn(0);
        MetadataMgr metadata = Mockito.mock(MetadataMgr.class);
        Mockito.when(metadata.getDeleteFiles(Mockito.any(IcebergTable.class), Mockito.eq(1L),
                Mockito.any(), Mockito.eq(FileContent.EQUALITY_DELETES))).thenReturn(Set.of(deleteFile));
        GlobalStateMgr state = Mockito.mock(GlobalStateMgr.class);
        Mockito.when(state.getMetadataMgr()).thenReturn(metadata);
        try (MockedStatic<GlobalStateMgr> global = Mockito.mockStatic(GlobalStateMgr.class)) {
            global.when(GlobalStateMgr::getCurrentState).thenReturn(state);
            IcebergEqualityDeleteRewriteRule rule = new IcebergEqualityDeleteRewriteRule();
            OptExpression input = OptExpression.create(original);
            Assertions.assertTrue(rule.check(input, context), "the original scan must apply equality deletes");
            OptExpression rewritten = rule.transform(input, context).get(0);
            LogicalIcebergScanOperator withDeletes = findDataWithDeletes(rewritten);
            Assertions.assertNotNull(withDeletes);
            LogicalIcebergScanOperator copied = duplicatePlan
                    ? (LogicalIcebergScanOperator) new OptExpressionDuplicator(factory, context)
                            .duplicate(OptExpression.create(withDeletes)).getOp()
                    : (LogicalIcebergScanOperator) OperatorBuilderFactory.build(withDeletes)
                            .withOperator(withDeletes).build();

            Assertions.assertTrue(copied.getColumnNameToColRefMap().containsKey(DATA_SEQUENCE_NUMBER));
            Assertions.assertEquals(withDeletes.getMORParam(), copied.getMORParam());
            Assertions.assertEquals(withDeletes.getTableFullMORParams(), copied.getTableFullMORParams());
            // MV definition plans can already contain the equality-delete expansion when they are
            // duplicated into a new query. Running its rewrite pass must not add the synthetic columns twice.
            Assertions.assertDoesNotThrow(() -> {
                OptExpression copiedInput = OptExpression.create(copied);
                if (rule.check(copiedInput, context)) {
                    rule.transform(copiedInput, context);
                }
            });
            Assertions.assertFalse(rule.check(OptExpression.create(copied), context));
        }
    }

    private LogicalIcebergScanOperator findDataWithDeletes(OptExpression expression) {
        if (expression.getOp() instanceof LogicalIcebergScanOperator scan
                && scan.getMORParam() == IcebergMORParams.DATA_FILE_WITH_EQ_DELETE) {
            return scan;
        }
        for (OptExpression child : expression.getInputs()) {
            LogicalIcebergScanOperator scan = findDataWithDeletes(child);
            if (scan != null) {
                return scan;
            }
        }
        return null;
    }

    // The LEFT ANTI JOIN that applies equality deletes must compare identity columns with null-safe
    // equals (EQ_FOR_NULL / <=>), not plain EQ: per the Iceberg spec a NULL value in a delete column
    // matches a NULL row value, and plain EQ would evaluate NULL = NULL -> UNKNOWN and silently keep
    // the row alive. The $data_sequence_number bound must stay a strict LT.
    @Test
    public void testBuildOnPredicateUsesNullSafeEqualsForIdentityColumns() {
        ColumnRefOperator leftId = new ColumnRefOperator(1, IntegerType.INT, "id", true);
        ColumnRefOperator leftSeq = new ColumnRefOperator(2, IntegerType.BIGINT, DATA_SEQUENCE_NUMBER, true);
        ColumnRefOperator rightId = new ColumnRefOperator(3, IntegerType.INT, "id", true);
        ColumnRefOperator rightSeq = new ColumnRefOperator(4, IntegerType.BIGINT, DATA_SEQUENCE_NUMBER, true);

        Map<String, ColumnRefOperator> leftCols = new HashMap<>();
        leftCols.put("id", leftId);
        leftCols.put(DATA_SEQUENCE_NUMBER, leftSeq);

        IcebergEqualityDeleteRewriteRule rule = new IcebergEqualityDeleteRewriteRule();
        ScalarOperator onPredicate = Deencapsulation.invoke(
                rule, "buildOnPredicate", leftCols, List.of(rightId, rightSeq));

        Map<String, BinaryType> byColumn = new HashMap<>();
        collectBinaryTypes(onPredicate, byColumn);

        Assertions.assertEquals(BinaryType.EQ_FOR_NULL, byColumn.get("id"),
                "identity column must use null-safe equals so NULL-key rows are deleted");
        Assertions.assertEquals(BinaryType.LT, byColumn.get(DATA_SEQUENCE_NUMBER),
                "data sequence number bound must remain a strict less-than");
    }

    // Walk the AND tree and index each leaf comparison by the name of its right-hand (delete-table) column.
    private void collectBinaryTypes(ScalarOperator op, Map<String, BinaryType> out) {
        if (op instanceof BinaryPredicateOperator binary) {
            ColumnRefOperator rightCol = (ColumnRefOperator) binary.getChild(1);
            out.put(rightCol.getName(), binary.getBinaryType());
            return;
        }
        for (ScalarOperator child : op.getChildren()) {
            collectBinaryTypes(child, out);
        }
    }
}
