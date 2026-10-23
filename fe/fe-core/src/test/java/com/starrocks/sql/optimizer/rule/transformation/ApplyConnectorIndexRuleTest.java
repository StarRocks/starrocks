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
import com.starrocks.catalog.PaimonTable;
import com.starrocks.connector.index.ConnectorIndexDescriptor;
import com.starrocks.connector.index.ConnectorIndexDescriptor.Coverage;
import com.starrocks.connector.index.ConnectorIndexDescriptor.IndexType;
import com.starrocks.connector.index.ConnectorIndexDescriptor.Operation;
import com.starrocks.connector.index.ConnectorIndexMetadata;
import com.starrocks.connector.index.ConnectorIndexMetadata.TableType;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ApplyConnectorIndexRuleTest {
    private final ColumnRefOperator idColumn =
            new ColumnRefOperator(1, IntegerType.INT, "id", true);

    @Test
    public void testPredicateRuleAnnotatesScanWithoutRemovingPredicate() {
        BinaryPredicateOperator predicate =
                new BinaryPredicateOperator(BinaryType.EQ, idColumn, ConstantOperator.createInt(7));
        LogicalPaimonScanOperator scan = createScan(predicate);
        ConnectorIndexMetadata metadata = metadata(List.of(scalarDescriptor()));
        ApplyPredicateIndexRule rule = new ApplyPredicateIndexRule(
                OperatorType.LOGICAL_PAIMON_SCAN, ignored -> metadata);
        OptExpression input = OptExpression.create(scan);

        Assertions.assertTrue(rule.check(input, null));
        List<OptExpression> outputs = rule.transform(input, null);

        Assertions.assertEquals(1, outputs.size());
        LogicalPaimonScanOperator annotated = (LogicalPaimonScanOperator) outputs.get(0).getOp();
        Assertions.assertSame(predicate, annotated.getPredicate());
        Assertions.assertEquals(new IndexCondition(predicate, Map.of(1, 1)), annotated.getIndexCondition());
    }

    @Test
    public void testPredicateRulePrecheckAvoidsMetadataLoad() {
        AtomicInteger loads = new AtomicInteger();
        LogicalPaimonScanOperator scan = createScan(ConstantOperator.createBoolean(true));
        ApplyPredicateIndexRule rule = new ApplyPredicateIndexRule(
                OperatorType.LOGICAL_PAIMON_SCAN, ignored -> {
                    loads.incrementAndGet();
                    return metadata(List.of(scalarDescriptor()));
                });
        OptExpression input = OptExpression.create(scan);

        Assertions.assertFalse(rule.check(input, null));
        Assertions.assertTrue(rule.transform(input, null).isEmpty());
        Assertions.assertEquals(0, loads.get());
    }

    @Test
    public void testPartitionPredicateShapesSkipMetadataLoad() {
        PaimonTable partitionedTable = new PaimonTable() {
            @Override
            public List<String> getPartitionColumnNames() {
                return List.of("id");
            }
        };
        Column id = new Column("id", IntegerType.INT);
        ApplyPredicateIndexRule rule = new ApplyPredicateIndexRule(
                OperatorType.LOGICAL_PAIMON_SCAN, ignored -> {
                    Assertions.fail("Partition-only predicates must not load index metadata");
                    return ConnectorIndexMetadata.empty();
                });
        for (ScalarOperator predicate : List.of(
                new BinaryPredicateOperator(BinaryType.EQ, idColumn, ConstantOperator.createInt(7)),
                new InPredicateOperator(false, List.of(idColumn, ConstantOperator.createInt(7))),
                new IsNullPredicateOperator(false, idColumn))) {
            LogicalPaimonScanOperator scan = new LogicalPaimonScanOperator(partitionedTable,
                    Map.of(idColumn, id), Map.of(id, idColumn), -1, predicate);
            OptExpression input = OptExpression.create(scan);
            Assertions.assertFalse(rule.check(input, null));
            Assertions.assertTrue(rule.transform(input, null).isEmpty());
        }
    }

    private ConnectorIndexMetadata metadata(List<ConnectorIndexDescriptor> descriptors) {
        return ConnectorIndexMetadata.of(42, TableType.DATA_EVOLUTION, descriptors);
    }

    private ConnectorIndexDescriptor scalarDescriptor() {
        return new ConnectorIndexDescriptor(IndexType.BITMAP, "test", idColumn.getId(),
                idColumn.getName(), Map.of(), Set.of(Operation.EQUAL), Coverage.FULL);
    }

    private LogicalPaimonScanOperator createScan(ScalarOperator predicate) {
        Column id = new Column(idColumn.getName(), IntegerType.INT);
        return new LogicalPaimonScanOperator(new PaimonTable(),
                Map.of(idColumn, id), Map.of(id, idColumn), -1, predicate);
    }
}
