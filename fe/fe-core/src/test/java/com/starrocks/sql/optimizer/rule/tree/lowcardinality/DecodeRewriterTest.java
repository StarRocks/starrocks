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

package com.starrocks.sql.optimizer.rule.tree.lowcardinality;

import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class DecodeRewriterTest {

    private static PhysicalTopNOperator newPartitionTopN(ColumnRefOperator partitionRef) {
        return new PhysicalTopNOperator(
                new OrderSpec(List.of(new Ordering(partitionRef, true, true))),
                Operator.DEFAULT_LIMIT, 0,
                List.of(partitionRef),
                1,
                SortPhase.FINAL,
                TopNType.ROW_NUMBER,
                false, false, false,
                null, null, null);
    }

    private static OptExpression newOptExpression(Operator op) {
        return OptExpression.builder()
                .setOp(op)
                .setInputs(List.of())
                .setLogicalProperty(new LogicalProperty(new ColumnRefSet()))
                .build();
    }

    @Test
    public void testTopNPartitionColumnDecodedBelowKeepsStringRef() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator stringRef = factory.create("s", ScalarType.createVarcharType(128), true);
        ColumnRefOperator dictRef = factory.create("s", Type.INT, true);

        DecodeContext context = new DecodeContext(factory);
        context.stringRefToDictRefMap.put(stringRef, dictRef);

        PhysicalTopNOperator topN = newPartitionTopN(stringRef);
        // the column was decoded below this TopN (e.g. under a join): it does NOT
        // arrive in dict form, so inputStringColumns stays empty
        context.operatorDecodeInfo.put(topN, DecodeInfo.create());

        DecodeRewriter rewriter = new DecodeRewriter(factory, context, new SessionVariable());
        OptExpression result = rewriter.visitPhysicalTopN(newOptExpression(topN), new ColumnRefSet());

        PhysicalTopNOperator newTopN = result.getOp().cast();
        // partition-by must keep the string ref; the dict slot does not exist here and
        // referencing it fails on BE with "slot_id not found"
        Assertions.assertEquals(List.of(stringRef), newTopN.getPartitionByColumns());
        Assertions.assertEquals(stringRef, newTopN.getOrderSpec().getOrderDescs().get(0).getColumnRef());
    }

    @Test
    public void testTopNPartitionColumnInDictFormRewrittenToDictRef() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator stringRef = factory.create("s", ScalarType.createVarcharType(128), true);
        ColumnRefOperator dictRef = factory.create("s", Type.INT, true);

        DecodeContext context = new DecodeContext(factory);
        context.stringRefToDictRefMap.put(stringRef, dictRef);

        PhysicalTopNOperator topN = newPartitionTopN(stringRef);
        // the column arrives at this TopN still in dict form
        DecodeInfo info = DecodeInfo.create();
        info.inputStringColumns.union(new ColumnRefSet(stringRef.getId()));
        context.operatorDecodeInfo.put(topN, info);

        DecodeRewriter rewriter = new DecodeRewriter(factory, context, new SessionVariable());
        OptExpression result = rewriter.visitPhysicalTopN(newOptExpression(topN), new ColumnRefSet());

        PhysicalTopNOperator newTopN = result.getOp().cast();
        Assertions.assertEquals(List.of(dictRef), newTopN.getPartitionByColumns());
        Assertions.assertEquals(dictRef, newTopN.getOrderSpec().getOrderDescs().get(0).getColumnRef());
    }
}
