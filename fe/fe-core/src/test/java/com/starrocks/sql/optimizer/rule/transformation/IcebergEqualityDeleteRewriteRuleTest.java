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

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.IcebergTable.DATA_SEQUENCE_NUMBER;

public class IcebergEqualityDeleteRewriteRuleTest {

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

        ScalarOperator onPredicate = IcebergEqualityDeleteScanBuilder.buildOnPredicate(
                leftCols, List.of(rightId, rightSeq));

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
