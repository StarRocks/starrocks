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

package com.starrocks.sql.optimizer.operator;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class ScanOperatorPredicatesTest {

    @Test
    public void testCloneAndClearWithoutPartitionNames() {
        ScanOperatorPredicates predicates = new ScanOperatorPredicates();
        PartitionKey partitionKey = new PartitionKey();
        partitionKey.pushColumn(new IntLiteral(1), PrimitiveType.INT);
        predicates.getIdToPartitionKey().put(1L, partitionKey);
        predicates.setSelectedPartitionIds(new ArrayList<>(Collections.singletonList(1L)));

        ScalarOperator partitionConjunct = ConstantOperator.createInt(1);
        predicates.getPartitionConjuncts().add(partitionConjunct);
        predicates.getNoEvalPartitionConjuncts().add(partitionConjunct);
        predicates.getNonPartitionConjuncts().add(partitionConjunct);
        predicates.getMinMaxConjuncts().add(partitionConjunct);
        ColumnRefOperator columnRef = new ColumnRefOperator(1, IntegerType.INT, "col", true);
        predicates.getMinMaxColumnRefMap().put(columnRef, new Column("col", IntegerType.INT));

        ScanOperatorPredicates cloned = predicates.clone();
        assertThat(cloned).isEqualTo(predicates);
        assertThat(cloned.hashCode()).isEqualTo(predicates.hashCode());
        assertThat(cloned.getIdToPartitionKey()).containsEntry(1L, partitionKey);
        assertThat(cloned.getSelectedPartitionIds()).containsExactly(1L);
        assertThat(cloned.getPartitionConjuncts()).containsExactly(partitionConjunct);

        predicates.clear();
        assertThat(predicates.getIdToPartitionKey()).isEmpty();
        assertThat(predicates.getSelectedPartitionIds()).isEmpty();
        assertThat(predicates.getPartitionConjuncts()).isEmpty();
        assertThat(predicates.getNoEvalPartitionConjuncts()).isEmpty();
        assertThat(predicates.getNonPartitionConjuncts()).isEmpty();
        assertThat(predicates.getMinMaxConjuncts()).isEmpty();
        assertThat(predicates.getMinMaxColumnRefMap()).isEmpty();
    }
}
