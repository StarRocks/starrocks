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

package com.starrocks.connector.index;

import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A connector-index request attached to a scan by the optimizer.
 *
 * <p>The original scan predicate remains on the scan. Index execution only supplies candidate row
 * IDs; it must not replace SQL predicate evaluation. Field bindings use connector field IDs rather
 * than column names, which can change independently of the index files.
 */
public class IndexCondition {
    private final ScalarOperator predicate;
    private final Map<Integer, Integer> fieldIds;

    public IndexCondition(ScalarOperator predicate) {
        this(predicate, Map.of());
    }

    public IndexCondition(ScalarOperator predicate, Map<Integer, Integer> fieldIds) {
        this.predicate = predicate;
        this.fieldIds = Map.copyOf(fieldIds);
    }

    public Map<Integer, Integer> getFieldIds() {
        return fieldIds;
    }

    public ScalarOperator getPredicate() {
        return predicate;
    }

    public ColumnRefSet getUsedColumns() {
        return predicate == null ? new ColumnRefSet() : predicate.getUsedColumns();
    }

    /** Returns a new condition when a plan copy assigns new column references to its scan. */
    public IndexCondition rewriteColumnRefs(ReplaceColumnRefRewriter rewriter) {
        Map<Integer, Integer> rewrittenIds = new HashMap<>();
        if (predicate != null) {
            predicate.getColumnRefs().forEach(column -> {
                Integer fieldId = fieldIds.get(column.getId());
                if (fieldId != null) {
                    ScalarOperator rewritten = rewriter.rewrite(column);
                    if (rewritten instanceof ColumnRefOperator) {
                        rewrittenIds.put(((ColumnRefOperator) rewritten).getId(),
                                fieldId);
                    }
                }
            });
        }
        return new IndexCondition(rewriter.rewrite(predicate), rewrittenIds);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexCondition that = (IndexCondition) o;
        return Objects.equals(predicate, that.predicate) && Objects.equals(fieldIds, that.fieldIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(predicate, fieldIds);
    }

    @Override
    public String toString() {
        return "predicate=" + predicate;
    }
}
