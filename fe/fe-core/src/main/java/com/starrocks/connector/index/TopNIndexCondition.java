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
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Arrays;
import java.util.Objects;

/** A normalized vector-index request whose candidates remain guarded by the original TopN and predicate. */
public final class TopNIndexCondition extends IndexCondition {
    private final ScalarOperator partitionPredicate;
    private final ScalarOperator residualPredicate;
    private final ScalarOperator scoreExpression;
    private final int fieldId;
    private final String columnName;
    private final float[] queryVector;
    private final VectorIndexMetric metric;
    private final int k;
    private final long limit;
    private final long offset;
    private final boolean ascending;
    private final boolean nullsFirst;

    public TopNIndexCondition(ScalarOperator indexPredicate, ScalarOperator residualPredicate,
                              ScalarOperator scoreExpression, int fieldId, String columnName,
                              float[] queryVector, VectorIndexMetric metric, int k,
                              long limit, long offset, boolean ascending, boolean nullsFirst) {
        this(indexPredicate, null, residualPredicate, scoreExpression, fieldId, columnName,
                queryVector, metric, k, limit, offset, ascending, nullsFirst);
    }

    public TopNIndexCondition(ScalarOperator indexPredicate, ScalarOperator partitionPredicate,
                              ScalarOperator residualPredicate, ScalarOperator scoreExpression,
                              int fieldId, String columnName, float[] queryVector,
                              VectorIndexMetric metric, int k, long limit, long offset,
                              boolean ascending, boolean nullsFirst) {
        super(indexPredicate);
        this.partitionPredicate = partitionPredicate;
        this.residualPredicate = residualPredicate;
        this.scoreExpression = Objects.requireNonNull(scoreExpression, "scoreExpression is null");
        if (fieldId < 0) {
            throw new IllegalArgumentException("fieldId must be non-negative");
        }
        this.fieldId = fieldId;
        this.columnName = Objects.requireNonNull(columnName, "columnName is null");
        this.queryVector = Objects.requireNonNull(queryVector, "queryVector is null").clone();
        this.metric = Objects.requireNonNull(metric, "metric is null");
        if (this.queryVector.length == 0) {
            throw new IllegalArgumentException("queryVector must not be empty");
        }
        for (float value : this.queryVector) {
            if (!Float.isFinite(value)) {
                throw new IllegalArgumentException("queryVector values must be finite");
            }
        }
        if ((metric == VectorIndexMetric.L2) != ascending) {
            throw new IllegalArgumentException("metric and ordering direction are incompatible");
        }
        final long expectedK;
        try {
            expectedK = Math.addExact(limit, offset);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("limit plus offset overflows", e);
        }
        if (limit <= 0 || offset < 0 || expectedK <= 0 || expectedK > Integer.MAX_VALUE || k != expectedK) {
            throw new IllegalArgumentException("k must equal limit plus offset and fit in an int");
        }
        this.k = k;
        this.limit = limit;
        this.offset = offset;
        this.ascending = ascending;
        this.nullsFirst = nullsFirst;
    }

    public ScalarOperator getResidualPredicate() {
        return residualPredicate;
    }

    public ScalarOperator getPartitionPredicate() {
        return partitionPredicate;
    }

    public boolean hasResidual() {
        return residualPredicate != null;
    }

    public ScalarOperator getScoreExpression() {
        return scoreExpression;
    }

    public int getFieldId() {
        return fieldId;
    }

    public String getColumnName() {
        return columnName;
    }

    public float[] getQueryVector() {
        return queryVector.clone();
    }

    public VectorIndexMetric getMetric() {
        return metric;
    }

    public int getK() {
        return k;
    }

    public long getLimit() {
        return limit;
    }

    public long getOffset() {
        return offset;
    }

    public boolean isAscending() {
        return ascending;
    }

    public boolean isNullsFirst() {
        return nullsFirst;
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet columns = super.getUsedColumns();
        if (partitionPredicate != null) {
            columns.union(partitionPredicate.getUsedColumns());
        }
        if (residualPredicate != null) {
            columns.union(residualPredicate.getUsedColumns());
        }
        columns.union(scoreExpression.getUsedColumns());
        return columns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TopNIndexCondition) || !super.equals(o)) {
            return false;
        }
        TopNIndexCondition that = (TopNIndexCondition) o;
        return fieldId == that.fieldId && k == that.k && limit == that.limit && offset == that.offset
                && ascending == that.ascending && nullsFirst == that.nullsFirst
                && Objects.equals(partitionPredicate, that.partitionPredicate)
                && Objects.equals(residualPredicate, that.residualPredicate)
                && Objects.equals(scoreExpression, that.scoreExpression)
                && Objects.equals(columnName, that.columnName) && Arrays.equals(queryVector, that.queryVector)
                && metric == that.metric;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(), partitionPredicate, residualPredicate, scoreExpression, fieldId,
                columnName, metric, k, limit, offset, ascending, nullsFirst);
        return 31 * result + Arrays.hashCode(queryVector);
    }

    @Override
    public String toString() {
        return super.toString() + ", partition=" + partitionPredicate + ", residual=" + residualPredicate
                + ", score=" + scoreExpression
                + ", fieldId=" + fieldId + ", column=" + columnName + ", metric=" + metric
                + ", k=" + k + ", limit=" + limit + ", offset=" + offset
                + ", ascending=" + ascending + ", nullsFirst=" + nullsFirst;
    }
}
