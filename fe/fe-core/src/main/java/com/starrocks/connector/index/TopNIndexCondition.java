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

import java.util.Map;
import java.util.Objects;

/** A scored connector-index request whose candidates are globally ordered by the original TopN. */
public final class TopNIndexCondition extends IndexCondition {
    private final ScalarOperator scoreExpression;
    private final long limit;
    private final long offset;
    private final boolean ascending;

    public TopNIndexCondition(ScalarOperator predicate, ScalarOperator scoreExpression,
                              Map<String, ConnectorIndexType> requiredIndexes,
                              long limit, long offset, boolean ascending) {
        super(predicate, requireIndexes(requiredIndexes));
        this.scoreExpression = Objects.requireNonNull(scoreExpression, "scoreExpression is null");
        if (limit <= 0 || offset < 0 || limit > Integer.MAX_VALUE - offset) {
            throw new IllegalArgumentException("TopN candidate count must be between 1 and Integer.MAX_VALUE");
        }
        this.limit = limit;
        this.offset = offset;
        this.ascending = ascending;
    }

    private static Map<String, ConnectorIndexType> requireIndexes(
            Map<String, ConnectorIndexType> requiredIndexes) {
        Objects.requireNonNull(requiredIndexes, "requiredIndexes is null");
        if (requiredIndexes.isEmpty()) {
            throw new IllegalArgumentException("TopN must require at least one connector index");
        }
        return requiredIndexes;
    }

    public ScalarOperator getScoreExpression() {
        return scoreExpression;
    }

    public long getLimit() {
        return limit;
    }

    public long getOffset() {
        return offset;
    }

    public int getCandidateLimit() {
        return Math.toIntExact(limit + offset);
    }

    public boolean isAscending() {
        return ascending;
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet columns = super.getUsedColumns();
        columns.union(scoreExpression.getUsedColumns());
        return columns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TopNIndexCondition)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        TopNIndexCondition that = (TopNIndexCondition) o;
        return limit == that.limit && offset == that.offset && ascending == that.ascending
                && Objects.equals(scoreExpression, that.scoreExpression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), scoreExpression, limit, offset, ascending);
    }

    @Override
    public String toString() {
        return super.toString() + ", score=" + scoreExpression + ", limit=" + limit
                + ", offset=" + offset + ", ascending=" + ascending;
    }
}
