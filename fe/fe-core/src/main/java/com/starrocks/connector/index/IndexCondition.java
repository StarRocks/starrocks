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

import java.util.Objects;

/**
 * A connector-index request attached to a scan by the optimizer.
 *
 * <p>The original scan predicate remains on the scan until a connector-specific executor returns
 * a snapshot-bound {@link ConnectorIndexResult}. This makes the planning framework a no-op for
 * connectors that do not yet implement index execution.
 */
public class IndexCondition {
    private final ScalarOperator predicate;

    public IndexCondition(ScalarOperator predicate) {
        this.predicate = predicate;
    }

    public ScalarOperator getPredicate() {
        return predicate;
    }

    public ColumnRefSet getUsedColumns() {
        return predicate == null ? new ColumnRefSet() : predicate.getUsedColumns();
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
        return Objects.equals(predicate, that.predicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(predicate);
    }

    @Override
    public String toString() {
        return "predicate=" + predicate;
    }
}
