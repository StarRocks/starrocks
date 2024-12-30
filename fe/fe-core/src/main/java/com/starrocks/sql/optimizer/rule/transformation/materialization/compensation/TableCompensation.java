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

package com.starrocks.sql.optimizer.rule.transformation.materialization.compensation;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTransparentState;

/**
 * To support mv compensation(transparent) rewrite, we need to compensate some partitions from the defined query of mv
 * if some of mv's base tables have updated/refreshed.
 * </p>
 * There are some differences for different types of tables to compensate:
 * - OlapTable, partition ids that are already updated.
 * - ExternalTable, partition keys that are already changed.
 */
public abstract class TableCompensation {
    protected final Table refBaseTable;
    protected final MVTransparentState state;

    public TableCompensation(Table refBaseTable, MVTransparentState state) {
        this.refBaseTable = refBaseTable;
        this.state = state;
    }

    public MVTransparentState getState() {
        return state;
    }

    public boolean isNoCompensate() {
        return state.isNoCompensate();
    }

    public boolean isUncompensable() {
        return state.isUncompensable();
    }

    /**
     * Compensate the query's scan operator with the given compensation.
     * NOTE: This method should be called only when input query is needed to be compensated which means
     * MV's refreshed partitions cannot satisfy the query's selected partitions.
     */
    public LogicalScanOperator compensate(OptimizerContext optimizerContext,
                                          MaterializedView mv,
                                          LogicalScanOperator scanOperator) {
        // do nothing
        return null;
    }

    @Override
    public String toString() {
        return state.toString();
    }

    public static class NoCompensation extends TableCompensation {
        public NoCompensation() {
            super(null, MVTransparentState.NO_COMPENSATE);
        }
    }

    public static class UnknownCompensation extends TableCompensation {
        public UnknownCompensation() {
            super(null, MVTransparentState.UNKNOWN);
        }
    }

    public static class NoRewriteCompensation extends TableCompensation {
        public NoRewriteCompensation() {
            super(null, MVTransparentState.NO_REWRITE);
        }
    }

    public static TableCompensation noCompensation() {
        return new NoCompensation();
    }

    public static TableCompensation noRewrite() {
        return new NoRewriteCompensation();
    }

    public static TableCompensation unknown() {
        return new UnknownCompensation();
    }

    public static TableCompensation create(MVTransparentState state) {
        switch (state) {
            case NO_COMPENSATE:
                return noCompensation();
            case NO_REWRITE:
                return noRewrite();
            case UNKNOWN:
                return unknown();
            default:
                throw new IllegalArgumentException("Unknown state: " + state);
        }
    }
}
