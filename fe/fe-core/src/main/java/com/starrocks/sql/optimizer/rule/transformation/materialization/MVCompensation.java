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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.catalog.PartitionKey;
import com.starrocks.qe.SessionVariable;

import java.util.List;

/**
 * MVCompensation is used to store the compensation information for materialized view in mv rewrite.
 */
public class MVCompensation {
    // The session variable of the current query.
    private final SessionVariable sessionVariable;
    // The state of compensation.
    private final MVTransparentState state;
    // The partition ids of olap table that need to be compensated.
    private final List<Long> olapCompensatePartitionIds;
    // The partition keys of external table that need to be compensated.
    private final List<PartitionKey> externalCompensatePartitionKeys;
    private final MVUnionRewriteMode unionRewriteMode;

    public MVCompensation(SessionVariable sessionVariable,
                          MVTransparentState state,
                          List<Long> olapCompensatePartitionIds,
                          List<PartitionKey> externalCompensatePartitionKeys) {
        this.sessionVariable = sessionVariable;
        this.state = state;
        this.olapCompensatePartitionIds = olapCompensatePartitionIds;
        this.externalCompensatePartitionKeys = externalCompensatePartitionKeys;
        this.unionRewriteMode = MVUnionRewriteMode.getInstance(sessionVariable.getMaterializedViewUnionRewriteMode());
    }

    public static MVCompensation createNoCompensateState(SessionVariable sessionVariable) {
        return new MVCompensation(sessionVariable, MVTransparentState.NO_COMPENSATE, null, null);
    }

    public static MVCompensation createNoRewriteState(SessionVariable sessionVariable) {
        return new MVCompensation(sessionVariable, MVTransparentState.NO_REWRITE, null, null);
    }

    public static MVCompensation createUnkownState(SessionVariable sessionVariable) {
        return new MVCompensation(sessionVariable, MVTransparentState.UNKNOWN, null, null);
    }

    public MVTransparentState getState() {
        return state;
    }

    public List<Long> getOlapCompensatePartitionIds() {
        return olapCompensatePartitionIds;
    }

    public List<PartitionKey> getExternalCompensatePartitionKeys() {
        return externalCompensatePartitionKeys;
    }

    public boolean isTransparentRewrite() {
        if (!unionRewriteMode.isTransparentRewrite()) {
            return false;
        }

        // No compensate once if mv's freshness is satisfied.
        if (state.isCompensate()) {
            return true;
        }

        return false;
    }

    public boolean isCompensatePartitionPredicate() {
        // always false if it's set to false from session variable
        if (!sessionVariable.isEnableMaterializedViewRewritePartitionCompensate()) {
            return false;
        }
        if (state.isNoCompensate()) {
            return false;
        } else if (state.isCompensate()) {
            return !unionRewriteMode.isTransparentRewrite();
        } else {
            return true;
        }
    }

    @Override
    public String toString() {
        return "MvCompensation{" +
                "state=" + state +
                ", olapCompensatePartitionIds=" + olapCompensatePartitionIds +
                ", externalCompensatePartitionKeys=" + externalCompensatePartitionKeys +
                '}';
    }
}
