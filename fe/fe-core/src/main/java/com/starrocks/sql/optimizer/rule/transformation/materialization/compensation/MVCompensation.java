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

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Table;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTransparentState;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * MVCompensation is used to store the compensation information for materialized view in mv rewrite.
 */
public class MVCompensation {
    private static final Logger LOG = LogManager.getLogger(MVCompensation.class);

    // The session variable of the current query.
    private final SessionVariable sessionVariable;
    // The state of compensation.
    private final MVTransparentState state;
    private final Map<Table, TableCompensation> compensations;

    public MVCompensation(SessionVariable sessionVariable,
                          MVTransparentState state,
                          Map<Table, TableCompensation> compensations) {
        this.sessionVariable = sessionVariable;
        this.state = state;
        if (state.isCompensate()) {
            Preconditions.checkArgument(compensations != null && !compensations.isEmpty());
        }
        this.compensations = compensations;
    }

    public boolean isNoCompensate() {
        if (state.isNoCompensate()) {
            return true;
        }
        if (state.isCompensate()) {
            if (CollectionUtils.sizeIsEmpty(compensations)) {
                return true;
            }
            if (compensations.values().stream().allMatch(e -> e.getState().isNoCompensate())) {
                return true;
            }
        }
        return false;
    }

    public boolean isUncompensable() {
        return state.isUncompensable();
    }

    public static MVCompensation compensate(SessionVariable sessionVariable,
                                            Map<Table, TableCompensation> compensations) {
        if (CollectionUtils.sizeIsEmpty(compensations) || compensations.values().stream()
                .allMatch(e -> e.isNoCompensate())) {
            return noCompensate(sessionVariable);
        } else {
            return new MVCompensation(sessionVariable, MVTransparentState.COMPENSATE, compensations);
        }
    }

    public static MVCompensation noCompensate(SessionVariable sessionVariable) {
        return new MVCompensation(sessionVariable, MVTransparentState.NO_COMPENSATE, null);
    }

    public static MVCompensation unknown(SessionVariable sessionVariable) {
        return new MVCompensation(sessionVariable, MVTransparentState.UNKNOWN, null);
    }

    public static MVCompensation withState(SessionVariable sessionVariable,
                                           MVTransparentState state) {
        Preconditions.checkArgument(state != MVTransparentState.COMPENSATE);
        return new MVCompensation(sessionVariable, state, null);
    }

    public MVTransparentState getState() {
        return state;
    }

    public Map<Table, TableCompensation> getCompensations() {
        return compensations;
    }

    public boolean isTransparentRewrite() {
        return state.isCompensate();
    }

    public boolean isCompensatePartitionPredicate() {
        // always false if it's set to false from session variable
        if (state.isNoCompensate() || state.isCompensate()) {
            return false;
        } else {
            return true;
        }
    }

    public boolean isTableNeedCompensate(Table table) {
        return !CollectionUtils.sizeIsEmpty(compensations) && compensations.containsKey(table);
    }

    public TableCompensation getTableCompensation(Table table) {
        return compensations.get(table);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("state=").append(state);
        if (!CollectionUtils.sizeIsEmpty(compensations)) {
            for (Map.Entry<Table, TableCompensation> entry : compensations.entrySet()) {
                sb.append(" [").append(entry.getKey().getName()).append(":").append(entry.getValue().toString()).append("]");
            }
        }
        return sb.toString();
    }
}
