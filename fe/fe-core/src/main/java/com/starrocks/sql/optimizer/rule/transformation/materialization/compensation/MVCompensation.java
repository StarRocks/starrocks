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

import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTransparentState;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
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
    private final Map<Table, BaseCompensation<?>> compensations;

    public MVCompensation(SessionVariable sessionVariable,
                          MVTransparentState state,
                          Map<Table, BaseCompensation<?>> compensations) {
        this.sessionVariable = sessionVariable;
        this.state = state;
        this.compensations = compensations;
    }

    public static MVCompensation createNoCompensateState(SessionVariable sessionVariable) {
        return new MVCompensation(sessionVariable, MVTransparentState.NO_COMPENSATE, null);
    }

    public static MVCompensation createNoRewriteState(SessionVariable sessionVariable) {
        return new MVCompensation(sessionVariable, MVTransparentState.NO_REWRITE, null);
    }

    public static MVCompensation createUnkownState(SessionVariable sessionVariable) {
        return new MVCompensation(sessionVariable, MVTransparentState.UNKNOWN, null);
    }

    public MVTransparentState getState() {
        return state;
    }

    public Map<Table, BaseCompensation<?>> getCompensations() {
        return compensations;
    }

    private boolean useTransparentRewrite() {
        return sessionVariable.isEnableMaterializedViewTransparentUnionRewrite();
    }

    public boolean isTransparentRewrite() {
        if (!useTransparentRewrite()) {
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
            return !useTransparentRewrite();
        } else {
            return true;
        }
    }

    @Override
    public String toString() {
        return "MvCompensation{" +
                "state=" + state +
                ", compensations=" + MvUtils.shrinkToSize(compensations, Config.max_mv_task_run_meta_message_values_length) +
                '}';
    }
}
