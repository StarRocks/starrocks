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

package com.starrocks.sql.common;

import com.google.api.client.util.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.FeConstants;
import com.starrocks.common.profile.Tracers;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TDebugAction;
import com.starrocks.thrift.TExecDebugOption;
import org.apache.logging.log4j.util.Strings;

import java.util.List;

public class QueryDebugOptions {
    private static QueryDebugOptions INSTANCE = new QueryDebugOptions();

    // Whether to canonize predicate after mv rewrite which make uts more stable
    @SerializedName(value = "enableNormalizePredicateAfterMVRewrite")
    public boolean enableNormalizePredicateAfterMVRewrite = false;

    @SerializedName(value = "maxRefreshMaterializedViewRetryNum")
    private int maxRefreshMaterializedViewRetryNum = 1;

    @SerializedName(value = "enableQueryTraceLog")
    private boolean enableQueryTraceLog = false;

    @SerializedName(value = "mvRefreshTraceMode")
    private String mvRefreshTraceMode;

    @SerializedName(value = "mvRefreshTraceModule")
    private String mvRefreshTraceModule;

    public static class ExecDebugOption {
        @SerializedName(value = "plan_node_id")
        private int planNodeId;
        @SerializedName(value = "debug_action")
        private String debugAction;
        @SerializedName(value = "value")
        private int value = 0;
        public TExecDebugOption toThirft() {
            final TExecDebugOption option = new TExecDebugOption();
            option.setDebug_node_id(planNodeId);
            option.setDebug_action(TDebugAction.valueOf(debugAction));
            option.setValue(value);
            return option;
        }
    }
    @SerializedName(value = "execDebugOptions")
    private List<ExecDebugOption> execDebugOptions = Lists.newArrayList();

    public QueryDebugOptions() {
        // To make unit test more stable, add retry times for refreshing materialized views.
        if (FeConstants.runningUnitTest) {
            this.maxRefreshMaterializedViewRetryNum = 3;
        }
    }

    public boolean isEnableNormalizePredicateAfterMVRewrite() {
        return enableNormalizePredicateAfterMVRewrite;
    }

    public void setEnableNormalizePredicateAfterMVRewrite(boolean enableNormalizePredicateAfterMVRewrite) {
        this.enableNormalizePredicateAfterMVRewrite = enableNormalizePredicateAfterMVRewrite;
    }

    public int getMaxRefreshMaterializedViewRetryNum() {
        return maxRefreshMaterializedViewRetryNum;
    }

    public void setMaxRefreshMaterializedViewRetryNum(int maxRefreshMaterializedViewRetryNum) {
        this.maxRefreshMaterializedViewRetryNum = maxRefreshMaterializedViewRetryNum;
    }

    public boolean isEnableQueryTraceLog() {
        return enableQueryTraceLog;
    }

    public void setEnableQueryTraceLog(boolean enableQueryTraceLog) {
        this.enableQueryTraceLog = enableQueryTraceLog;
    }

    public Tracers.Mode getMvRefreshTraceMode() {
        return Strings.isEmpty(mvRefreshTraceMode) ? Tracers.Mode.TIMER : Tracers.Mode.valueOf(mvRefreshTraceMode);
    }

    public Tracers.Module getMvRefreshTraceModule() {
        return Strings.isEmpty(mvRefreshTraceModule) ? Tracers.Module.BASE : Tracers.Module.valueOf(mvRefreshTraceModule);
    }

    public List<ExecDebugOption> getExecDebugOptions() {
        return execDebugOptions;
    }

    public static QueryDebugOptions getInstance() {
        return INSTANCE;
    }

    public static QueryDebugOptions read(String json) {
        return GsonUtils.GSON.fromJson(json, QueryDebugOptions.class);
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }
}
