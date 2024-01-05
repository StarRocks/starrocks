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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.FeConstants;
import com.starrocks.persist.gson.GsonUtils;

public class QueryDebugOptions {
    private static QueryDebugOptions INSTANCE = new QueryDebugOptions();

    // Whether to canonize predicate after mv rewrite which make uts more stable
    @SerializedName(value = "enableNormalizePredicateAfterMVRewrite")
    public boolean enableNormalizePredicateAfterMVRewrite = false;

    @SerializedName(value = "maxRefreshMaterializedViewRetryNum")
    private int maxRefreshMaterializedViewRetryNum = 1;

    @SerializedName(value = "enableMVEagerUnionAllRewrite")
    private boolean enableMVEagerUnionAllRewrite = false;

    @SerializedName(value = "enableQueryTraceLog")
    private boolean enableQueryTraceLog = false;

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

    public boolean isEnableMVEagerUnionAllRewrite() {
        return enableMVEagerUnionAllRewrite;
    }

    public void setEnableMVEagerUnionAllRewrite(boolean enableMVEagerUnionAllRewrite) {
        this.enableMVEagerUnionAllRewrite = enableMVEagerUnionAllRewrite;
    }

    public boolean isEnableQueryTraceLog() {
        return enableQueryTraceLog;
    }

    public void setEnableQueryTraceLog(boolean enableQueryTraceLog) {
        this.enableQueryTraceLog = enableQueryTraceLog;
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
