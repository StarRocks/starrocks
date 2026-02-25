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

package com.starrocks.sql.optimizer.rule.mv;

import com.google.common.base.Strings;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * To avoid MvRewriteProcessor cost too much optimizer time, reduce all related mvs to a limited size.
 * <h3>Why to Choose The Best Related MVs Strategy</h3>
 *
 * <p>Why still to choose limited related mvs from all active mvs?</p>
 *
 * <p>1. optimizer time cost. Even there is MVPlanCache to reduce mv optimizer plan time, but it still may cost
 * too much time for mv preprocessor, because mv optimizer plan costs too much time because of cold start when
 * MVPlanCache has no cache, or MVPlanCache has exceeded limited capacity which is 1000 by default.</p>
 *
 * <p>2. check mv's freshness for each mv will cost too much time if there are too many mvs.</p>
 *
 * <h3>How to Choose The Best Related MVs Strategy</h3>
 * <p>
 *     Choose the best related mvs from all active mvs as following order:
 *     1. find the max intersected table num between mv and query which means it's better for rewrite.
 *     2. find the latest fresh mv which means its freshness is better.
 * </p>
 *
 * <h3>More Information</h3>
 * <p>
 *  NOTE: there are still some limitations about this ordering algorithm:
 *  1. consider repeated tables in one mv later which one table can be used repeatedly in one mv.
 *  2. consider random factor so can cache and use more mvs.
 * </p>
 */
public class MVCorrelation implements Comparable<MVCorrelation> {
    private final MaterializedView mv;
    private final long mvQueryIntersectedTablesNum;
    private final int mvQueryScanOpNumDiff;
    private final long mvRefreshTimestamp;
    private final int level;

    public MVCorrelation(MaterializedView mv,
                         long mvQueryIntersectedTablesNum,
                         int mvQueryScanOpNumDiff,
                         long mvRefreshTimestamp,
                         int level) {
        this.mv = mv;
        this.level = level;
        this.mvQueryIntersectedTablesNum = mvQueryIntersectedTablesNum;
        this.mvQueryScanOpNumDiff = mvQueryScanOpNumDiff;
        this.mvRefreshTimestamp = mvRefreshTimestamp;
    }

    public MVCorrelation(MaterializedView mv,
                         long mvQueryIntersectedTablesNum,
                         int mvQueryScanOpNumDiff,
                         long mvRefreshTimestamp) {
        this(mv, mvQueryIntersectedTablesNum, mvQueryScanOpNumDiff, mvRefreshTimestamp, 0);
    }

    public MaterializedView getMv() {
        return this.mv;
    }

    public MaterializedViewWrapper getWrapper() {
        return MaterializedViewWrapper.create(mv, level);
    }

    public static long getMvQueryIntersectedTableNum(List<BaseTableInfo> baseTableInfos,
                                                     Set<String> queryTableNames) {
        return baseTableInfos.stream()
                .filter(baseTableInfo -> {
                    String baseTableName = baseTableInfo.getTableName();
                    // assert not null
                    if (Strings.isNullOrEmpty(baseTableName)) {
                        return false;
                    }
                    return queryTableNames.contains(baseTableName);
                }).count();
    }

    public static int getMvQueryScanOpDiff(List<MvPlanContext> planContexts,
                                           int mvBaseTableSize,
                                           int queryScanOpNum) {
        int diff = Math.abs(queryScanOpNum - mvBaseTableSize);
        if (planContexts == null || planContexts.isEmpty()) {
            return diff;
        }
        return planContexts.stream()
                .map(mvPlanContext -> mvPlanContext.getMvScanOpNum())
                .map(num -> Math.abs(queryScanOpNum - num))
                .min(Comparator.comparing(Integer::intValue))
                .orElse(diff);
    }

    /**
     * MVCorrelation is a min heap, so the first element is the largest one, we need to keep the best mv in the last and
     * remove the worst mv in the first.
     */
    @Override
    public int compareTo(@NotNull MVCorrelation other) {
        // compare level, less is better
        int result = Integer.compare(other.level, this.level);
        if (result != 0) {
            return result;
        }
        // compare intersected table nums, larger is better.
        result = Long.compare(this.mvQueryIntersectedTablesNum, other.mvQueryIntersectedTablesNum);
        if (result != 0) {
            return result;
        }
        // compare base table num diff,  less is better
        result = Integer.compare(other.mvQueryScanOpNumDiff, this.mvQueryScanOpNumDiff);
        if (result != 0) {
            return result;
        }
        // compare refresh timestamp, larger is better.
        return Long.compare(this.mvRefreshTimestamp, other.mvRefreshTimestamp);
    }

    @Override
    public String toString() {
        return String.format("Correlation: mv=%s, mvQueryInteractedTablesNum=%s, " +
                        "mvQueryScanOpNumDiff=%s, mvRefreshTimestamp=%s", mv.getName(),
                mvQueryIntersectedTablesNum, mvQueryScanOpNumDiff, mvRefreshTimestamp);
    }
}