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

package com.starrocks.catalog.mv;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvBaseTableUpdateInfo;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static com.starrocks.catalog.MvRefreshArbiter.getMvBaseTableUpdateInfo;
import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;

public final class MVTimelinessNonPartitionArbiter extends MVTimelinessArbiter {
    private static final Logger LOG = LogManager.getLogger(MVTimelinessNonPartitionArbiter.class);

    public MVTimelinessNonPartitionArbiter(MaterializedView mv, boolean isQueryRewrite) {
        super(mv, isQueryRewrite);
    }

    /**
     * For non-partitioned materialized view, once its base table have updated, we need refresh the
     * materialized view's totally.
     * @return : non-partitioned materialized view's all need updated partition names.
     */
    @Override
    protected MvUpdateInfo getMVTimelinessUpdateInfoInChecked() {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        Preconditions.checkState(partitionInfo.isUnPartitioned());
        List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
        com.starrocks.catalog.TableProperty tableProperty = mv.getTableProperty();
        boolean isDisableExternalForceQueryRewrite = tableProperty != null &&
                tableProperty.getForceExternalTableQueryRewrite() == TableProperty.QueryRewriteConsistencyMode.DISABLE;
        for (BaseTableInfo tableInfo : baseTableInfos) {
            Table table = MvUtils.getTableChecked(tableInfo);
            // skip check freshness of view
            if (table.isView()) {
                continue;
            }

            // skip check external table if the external does not support rewrite.
            if (!table.isNativeTableOrMaterializedView() && isDisableExternalForceQueryRewrite) {
                logMVPrepare(mv, "Non-partitioned contains external table, and it's disabled query rewrite");
                return MvUpdateInfo.fullRefresh(mv);
            }

            // once mv's base table has updated, refresh the materialized view totally.
            MvBaseTableUpdateInfo mvBaseTableUpdateInfo = getMvBaseTableUpdateInfo(mv, table, true, isQueryRewrite);
            // TODO: fixme if mvBaseTableUpdateInfo is null, should return full refresh?
            if (mvBaseTableUpdateInfo != null &&
                    CollectionUtils.isNotEmpty(mvBaseTableUpdateInfo.getToRefreshPartitionNames())) {
                logMVPrepare(mv, "Non-partitioned base table has updated, need refresh totally.");
                return MvUpdateInfo.fullRefresh(mv);
            }
        }
        return MvUpdateInfo.noRefresh(mv);
    }

    @Override
    public MvUpdateInfo getMVTimelinessUpdateInfoInLoose() {
        List<Partition> partitions = Lists.newArrayList(mv.getPartitions());
        if (partitions.size() > 0 && partitions.get(0).getDefaultPhysicalPartition().getVisibleVersion() <= 1) {
            // the mv is newly created, can not use it to rewrite query.
            return MvUpdateInfo.fullRefresh(mv);
        }
        return MvUpdateInfo.noRefresh(mv);
    }

    @Override
    public MvUpdateInfo getMVTimelinessUpdateInfoInForceMVMode() {
        // for force mv mode, always no need to refresh for non-partitioned mv.
        return MvUpdateInfo.noRefresh(mv);
    }
}
