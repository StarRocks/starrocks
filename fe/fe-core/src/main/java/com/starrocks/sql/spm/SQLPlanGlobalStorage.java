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
package com.starrocks.sql.spm;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.starrocks.common.Config;
import com.starrocks.common.util.DateUtils;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

class SQLPlanGlobalStorage implements SQLPlanStorage {
    /*
     * SPM baselines table
     * id         | bigint
     * bind_sql   | varchar(65530)
     * bind_sql_digest | varchar(65530)
     * bind_sql_hash | bigint
     * plan_sql   | varchar(65530)
     * costs      | double
     * update_time| datetime
     */
    private static final Logger LOG = LogManager.getLogger(SQLPlanGlobalStorage.class);
    private static final String QUERY_SQL = "SELECT * FROM " + StatsConstants.SPM_BASELINE_TABLE_NAME + " ";
    private static final String INSERT_SQL = "INSERT INTO " + StatsConstants.SPM_BASELINE_TABLE_NAME + " VALUES ";
    private static final String DELETE_SQL = "DELETE FROM " + StatsConstants.SPM_BASELINE_TABLE_NAME + " WHERE id = ";

    // for lazy load baseline plan
    private record BaselineId(long id, long bindSQLHash) {}

    // thread safe
    private final Set<BaselineId> allBaselineIds = Sets.newConcurrentHashSet();

    private boolean hasInit = false;

    // cache
    private final BaselinePlanLoader loader = new BaselinePlanLoader();
    private final LoadingCache<Long, BaselinePlan> cache = Caffeine.newBuilder()
            .maximumSize(Config.max_spm_cache_baseline_size)
            .executor(Executors.newFixedThreadPool(1,
                    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("baseline-cache-%d").build()))
            .build(loader);
    private final SimpleExecutor executor = new SimpleExecutor("SPMExecutor", TResultSinkType.HTTP_PROTOCAL);

    public void init() {
        if (hasInit) {
            return;
        }
        List<BaselinePlan> bps = getAllBaselines();
        bps.forEach(bp -> cache.put(bp.getId(), bp));
        allBaselineIds.addAll(bps.stream().map(bp -> new BaselineId(bp.getId(), bp.getBindSqlHash())).toList());
        hasInit = true;
    }

    // only show stmt used, query be directly
    public List<BaselinePlan> getAllBaselines() {
        try {
            List<TResultBatch> datas = executor.executeDQL(QUERY_SQL);
            List<BaselinePlan> bps = castToBaselinePlan(datas);
            if (!hasInit) {
                bps.forEach(bp -> cache.put(bp.getId(), bp));
                allBaselineIds.addAll(bps.stream().map(bp -> new BaselineId(bp.getId(), bp.getBindSqlHash())).toList());
            }
            return bps;
        } catch (Exception e) {
            LOG.warn("sql plan baselines get all baseline fail", e);
            return List.of();
        }
    }

    public void storeBaselinePlan(BaselinePlan plan) {
        try {
            plan.setId(GlobalStateMgr.getCurrentState().getNextId());
            List<String> values = Lists.newArrayList();
            values.add(String.valueOf(plan.getId()));
            values.add("'" + plan.getBindSql() + "'");
            values.add("'" + plan.getBindSqlDigest() + "'");
            values.add(String.valueOf(plan.getBindSqlHash()));
            values.add("'" + plan.getPlanSql() + "'");
            values.add(String.valueOf(plan.getCosts()));
            values.add("'" + DateUtils.formatDateTimeUnix(plan.getUpdateTime()) + "'");
            String sql = INSERT_SQL + "(" + String.join(", ", values) + ");";
            executor.executeDML(sql);

            GlobalStateMgr.getCurrentState().getEditLog().logCreateSPMBaseline(plan);
            allBaselineIds.add(new BaselineId(plan.getId(), plan.getBindSqlHash()));
        } catch (Exception e) {
            LOG.warn("sql plan baselines store baseline fail", e);
        }
    }

    public List<BaselinePlan> findBaselinePlan(String sqlDigest, long hash) {
        init();
        List<Long> ids = allBaselineIds.stream()
                .filter(baselineId -> baselineId.bindSQLHash == hash).map(f -> f.id).toList();
        Map<Long, BaselinePlan> plans = cache.getAll(ids);
        return plans.values().stream().filter(plan -> plan.getBindSqlDigest().equals(sqlDigest)).toList();
    }

    public void dropBaselinePlan(long baseLineId) {
        try {
            List<BaselineId> ids = allBaselineIds.stream().filter(b -> b.id == baseLineId).toList();
            Preconditions.checkState(ids.size() == 1);
            for (BaselineId id : ids) {
                executor.executeDML(DELETE_SQL + id.id + ";");
                GlobalStateMgr.getCurrentState().getEditLog()
                        .logDropSPMBaseline(new BaselinePlan(id.id, id.bindSQLHash));
                allBaselineIds.remove(id);
                cache.invalidate(id.id);
            }
        } catch (Exception e) {
            LOG.warn("sql plan baselines drop baseline fail", e);
        }
    }

    // for ut test use
    public void dropAllBaselinePlans() {
        allBaselineIds.clear();
    }

    private List<BaselinePlan> castToBaselinePlan(List<TResultBatch> datas) {
        List<BaselinePlan> result = Lists.newArrayList();
        for (TResultBatch batch : ListUtils.emptyIfNull(datas)) {
            for (ByteBuffer buffer : batch.getRows()) {
                ByteBuf copied = Unpooled.copiedBuffer(buffer);
                String jsonString = copied.toString(Charset.defaultCharset());
                /*
                 * SPM baselines table
                 * 0.id         | bigint
                 * 1.bind_sql   | varchar(65530)
                 * 2.bind_sql_digest | varchar(65530)
                 * 3.bind_sql_hash | bigint
                 * 4.plan_sql   | varchar(65530)
                 * 5.costs      | double
                 * 6.update_time| datetime
                 */
                JsonElement obj = JsonParser.parseString(jsonString);
                JsonArray data = obj.getAsJsonObject().get("data").getAsJsonArray();
                BaselinePlan bp = new BaselinePlan(true, data.get(1).getAsString(), data.get(2).getAsString(),
                        data.get(3).getAsLong(), data.get(4).getAsString(), data.get(5).getAsDouble(),
                        DateUtils.parseUnixDateTime(data.get(6).getAsString()));
                bp.setId(data.get(0).getAsLong());
                result.add(bp);
            }
        }
        return result;
    }

    @Override
    public void replayBaselinePlan(BaselinePlan plan, boolean isCreate) {
        if (isCreate) {
            allBaselineIds.add(new BaselineId(plan.getId(), plan.getBindSqlHash()));
        } else {
            allBaselineIds.removeIf(b -> b.id == plan.getId());
        }
    }

    private class BaselinePlanLoader implements CacheLoader<Long, BaselinePlan> {
        @Override
        public BaselinePlan load(@NonNull Long key) {
            try {
                List<TResultBatch> datas = executor.executeDQL(QUERY_SQL + "WHERE id = " + key + ";");
                if (!datas.isEmpty()) {
                    return castToBaselinePlan(datas).get(0);
                }
            } catch (Exception e) {
                LOG.warn("sql plan baselines load baseline fail", e);
            }
            return null;
        }

        @NonNull
        @Override
        public Map<Long, BaselinePlan> loadAll(Iterable<? extends Long> keys) {
            List<String> ks = Lists.newArrayList();
            keys.forEach(key -> ks.add(String.valueOf(key)));
            try {
                List<TResultBatch> datas =
                        executor.executeDQL(QUERY_SQL + "WHERE id IN (" + String.join(", ", ks) + ");");
                return castToBaselinePlan(datas).stream().collect(Collectors.toMap(BaselinePlan::getId, b -> b));
            } catch (Exception e) {
                LOG.warn("sql plan baselines load all baseline fail", e);
            }
            return Map.of();
        }
    }

}
