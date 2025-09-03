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
import com.google.common.collect.ImmutableMap;
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
import com.starrocks.sql.Expr2SQLPrinter;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.statistic.StatisticUtils;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

class SQLPlanGlobalStorage implements SQLPlanStorage {
    private static final Logger LOG = LogManager.getLogger(SQLPlanGlobalStorage.class);
    /*
     * SPM baselines table
     * id         | bigint
     * is_enable  | boolean
     * bind_sql   | varchar(65530)
     * bind_sql_digest | varchar(65530)
     * bind_sql_hash | bigint
     * plan_sql   | varchar(65530)
     * costs      | double
     * query_ms   | double
     * source     | varchar(100)
     * update_time| datetime
     */
    private static final String QUERY_SQL = "SELECT * FROM " + StatsConstants.SPM_BASELINE_TABLE_NAME + " ";
    private static final String INSERT_SQL = "INSERT INTO " + StatsConstants.SPM_BASELINE_TABLE_NAME + " VALUES ";
    private static final String DELETE_SQL = "DELETE FROM " + StatsConstants.SPM_BASELINE_TABLE_NAME + " WHERE id IN ";
    private static final String UPDATE_SQL = "UPDATE " + StatsConstants.SPM_BASELINE_TABLE_NAME + " SET is_enable = ";
    private static final Map<String, String> BASELINE_FIELD_TO_COLUMN_MAPPING = ImmutableMap.<String, String>builder()
            .put("id", "id")
            .put("global", "global")
            .put("enable", "is_enable")
            .put("bindsqldigest", "bind_sql_digest")
            .put("bindsqlhash", "bind_sql_hash")
            .put("bindsql", "bind_sql")
            .put("plansql", "plan_sql")
            .put("costs", "costs")
            .put("queryms", "query_ms")
            .put("source", "source")
            .put("updatetime", "update_time")
            .build();

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
        List<BaselinePlan> bps = getBaselines(null);
        bps.forEach(bp -> cache.put(bp.getId(), bp));
        allBaselineIds.addAll(bps.stream().map(bp -> new BaselineId(bp.getId(), bp.getBindSqlHash())).toList());
        hasInit = true;
    }

    protected Optional<String> generateQuerySql(Expr where) {
        String sql = QUERY_SQL;
        if (where != null) {
            ColumnRefFactory factory = new ColumnRefFactory();
            ScalarOperator p = SqlToScalarOperatorTranslator.translateWithSlotRef(where, slotRef -> {
                if ("global".equalsIgnoreCase(slotRef.getColumnName())) {
                    return ConstantOperator.TRUE;
                }
                return factory.create(BASELINE_FIELD_TO_COLUMN_MAPPING.get(slotRef.getColumnName().toLowerCase()),
                        slotRef.getType(), true);
            });
            ScalarOperatorRewriter re = new ScalarOperatorRewriter();
            ScalarOperator result = re.rewrite(p, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
            if (!result.isConstantRef()) {
                Expr2SQLPrinter<Void> print = new Expr2SQLPrinter<>();
                sql += " WHERE " + print.print(result);
            } else if (result.isConstantNullOrFalse()) {
                return Optional.empty();
            }
        }
        return Optional.of(sql);
    }

    // only show stmt used, query be directly
    public List<BaselinePlan> getBaselines(Expr where) {
        if (!StatisticUtils.checkStatisticTables(List.of(StatsConstants.SPM_BASELINE_TABLE_NAME))) {
            return Collections.emptyList();
        }
        try {
            Optional<String> sql = generateQuerySql(where);
            if (sql.isEmpty()) {
                return Collections.emptyList();
            }
            List<TResultBatch> datas = executor.executeDQL(sql.get());
            List<BaselinePlan> bps = castToBaselinePlan(datas);
            if (!hasInit) {
                bps.forEach(bp -> cache.put(bp.getId(), bp));
                allBaselineIds.addAll(bps.stream().map(bp -> new BaselineId(bp.getId(), bp.getBindSqlHash())).toList());
            }
            return bps;
        } catch (Exception e) {
            LOG.warn("sql plan baselines get all baseline fail", e);
            return Collections.emptyList();
        }
    }

    public void storeBaselinePlan(List<BaselinePlan> plans) {
        try {
            if (!StatisticUtils.checkStatisticTables(List.of(StatsConstants.SPM_BASELINE_TABLE_NAME))) {
                return;
            }
            StringBuilder sb = new StringBuilder();
            sb.append(INSERT_SQL).append(" ");
            for (BaselinePlan plan : plans) {
                plan.setId(GlobalStateMgr.getCurrentState().getNextId());
                List<String> values = Lists.newArrayList();
                values.add(String.valueOf(plan.getId()));
                values.add(plan.isEnable() ? "true" : "false");
                values.add("'" + plan.getBindSql() + "'");
                values.add("'" + plan.getBindSqlDigest() + "'");
                values.add(String.valueOf(plan.getBindSqlHash()));
                values.add("'" + plan.getPlanSql() + "'");
                values.add(String.valueOf(plan.getCosts()));
                values.add(String.valueOf(plan.getQueryMs()));
                values.add("'" + plan.getSource() + "'");
                values.add("'" + DateUtils.formatDateTimeUnix(plan.getUpdateTime()) + "'");
                sb.append("(").append(String.join(",", values)).append("),");
            }
            sb.setLength(sb.length() - 1);
            sb.append(";");
            executor.executeDML(sb.toString());

            for (BaselinePlan plan : plans) {
                allBaselineIds.add(new BaselineId(plan.getId(), plan.getBindSqlHash()));
            }

            BaselinePlan.Info info = new BaselinePlan.Info();
            info.setReplayIds(plans.stream().map(BaselinePlan::getId).collect(Collectors.toList()));
            info.setReplayBindSQLHash(plans.stream().map(BaselinePlan::getBindSqlHash).collect(Collectors.toList()));
            GlobalStateMgr.getCurrentState().getEditLog().logCreateSPMBaseline(info);
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

    public void dropBaselinePlan(List<Long> baseLineIds) {
        try {
            List<BaselineId> ids = allBaselineIds.stream().filter(b -> baseLineIds.contains(b.id)).toList();
            for (BaselineId id : ids) {
                allBaselineIds.remove(id);
                cache.invalidate(id.id);
            }
            if (!StatisticUtils.checkStatisticTables(List.of(StatsConstants.SPM_BASELINE_TABLE_NAME))) {
                return;
            }

            String s = ids.stream().map(BaselineId::id).map(String::valueOf).collect(Collectors.joining(","));
            executor.executeDML(DELETE_SQL + "(" + s + ");");
            BaselinePlan.Info p = new BaselinePlan.Info();
            p.setReplayIds(ids.stream().map(BaselineId::id).collect(Collectors.toList()));
            GlobalStateMgr.getCurrentState().getEditLog().logDropSPMBaseline(p);
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
                 * 1.enable     | boolean
                 * 2.bind_sql   | varchar(65530)
                 * 3.bind_sql_digest | varchar(65530)
                 * 4.bind_sql_hash | bigint
                 * 5.plan_sql   | varchar(65530)
                 * 6.costs      | double
                 * 7.query_ms   | double
                 * 8.source     | varchar(100)
                 * 9.update_time| datetime
                 */
                JsonElement obj = JsonParser.parseString(jsonString);
                JsonArray data = obj.getAsJsonObject().get("data").getAsJsonArray();
                BaselinePlan bp =
                        new BaselinePlan(data.get(2).getAsString(), data.get(3).getAsString(), data.get(4).getAsLong(),
                                data.get(5).getAsString(), data.get(6).getAsDouble());
                bp.setId(data.get(0).getAsLong());
                bp.setGlobal(true);
                bp.setEnable(data.get(1).getAsInt() == 1);
                bp.setQueryMs(data.get(7).getAsDouble());
                bp.setSource(data.get(8).getAsString());
                bp.setUpdateTime(DateUtils.parseUnixDateTime(data.get(9).getAsString()));
                result.add(bp);
            }
        }
        return result;
    }

    @Override
    public void replayBaselinePlan(BaselinePlan.Info info, boolean isCreate) {
        if (isCreate) {
            for (int i = 0; i < info.getReplayIds().size(); i++) {
                allBaselineIds.add(new BaselineId(info.getReplayIds().get(i), info.getReplayBindSQLHash().get(i)));
            }
        } else {
            allBaselineIds.removeIf(b -> info.getReplayIds().contains(b.id));
        }
    }

    public void replayUpdateBaselinePlan(BaselinePlan.Info info, boolean isEnable) {
        // invalidate cache
        cache.invalidateAll(info.getReplayIds());
    }

    @Override
    public void controlBaselinePlan(boolean isEnable, List<Long> baseLineIds) {
        if (!StatisticUtils.checkStatisticTables(List.of(StatsConstants.SPM_BASELINE_TABLE_NAME))) {
            return;
        }
        List<Long> ids = allBaselineIds.stream()
                .filter(b -> baseLineIds.contains(b.id))
                .map(BaselineId::id)
                .toList();
        if (ids.isEmpty()) {
            return;
        }

        String sql = UPDATE_SQL + isEnable + " WHERE id IN (" +
                ids.stream().map(String::valueOf).collect(Collectors.joining(",")) + ");";
        try {
            executor.executeDML(sql);
        } catch (Exception e) {
            LOG.warn("sql plan baselines control baseline fail", e);
        }

        BaselinePlan.Info p = new BaselinePlan.Info();
        p.setReplayIds(ids);
        GlobalStateMgr.getCurrentState().getEditLog().logUpdateSPMBaseline(p, isEnable);
        // invalidate cache
        cache.invalidateAll(ids);
    }

    @Override
    public List<BaselinePlan> queryBaselinePlan(List<String> sqlDigest, String source) {
        if (sqlDigest.isEmpty()) {
            return List.of();
        }
        if (!StatisticUtils.checkStatisticTables(List.of(StatsConstants.SPM_BASELINE_TABLE_NAME))) {
            return Collections.emptyList();
        }
        StringBuilder sql = new StringBuilder(QUERY_SQL);
        sql.append("WHERE ");
        sql.append("source = ");
        sql.append("'").append(source).append("'");

        sql.append(" and bind_sql_digest IN (");
        sqlDigest.forEach(digest -> sql.append("'").append(digest).append("',"));
        sql.setLength(sql.length() - 1);
        sql.append(")");

        try {
            List<TResultBatch> datas = executor.executeDQL(sql.toString());
            return castToBaselinePlan(datas);
        } catch (Exception e) {
            LOG.warn("sql plan baselines get baseline failed, sql: " + sql, e);
            return List.of();
        }
    }

    private class BaselinePlanLoader implements CacheLoader<Long, BaselinePlan> {
        @Override
        public BaselinePlan load(@NonNull Long key) {
            try {
                if (!StatisticUtils.checkStatisticTables(List.of(StatsConstants.SPM_BASELINE_TABLE_NAME))) {
                    return null;
                }
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
            if (!StatisticUtils.checkStatisticTables(List.of(StatsConstants.SPM_BASELINE_TABLE_NAME))) {
                return Map.of();
            }
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
