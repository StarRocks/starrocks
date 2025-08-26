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

package com.starrocks.sql.automv.qe;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.TableName;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.FeConstants;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.epack.persist.SRMetaBlockIDEPack;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.automv.util.MetaUtil;
import com.starrocks.sql.automv.util.Result;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Collectors;

public class RecommendationsTaskManager extends FrontendDaemon {
    public static final ScheduledThreadPoolExecutor
            THREAD_POOL = ThreadPoolManager.newDaemonScheduledThreadPool(1, "show-recommendations-task-pool", true);
    private static final Logger LOG = LogManager.getLogger(RecommendationsTaskManager.class);
    private final Map<String, RecommendationsTaskStatus> taskStatusMap = Maps.newConcurrentMap();

    public RecommendationsTaskManager() {
    }

    public RecommendationsTaskStatus getTaskStatus(String taskName) {
        return taskStatusMap.get(taskName);
    }

    private boolean isExpired(RecommendationsTaskStatus taskStatus) {
        long expireTime = GlobalVariable.getAutoMVRecommendationsTaskExpireTime();
        Instant deadline = Instant.ofEpochMilli(taskStatus.getStartTime()).plus(expireTime, ChronoUnit.SECONDS);
        return deadline.isBefore(Instant.now());
    }

    public void trySubmitTask(RecommendationsTaskStatus taskStatus) {
        String taskName = taskStatus.getTaskName();
        RecommendationsTaskStatus duplicateTaskStatus = taskStatusMap.get(taskName);
        if (duplicateTaskStatus != null && duplicateTaskStatus.isPending()) {
            throw new SemanticException("Duplicate pending task '%s' exists", taskName);
        }
        long numPendingTasks = taskStatusMap.values()
                .stream().filter(status -> status.isPending() && !isExpired(status)).count();
        if (numPendingTasks >= GlobalVariable.getAutoMVRecommendationsTaskPendingLimit()) {
            throw new SemanticException("Too many pending tasks: num=%d, limit=%d", numPendingTasks,
                    GlobalVariable.getAutoMVRecommendationsTaskPendingLimit());
        }
        // submit many tasks concurrently
        duplicateTaskStatus = taskStatusMap.putIfAbsent(taskName, taskStatus);
        if (duplicateTaskStatus != null && duplicateTaskStatus.isPending()) {
            throw new SemanticException("Duplicate pending task '%s' exists", taskName);
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        // reset if the interval has been changed
        setInterval(10L * 1000L);

        if (FeConstants.runningUnitTest) {
            return;
        }
        ConnectContext ctx = new ConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        ctx.setSessionVariable(GlobalStateMgr.getCurrentState().getVariableMgr().newSessionVariable());
        ctx.getSessionVariable().setAutoMVDecayAcceleratedQueries(true);
        ctx.setThreadLocalInfo();

        try (ConnectContext.ScopeGuard scopedGuard = ctx.bindScope()) {
            List<RecommendationsTaskStatus> expiredList = taskStatusMap.values()
                    .stream().filter(this::isExpired).collect(Collectors.toList());
            CustomizedQueryExecutor executor = new CustomizedQueryExecutor();
            for (RecommendationsTaskStatus expiredTaskStatus : expiredList) {
                String resultTable = expiredTaskStatus.getResultTable();
                String taskName = expiredTaskStatus.getTaskName();
                String deleteSql = String.format("drop from %s where taskName='%s'", resultTable, taskName);
                Result.wrap(() -> executor.exec(ctx, deleteSql))
                        .ifError(err -> LOG.error("Fail to remove results of RecommendationsTasK '{}'", taskName, err));
                boolean tableExists = MetaUtil.exists(TableName.fromString(resultTable));
                String checkSql = String.format("select id from %s where taskName='%s'", resultTable, taskName);
                boolean removed = Result.wrap(() -> executor.query(ctx, checkSql)).unwrap()
                        .map(Collection::isEmpty).orElse(false);
                if (tableExists && !removed) {
                    continue;
                }
                expiredTaskStatus.setStatus(RecommendationsTaskStatus.Status.EXPIRED);
                expiredTaskStatus.persist();
                taskStatusMap.remove(expiredTaskStatus.getTaskName());
            }
        } catch (Throwable e) {
        }
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        try {
            reader.readCollection(RecommendationsTaskStatus.class, taskStatus -> {
                taskStatusMap.put(taskStatus.getTaskName(), taskStatus);
            });
        } catch (Throwable ignored) {
            taskStatusMap.clear();
        }
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        int numJson = 1 + taskStatusMap.size();
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockIDEPack.RECOMMENDATIONS_TASK_MGR, numJson);
        writer.writeInt(taskStatusMap.size());
        for (RecommendationsTaskStatus taskStatus : taskStatusMap.values()) {
            writer.writeJson(taskStatus);
        }
        writer.close();
    }

    public void applyLogEntry(RecommendationsTaskStatus taskStatus) {
        if (taskStatus.getStatus().equals(RecommendationsTaskStatus.Status.EXPIRED)) {
            taskStatusMap.remove(taskStatus.getTaskName());
        } else {
            taskStatusMap.put(taskStatus.getTaskName(), taskStatus);
        }
    }
}
