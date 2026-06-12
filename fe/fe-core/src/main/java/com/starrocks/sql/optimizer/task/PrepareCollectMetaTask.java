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

package com.starrocks.sql.optimizer.task;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.MetaPreparationItem;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

public class PrepareCollectMetaTask extends OptimizerTask {

    private final OptExpression planTree;

    public PrepareCollectMetaTask(TaskContext context, OptExpression root) {
        super(context);
        this.planTree = root;
    }

    @Override
    public void execute() {
        List<LogicalScanOperator> scanOperators = collectScanOperators(planTree)
                .stream()
                .filter(scanOperator -> scanOperator.getTable().supportPreCollectMetadata())
                .collect(Collectors.groupingBy(LogicalScanOperator::getOpType))
                .values()
                .stream()
                .filter(list -> list.size() > 1)
                .flatMap(List::stream)
                .collect(Collectors.toList());

        if (scanOperators.isEmpty()) {
            return;
        }

        int threadPoolSize = context.getOptimizerContext().getSessionVariable().getPrepareMetadataPoolSize();
        String queryId = context.getOptimizerContext().getQueryId().toString();
        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize,
                new ThreadFactoryBuilder().setNameFormat(String.format("prepare-metadata-%s", queryId)).build());

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Tracers ownerTracers = Tracers.get();
        ConnectContext connectContext = ConnectContext.get();
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "EXTERNAL.parallel_prepare_metadata")) {
            // Fork tracers for each parallel task on the owner thread
            int numTasks = scanOperators.size();
            List<Tracers> forks = new ArrayList<>(numTasks);
            CompletableFuture<?>[] futures = new CompletableFuture[numTasks];
            for (int i = 0; i < numTasks; i++) {
                LogicalScanOperator op = scanOperators.get(i);
                Tracers forked = ownerTracers.fork();
                forks.add(forked);
                futures[i] = CompletableFuture.supplyAsync(() ->
                                metadataMgr.prepareMetadata(queryId, op.getTable().getCatalogName(),
                                        new MetaPreparationItem(op.getTable(), op.getPredicate(),
                                                op.getLimit(), op.getTvrVersionRange()),
                                        forked, connectContext),
                        executorService);
            }
            CompletableFuture.allOf(futures).join();
            // Merge all forks back on the owner thread (single-threaded, no race)
            for (Tracers f : forks) {
                ownerTracers.mergeFrom(f);
            }
        }
        executorService.shutdown();
    }

    private List<LogicalScanOperator> collectScanOperators(OptExpression tree) {
        List<LogicalScanOperator> scanOperators = new ArrayList<>();
        Utils.extractOperator(tree, scanOperators, op -> op instanceof LogicalScanOperator);
        return scanOperators;
    }
}
