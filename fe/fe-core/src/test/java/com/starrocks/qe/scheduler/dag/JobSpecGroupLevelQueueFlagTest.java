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

package com.starrocks.qe.scheduler.dag;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.CoordinatorPreprocessor;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.scheduler.slot.ResourceUsageMonitor;
import com.starrocks.qe.scheduler.slot.SlotManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TWorkGroup;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The enable_group_level_query_queue flag sent to BE makes it skip its own resource-group
 * concurrency-limit check, on the premise that the FE queue enforces the limit by group-level
 * queueing. It must therefore be set only for queries that actually go through the FE queue, that is,
 * when the queue is enabled AND the query is not exempted from queueing.
 */
public class JobSpecGroupLevelQueueFlagTest {
    private boolean prevEnableGroupLevelQueryQueue = false;

    @Mocked
    private ScanNode scanNode;

    @BeforeEach
    public void before() {
        prevEnableGroupLevelQueryQueue = GlobalVariable.isEnableGroupLevelQueryQueue();
    }

    @AfterEach
    public void after() {
        GlobalVariable.setEnableGroupLevelQueryQueue(prevEnableGroupLevelQueryQueue);
    }

    @Test
    public void testFlagRequiresQueueEnabledNeedQueuedAndVariableOn(@Mocked GlobalStateMgr globalStateMgr,
                                                                   @Mocked ConnectContext connectContext) {
        new MockUp<CoordinatorPreprocessor>() {
            @Mock
            public TWorkGroup prepareResourceGroup(ConnectContext connect,
                                                   ResourceGroupClassifier.QueryType queryType) {
                return null;
            }
        };

        boolean[] queueEnabled = {true};
        SlotManager stubSlotManager = new SlotManager(new ResourceUsageMonitor()) {
            @Override
            public boolean isEnableQueryQueue(ConnectContext connect, JobSpec instance) {
                return queueEnabled[0];
            }
        };
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getSlotManager();
                result = stubSlotManager;
                minTimes = 0;

                connectContext.isNeedQueued();
                result = true;
                minTimes = 0;
            }
        };

        // A queued query with the variable on: the flag is propagated, FE enforces the limit by queueing.
        GlobalVariable.setEnableGroupLevelQueryQueue(true);
        JobSpec jobSpec = buildJobSpec(connectContext, ImmutableList.of(scanNode));
        assertThat(jobSpec.isEnableQueue()).isTrue();
        assertThat(jobSpec.isNeedQueued()).isTrue();
        assertThat(jobSpec.isEnableGroupLevelQueue()).isTrue();

        // A query exempted from queueing (no scan node, likewise schema-only): it never reaches the FE queue,
        // so the flag must not be propagated, otherwise BE would skip its check for a query FE does not queue.
        jobSpec = buildJobSpec(connectContext, Collections.emptyList());
        assertThat(jobSpec.isEnableQueue()).isTrue();
        assertThat(jobSpec.isNeedQueued()).isFalse();
        assertThat(jobSpec.isEnableGroupLevelQueue()).isFalse();

        // The variable is off: group-level queueing is not requested at all.
        GlobalVariable.setEnableGroupLevelQueryQueue(false);
        jobSpec = buildJobSpec(connectContext, ImmutableList.of(scanNode));
        assertThat(jobSpec.isEnableGroupLevelQueue()).isFalse();

        // The queue is disabled: nothing is queued on FE, so BE must keep enforcing the limit.
        queueEnabled[0] = false;
        GlobalVariable.setEnableGroupLevelQueryQueue(true);
        jobSpec = buildJobSpec(connectContext, ImmutableList.of(scanNode));
        assertThat(jobSpec.isEnableQueue()).isFalse();
        assertThat(jobSpec.isEnableGroupLevelQueue()).isFalse();
    }

    private static JobSpec buildJobSpec(ConnectContext connectContext, List<ScanNode> scanNodes) {
        TQueryOptions queryOptions = new TQueryOptions();
        queryOptions.setQuery_type(TQueryType.SELECT);
        return new JobSpec.Builder()
                .queryId(new TUniqueId(1, 2))
                .fragments(Collections.emptyList())
                .scanNodes(scanNodes)
                .queryOptions(queryOptions)
                .commonProperties(connectContext)
                .build();
    }
}
