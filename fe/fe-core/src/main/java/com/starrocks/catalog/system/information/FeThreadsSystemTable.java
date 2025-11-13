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
package com.starrocks.catalog.system.information;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.Pair;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TFeThreadInfo;
import com.starrocks.thrift.TGetFeThreadsRequest;
import com.starrocks.thrift.TGetFeThreadsResponse;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.List;

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class FeThreadsSystemTable {
    private static final Logger LOG = LogManager.getLogger(FeThreadsSystemTable.class);
    private static final String NAME = "fe_threads";

    public static SystemTable create() {
        return new SystemTable(SystemId.FE_THREADS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("FE_ADDRESS", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("THREAD_ID", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("THREAD_NAME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("GROUP_NAME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("THREAD_STATE", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("IS_DAEMON", TypeFactory.createType(PrimitiveType.BOOLEAN))
                        .column("PRIORITY", TypeFactory.createType(PrimitiveType.INT))
                        .column("CPU_TIME_MS", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("USER_TIME_MS", TypeFactory.createType(PrimitiveType.BIGINT))
                        .build(), TSchemaTableType.SCH_FE_THREADS);
    }

    public static TGetFeThreadsResponse generateFeThreadsResponse(TGetFeThreadsRequest request) throws TException {
        TGetFeThreadsResponse response = new TGetFeThreadsResponse();
        TStatus status = new TStatus(TStatusCode.OK);
        response.setStatus(status);

        try {
            ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
            long[] threadIds = threadMXBean.getAllThreadIds();
            ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadIds);

            List<TFeThreadInfo> threads = Lists.newArrayList();
            Pair<String, Integer> selfNode = GlobalStateMgr.getCurrentState().getNodeMgr().getSelfNode();
            String feAddress = selfNode.first + ":" + selfNode.second;

            for (ThreadInfo threadInfo : threadInfos) {
                if (threadInfo == null) {
                    continue;
                }
                TFeThreadInfo threadData = new TFeThreadInfo();
                threadData.setFe_address(feAddress);
                threadData.setThread_id(threadInfo.getThreadId());
                threadData.setThread_name(threadInfo.getThreadName());
                
                // Get thread object to check daemon status and get thread group name
                Thread thread = findThreadById(threadInfo.getThreadId());
                String groupName = "";
                if (thread != null && thread.getThreadGroup() != null) {
                    groupName = thread.getThreadGroup().getName();
                }
                threadData.setGroup_name(groupName);
                threadData.setThread_state(threadInfo.getThreadState().toString());
                threadData.setIs_daemon(thread != null && thread.isDaemon());
                threadData.setPriority(threadInfo.getPriority());

                // Get CPU time if supported
                long cpuTime = -1;
                long userTime = -1;
                if (threadMXBean.isThreadCpuTimeSupported()) {
                    cpuTime = threadMXBean.getThreadCpuTime(threadInfo.getThreadId());
                    if (cpuTime != -1) {
                        cpuTime = cpuTime / 1000000; // Convert nanoseconds to milliseconds
                    }
                    if (threadMXBean instanceof com.sun.management.ThreadMXBean) {
                        com.sun.management.ThreadMXBean sunThreadMXBean =
                                (com.sun.management.ThreadMXBean) threadMXBean;
                        userTime = sunThreadMXBean.getThreadUserTime(threadInfo.getThreadId());
                        if (userTime != -1) {
                            userTime = userTime / 1000000; // Convert nanoseconds to milliseconds
                        }
                    }
                }
                threadData.setCpu_time_ms(cpuTime);
                threadData.setUser_time_ms(userTime);

                threads.add(threadData);
            }

            response.setThreads(threads);
        } catch (Exception e) {
            LOG.warn("Failed to get FE threads", e);
            status.setStatus_code(TStatusCode.INTERNAL_ERROR);
            status.addToError_msgs("Failed to get FE threads: " + e.getMessage());
        }

        return response;
    }

    private static Thread findThreadById(long threadId) {
        ThreadGroup rootGroup = Thread.currentThread().getThreadGroup();
        while (rootGroup.getParent() != null) {
            rootGroup = rootGroup.getParent();
        }
        Thread[] threads = new Thread[rootGroup.activeCount() * 2];
        int count = rootGroup.enumerate(threads, true);
        for (int i = 0; i < count; i++) {
            if (threads[i].getId() == threadId) {
                return threads[i];
            }
        }
        return null;
    }
}
