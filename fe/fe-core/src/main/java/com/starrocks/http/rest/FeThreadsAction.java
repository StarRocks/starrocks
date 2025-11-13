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

package com.starrocks.http.rest;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//fehost:port/api/fe_threads
public class FeThreadsAction extends RestBaseAction {

    private static final Logger LOG = LogManager.getLogger(FeThreadsAction.class);
    public static final String API_PATH = "/api/fe_threads";
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    public FeThreadsAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, API_PATH, new FeThreadsAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) throws DdlException {
        try {
            ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
            long[] threadIds = threadMXBean.getAllThreadIds();
            ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadIds);

            List<Map<String, Object>> threads = new ArrayList<>();
            String feId = GlobalStateMgr.getCurrentState().getNodeMgr().getSelfNode().toString();

            for (ThreadInfo threadInfo : threadInfos) {
                if (threadInfo == null) {
                    continue;
                }
                Map<String, Object> threadData = new HashMap<>();
                threadData.put("fe_id", feId);
                threadData.put("thread_id", threadInfo.getThreadId());
                threadData.put("thread_name", threadInfo.getThreadName());
                threadData.put("thread_state", threadInfo.getThreadState().toString());
                
                // Get thread object to check daemon status
                Thread thread = findThreadById(threadInfo.getThreadId());
                threadData.put("is_daemon", thread != null && thread.isDaemon());
                threadData.put("priority", threadInfo.getPriority());

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
                threadData.put("cpu_time_ms", cpuTime);
                threadData.put("user_time_ms", userTime);

                threads.add(threadData);
            }

            Map<String, Object> result = new HashMap<>();
            result.put("threads", threads);

            response.setContentType("application/json");
            response.getContent().append(GSON.toJson(result));
            sendResult(request, response);
        } catch (Exception e) {
            LOG.warn("Failed to get FE threads", e);
            throw new DdlException("Failed to get FE threads: " + e.getMessage());
        }
    }

    private Thread findThreadById(long threadId) {
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
